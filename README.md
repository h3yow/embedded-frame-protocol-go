# embedded-frame-protocol-go

一套可在本地直接运行的**嵌入式二进制帧协议栈**，用于在字节流通道（串口、
TCP、内存管道等）上传输**带序号的应用消息**。协议栈聚焦三件事：

1. 在含噪声、半包到达的字节流里稳定切出帧并在损坏后**重新同步**；
2. 将大消息**分片 / 重组**，对乱序、重复、超时未齐给出确定性结果，且资源
   占用有硬上限；
3. 支持会话**并发读写与断线重连**，序号语义明确，重复投递不会被上层消费两次。

## 目录结构

```
pkg/frame        帧格式、CRC、噪声流解码器（resync）
pkg/reassembly   分片器 Splitter 与重组器 Assembler
pkg/session      并发、可重连的消息传输层（去重 + 按序投递）
```

三层相互独立：`frame` 不认识分片，`reassembly` 不认识字节流，`session`
把两者粘合起来。可以单独使用任意一层。

## 一、帧格式（pkg/frame）

所有整数均为**小端序**。一帧的完整布局：

```
偏移   长度  字段                 说明
0      2    magic                固定 0xA5 0x5A，帧同步前导
2      1    version              协议版本，当前为 1
3      1    flags                保留位（编码写 0，解码不校验）
4      2    payload length N     载荷字节数，范围 0..65000（uint16）
6      8    session id           会话标识（uint64）
14     4    sequence number      帧/消息序号（uint32）
18     N    payload              载荷
18+N   4    CRC-32 (IEEE)        对区间 [0, 18+N) 的校验和（uint32）
```

帧总长 = `18 + N + 4`，常量见 `frame.HeaderLength`、`frame.ChecksumLength`、
`frame.MaxPayloadLength`。

### 噪声流切帧与重新同步

`frame.Decoder.Feed(p)` 是增量式的，可在任意时刻喂入任意长度的字节
（一次一帧、一次一字节、多帧粘连、噪声夹杂都支持）。它内部维护一个缓冲，
处理规则：

- 缓冲不足一帧时保留尾部半包，等待更多字节；
- 起始处不是 magic 时，**向前扫描下一个 `A5 5A`**，丢弃之间的噪声字节并
  产出一个 `ErrSyncLost` 事件（非致命），随后继续解析；
- 长度非法（超过配置上限）时拒绝该帧。由于无法确定真实边界，解码器丢弃
  magic 后重新扫描前导；
- 版本不支持、CRC 不匹配时，长度可信，跳过整帧后继续，因此**不会永久卡死**。

`frame.StreamReader` 在 `io.Reader` 之上封装同样的逻辑，坏帧/重同步事件通过
`NextError()` 取出。

### 帧级错误分类（可用 `errors.Is` 区分）

| 错误哨兵 | 触发条件 |
| --- | --- |
| `frame.ErrShortBuffer` | 数据不足以构成完整帧（内部状态，通常不对外） |
| `frame.ErrSyncLost` | 丢失前导、丢弃噪声字节后重新同步 |
| `frame.ErrIllegalLength` | 声明的载荷长度超出允许范围 |
| `frame.ErrUnsupportedVersion` | 版本号不是 1 |
| `frame.ErrChecksum` | CRC-32 校验失败 |

所有错误都包装为 `*frame.FrameError`，含 `Op`、流偏移 `Offset`、`Kind`
与 `Detail`，便于定位损坏位置。

## 二、分片与重组（pkg/reassembly）

应用消息按固定大小（默认 1024 字节）切成若干片段。片段元信息承载于**帧
载荷**内部（会话 id 与消息序号复用外层帧头，不重复编码）：

```
偏移  长度  字段
0     1    flags（bit0 = First，bit1 = Last）
1     2    fragment index（从 0 开始，小端）
3     2    total fragment count（小端）
5     ...  fragment data
```

- `Splitter.Split(session, seq, payload)` 确定性地产生片段；空消息产生单个
  空的 First|Last 片段。
- `Assembler.Offer(fragment)` 每投入一个片段返回**一个确定性事件**，绝不
  静默丢弃，也不会把不完整消息交出去。

### 重组事件类型

| 事件 | 含义 |
| --- | --- |
| `EventProgress` | 片段已收下，消息尚未集齐，无交付 |
| `EventCompleted` | 全部到齐，`Data` 为拼好的完整消息 |
| `EventDuplicate` | 相同片段重复到达（幂等，状态不变） |
| `EventRejected` | 违反协议/超限，`Err` 给出可区分原因 |
| `EventTimeout` | 超过 TTL 仍未集齐，消息被放弃 |
| `EventAborted` | 主动 Reset（重连）放弃，损失被显式上报 |

### 确定性规则

- **乱序**：按 index 归位，任意顺序到达都能正确重组；
- **重复**：index 已存在且内容相同 → `EventDuplicate`；内容不同 →
  `EventRejected/ErrPartialDuplicate`，原状态不动；
- **total 不一致**：同一消息不同片段声称的分片数不同 →
  `ErrTotalMismatch`；
- **超时未齐**：`SweepExpired(now)` 放弃到期消息并为每条产生
  `EventTimeout`，不完整数据**永不交付**；
- **资源上限**：`MaxTotal`（每消息最大分片数）、`MaxMessage`（最大总字节）、
  `MaxPending`（同时进行中的不完整消息条数）三者共同封顶内存；超限分别
  返回 `ErrTooManyFragments` / `ErrMessageTooLarge` /
  `ErrPayloadOverflow` / `ErrTooManyPending`。

重组错误哨兵：`ErrInvalidFragment`、`ErrIndexOutOfRange`、
`ErrTotalMismatch`、`ErrPartialDuplicate`、`ErrPayloadOverflow`、
`ErrTooManyFragments`、`ErrMessageTooLarge`、`ErrTooManyPending`、
`ErrMessageIncomplete`。

## 三、会话层（pkg/session）

`session.New(transport, cfg)` 启动三条 goroutine：读取、分发（重组+去重+
按序投递）、超时清扫。

### 发送语义

- `Send(data)` 可被多 goroutine 并发调用；写锁同时负责**序号分配**，保证
  字节流上序号严格单调；
- 发送序号在**重连后继续递增**，不回绕、不重置；
- 超过重组上限的消息返回 `session.ErrMessageTooLarge`，不会发出半成品。

### 接收、去重与按序投递

- `Recv()` 返回按消息序号**严格有序、且最多一次**的完整消息；
- 内部维护一个有界环形接收窗口（默认 1024）：已投递序号被记录，重放的
  重复帧（**包括重连后对端重放**）在分发前被抑制，不会二次到达上层；
- 乱序完成的消息先进入 `ready` 表，等前面序号补齐后一次性按序投递。

### 外站帧

帧头 session id 与本会话不一致的帧被拒绝并产生
`EventFrameRejected`（`errors.Is(.., session.ErrBadSessionID)`），不影响
后续正常帧。

### 重连语义（关键约定）

`Reconnect()` 或自动重连（读失败且配置了 `Dial`）遵循：

1. 先摘除并**关闭旧 transport**，唤醒阻塞在其 `Read` 上的读循环；
2. 通过 `Dial()` 建立新连接（锁外拨号，避免持锁阻塞）；
3. **帧解码器重置**——旧字节流的半包不带入新连接；已解码排队帧清空；
4. **未完成的分片消息全部放弃**，对每条产生 `EventMessageTimeout`；
   放弃的序号被标记为"永久消费"：
   - 对端随后重放这些序号 → 去重窗口继续抑制；
   - 接收游标**跳过缺口**，后面序号更大的新消息不会被队首空洞永久阻塞；
5. **去重窗口跨重连保留**；发送序号继续递增；
6. 已经完整组装但尚未投递的消息：其序号已进入去重窗口，重放同样被抑制。

代次（generation）计数保证读循环只会读取当前连接，绝不会把新旧两条字节
流交错处理。

### 会话错误分类

| 错误 | 含义 |
| --- | --- |
| `session.ErrClosed` | 会话已关闭后再操作 |
| `session.ErrNoTransport` | 当前无连接（未重连成功）时发送 |
| `session.ErrReconnectFailed` | 未配置 Dial 或拨号失败 |
| `session.ErrBadSessionID` | 收到外站 session id 的帧 |
| `session.ErrMessageTooLarge` | 消息超出配置上限 |

非致命情况通过 `Events()` 通道以 `Event` 上报：`EventFrameRejected`、
`EventReconnected`、`EventMessageTimeout`、`EventAssemblyRejected`、
`EventReadError`。事件通道满时丢弃最旧审计信息不阻塞协议正确性。

## 四、本地验证

需要 Go 1.25+。

```bash
# 构建与静态检查
go build ./...
go vet ./...

# 全量单测（带竞态检测）
go test ./... -race

# 重复运行验证稳定性
go test ./... -race -count=10

# 只跑某一层 / 某个用例并查看关键输入与判定日志
go test ./pkg/frame/        -race -v
go test ./pkg/reassembly/   -race -v
go test ./pkg/session/      -race -v
go test ./pkg/session/ -race -v -run TestReconnectReplay
```

### 测试覆盖场景

- `pkg/frame`
  - `TestRoundTrip`：编码/解析与字段保持；
  - `TestFeedHalfFrames`：逐字节喂入（半包），直到末字节才出帧；
  - `TestFeedNoiseAndMultipleFrames`：噪声前导 + 好帧 + CRC 坏帧 + 好帧，
    验证 `ErrSyncLost` / `ErrChecksum` 可区分且能恢复；
  - `TestIllegalLengthAndVersion`：超长与错误版本分别分类；
  - `TestCorruptionRecovery`：长度字段被破坏（边界不可知）后仍能恢复；
  - `TestStreamReader`：`io.Reader` 包装器在垃圾前缀后交付并保留事件。
- `pkg/reassembly`
  - `TestOutOfOrderReassembly`：乱序分片重组；
  - `TestDuplicates`：相同重复（幂等）与冲突重复（拒绝）；
  - `TestTimeout`：TTL 到期放弃并释放内存；
  - `TestPendingCap`：进行中消息条数上限；
  - `TestTotalMismatch` / `TestEncodeDecodeRoundTrip` / `TestSingleAndEmpty`；
  - `TestResetAborts`：重置时每条被弃消息都有显式事件。
- `pkg/session`
  - `TestEndToEnd`：单条 / 空 / 大消息双向收发；
  - `TestHalfPacketDelivery`：对端一字节一字节释放，端到端半包重组；
  - `TestCorruptFrameSkipped`：噪声+坏帧被拒绝后正常消息继续送达；
  - `TestOutOfOrderAtSessionLevel`：会话层乱序分片；
  - `TestReplayDedup`：同一消息重复投递只消费一次；
  - `TestForeignSessionRejected`：外站帧单独分类；
  - `TestReconnectReplay`：断线重连后重放旧消息被抑制、新消息正常送达；
  - `TestReconnectAbortsPartial`：重连放弃未集齐消息并跳过缺口；
  - `TestConcurrentSends`：40 个 goroutine 并发发送，序号唯一、交付一次；
  - `TestPartialMessageTimeout`：运行期 TTL 清扫。

所有测试用例都通过 `t.Logf` 打印**关键输入**（喂入字节、分片顺序、注入
的序号）与**判定依据**（识别出的错误类型、交付序号），失败时可用 `-v`
查看。`go test -race` 在整个套件上保持干净。

## 五、快速上手

```go
import (
    "github.com/h3yow/embedded-frame-protocol-go/pkg/frame"
    "github.com/h3yow/embedded-frame-protocol-go/pkg/session"
)

sess, _ := session.New(conn, session.Config{
    SessionID: 42,
    Frame:     frame.Config{MaxPayload: 4096},
    Dial:      func() (session.Transport, error) { return redial() },
})
go func() {
    for ev := range sess.Events() { log.Printf("event: %v %v", ev.Type, ev.Detail) }
}()

seq, _ := sess.Send([]byte("hello"))          // 自动分片、写入序号
msg, err := sess.Recv()                       // 按序、去重的完整消息
```
