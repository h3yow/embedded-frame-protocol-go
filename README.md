# embedded-frame-protocol-go

一套可本地运行的嵌入式二进制帧协议栈（包名 `efp`），用于在字节流通道
（串口、TCP、`net.Pipe` 等任意 `io.ReadWriteCloser`）上传输带序号的应用
消息，支持分片/重组、坏帧拒绝与重同步、断线重连与重复投递抑制、
连接级能力对齐，以及运行期可观测统计。

## 帧格式

所有整数均为大端序，定长 18 字节头部：

```
+----------+-----+-------+--------+--------+----------+----------+-----+=========+--------+
| MAGIC(2) | VER | FLAGS | SEQ(4) | MSGID  | FRAG_IDX | FRAG_CNT | LEN | PAYLOAD | CRC32  |
| EF A5    | (1) | (1)   |        | (4)    | (2)      | (2)      | (2) | (LEN)   | (4)    |
+----------+-----+-------+--------+--------+----------+----------+-----+=========+--------+
```

- `MAGIC`：固定 `0xEF 0xA5`，用于字节流中的帧定界与重同步。
- `VER`：协议版本。`1` 为数据帧（本版所有应用消息仍用 v1，保持对旧
  固件兼容）；`2` 仅用于连接建立时的能力协商控制帧（见下）。不支持的
  版本直接拒绝并重同步。
- `FLAGS`：bit0 `FlagFragment` 表示该帧是多分片消息的一片；
  bit1 `FlagControl` 表示该帧是连接控制帧（v2，当前仅能力协商一种）。
- `SEQ`：发送方单调递增的帧序号（每帧 +1，含分片）。
- `MSGID`：应用消息 ID，同一消息的所有分片相同。
- `FRAG_IDX` / `FRAG_CNT`：分片序号与总分片数；未分片消息为 `0 / 1`。
- `LEN`：载荷字节数，上限 `MaxPayload = 1024`。
- `CRC32`：IEEE 多项式，覆盖头部 + 载荷。

## 错误分类

所有帧级错误都是 `*FrameError`，携带 `Kind` 字段，可用 `errors.Is`
区分：

| 哨兵错误                  | 含义                       |
|---------------------------|----------------------------|
| `ErrBadMagic`             | 魔数不匹配（噪声/错位）    |
| `ErrUnsupportedVersion`   | 协议版本不支持             |
| `ErrBadLength`            | 长度字段非法/不一致        |
| `ErrCRCMismatch`          | CRC 校验失败               |
| `ErrMessageTooLarge`      | 消息超出本地可分片上限     |
| `ErrPeerLimitExceeded`    | 消息超出对端接收上限，**发送前当场拒绝**（区别于线路错误）|
| `ErrReassemblyOverflow`   | 重组资源超限（在飞数/字节数）|
| `ErrReassemblyTimeout`    | 分片超时未齐               |
| `ErrSessionClosed`        | 会话已关闭                 |

`ErrPeerLimitExceeded` 与 `ErrMessageTooLarge` 的区别：后者表示消息大到
本地发送方根本无法分片（协议极限），前者表示消息大到**当前这个对端不愿
接收**（可能换一个对端就能发）；两者都意味着一个字节都没有上线，不会
出现"假装发送成功、对端再丢掉"的情况。

重组失败以 `*ReassemblyError`（含 `MsgID` 与 `Reason`）上报，可用
`errors.As` 取出、用 `errors.Is` 匹配上述原因。

## 解码与重同步语义

`Decoder.Feed` 增量消费字节流：

- **半包**：不足一帧的字节被保留，等待下一次 `Feed`，绝不误判。
- **噪声/坏帧**：扫描下一个魔数进行重同步；被跳过的字节计入
  `BytesDropped` 并产生对应分类错误。解码器永远不会因损坏数据而卡死。
- 版本与长度在信任 `LEN` 之前先校验，避免被恶意长度字段拖垮。

## 分片与重组语义

- 乱序分片：按 `FRAG_IDX` 落位，收齐 `FRAG_CNT` 片后按序拼接，**恰好
  交付一次**。
- 重复分片：确定性忽略（不重复计数、不重复交付）。
- 不完整消息**绝不**交付给上层。
- 资源上限（`ReassemblyConfig`）：`MaxInFlight`（并发未完成消息数）、
  `MaxMessageSize`（重组后总字节数）、`FragmentTimeout`（未完成消息最大
  存活时间）。超限/超时产生确定性的 `*ReassemblyError`，不静默丢弃。
- 同一消息的分片若 `FRAG_CNT` 不一致，该消息被整体拒绝。

## 连接能力对齐（capability handshake）

每次 `Attach`（初次连接与每次重连）都会在新连接上做一次能力协商，
目的是让发送方按**对端愿意收**的上限发送：

1. 连上后双方各发一个能力帧（v2、`FlagControl`，8 字节定长载荷）：
   `MaxMessageSize uint32` + `MaxInFlight uint32`，都是**接收侧**限制。
2. 收到对端能力帧即按其上限生效，`Negotiations` 计数 +1。
3. 对端在 `NegotiationTimeout`（默认 `DefaultNegotiationTimeout = 750ms`）
   内没有回应（旧固件 / 抖动丢包），自动落到保守默认值
   `DefaultCapabilities()`：`MaxMessageSize = MaxPayload (1024)`、
   `MaxInFlight = 1`，`NegotiationFalls` 计数 +1。`Attach` 本身永不阻塞
   等待握手；第一条 `SendMessage` 会等本次连接协商落定（通常已完成）。
4. **重连即重新协商**：新连接、新上限立即生效，无需重启进程；上一条
   连接协商出的上限不复用。

发送时的策略：

- `SendMessage` 在写入任何字节之前先比对当前对端 `MaxMessageSize`；
  超限立即返回包裹 `ErrPeerLimitExceeded` 的错误，消息**完全不上线**。
- 可通过 `PeerCapabilities() (caps, fromPeer)` 随时查看当前生效上限及
  其来源（对端真实通告 vs 保守降级）；`LocalCapabilities()` 查看本端
  通告值。
- 本端通告值默认取自 `ReassemblyConfig`，也可用
  `SessionConfig.LocalCapabilities` 单独覆盖；超时可用
  `SessionConfig.NegotiationTimeout` 调整。

### 与不同对端版本的兼容范围

| 本端 | 对端 | 行为 |
|------|------|------|
| 新版 | 新版 | 能力帧互换，按对端实际上限发送，超限本地拒绝 |
| 新版 | **旧版（不识别能力帧）** | 旧端收到 v2 帧按"不支持的版本"拒绝并重同步，不影响其后的 v1 数据通信；新端超时后按 `DefaultCapabilities()`（1024/1）发送，大于 1024 的消息收到 `ErrPeerLimitExceeded`，绝不假成功 |
| 旧版 | 新版 | 新端等不到回应会降级；旧端只发 v1 数据帧，新端照常重组，能力协商对旧端透明 |

注意：数据帧的 `VER` 仍是 `1`，因此只要消息不超过保守默认值，新旧固件
可以互通；能力协商只新增了 v2 控制帧，不改变 v1 数据帧格式。

## 重连接收边界与会话语义

`Session` 支持并发读写（`SendMessage` 可从多个 goroutine 调用，单帧字节
不会在在线路上交错）：

- 发送序号 `SEQ` 与消息 ID 在 `Attach`（重连）之间保持连续，不复位。
- **严格的连接接收边界**：每次 `Attach` 都 (a) 推进一个连接纪元
  （epoch），(b) 调用 `Decoder.Reset()` 清空残留半包。旧连接读循环即便
  在连接被替换之后才读到字节（某些串口栈 `Close` 不会唤醒阻塞的
  `Read`），这些字节也会按纪元被整段丢弃，绝不进入解码器——因此两段
  连接的首尾字节不可能拼成一个"看似完整、内容错乱"的帧。
- 接收侧维护大小为 1024 的已交付 MSGID 去重窗口：重连后对端重放的
  已交付消息会被丢弃，**上层不会重复消费**。
- **跨连接补全**：未完成的重组状态刻意跨重连保留。上一条连接没凑齐的
  大消息，仍可用新连接上到达（含重放）的分片补完；重放分片按重复分片
  规则只算一片，消息恰好交付一次。
  规则可一句话概括：**帧边界按连接隔离，消息重组跨连接延续。**
- 被 `Attach`/`Close` 替换掉的旧传输产生的读错误不会作为伪错误上报。
- 全部共享状态由互斥锁保护，统计计数全部基于原子操作，测试通过
  `go test -race`。

## 运行期可观测性

`SnapshotStats()` 随时返回一份不可变的 `Stats` 值拷贝，可在协议运行期间
被任意多个 goroutine 并发读取，不与读循环/清理协程产生数据竞争：

| 字段 | 含义 |
|------|------|
| `FramesDecoded`      | 成功解码的帧数（含能力帧） |
| `BytesDropped`       | 重同步时跳过的噪声字节数 |
| `DecodeErrors`       | 坏魔数/版本/长度/CRC 错误次数 |
| `DuplicateFragments` | 收到的重复分片次数（只计数、不重复落片） |
| `FragmentTimeouts`   | 分片超时未齐被清理的消息数 |
| `ReassemblyRejects`  | 因在飞数/字节数上限被拒的新消息数 |
| `MessagesDelivered`  | 交付上层的完整消息数 |
| `Reconnects`         | 替换掉旧传输的 `Attach` 次数 |
| `Negotiations`       | 与对端完成能力协商的次数 |
| `NegotiationFalls`   | 能力协商超时、落到默认值的次数 |

重复分片除了计数，还会以**可区分的事件**形式从
`Events() <-chan DuplicateFragmentEvent`（含 `MsgID`、`FragIdx`）报出。
语义不变：重复分片仍只算一片，消息仍只交付一次。事件通道对协议非阻塞
（应用不及时取走时丢事件、不丢数据）。

## 本地验证

```sh
# 全部单元测试（-v 可看清每个用例喂了什么、期望什么、实际判成什么）
go test ./... -v

# 竞态检测（新增统计读取用例同样过 -race）
go test ./... -race -count=1

# 只看本轮新增的三类能力相关用例
go test ./efp/ -v -count=1 -run 'TestCap|TestDecoderReset|TestSessionReconnect|TestSessionStale|TestDuplicateFragment|TestStats'
```

测试覆盖（含原有场景，无回归）：

- 能力对齐：能力帧编解码、按对端上限收/发、超限**发送前**以
  `ErrPeerLimitExceeded` 拒绝、旧版对端超时降级且不假成功、重连后按新
  对端上限立即重协商。
- 重连接收边界：解码器 `Reset` 隔离、两段连接首尾字节不拼成错乱帧、
  被替换传输迟到的字节按纪元整段丢弃（计数为 0）、旧传输读退出不报伪
  错误。
- 跨连接补全：上一连接只到部分分片，重连后重放+补齐，恰好交付一次。
- 重复分片：`DuplicateFragmentEvent` 事件逐个可区分上报、
  `DuplicateFragments` 计数正确、只算一片、消息只交付一次。
- 统计：在飞上限拒绝、分片超时、噪声跳字节、重连等计数分开；多
  goroutine 在有流量时并发 `SnapshotStats()`，过竞态检测。
- 原有：半包逐字节喂入、噪声注入、CRC 损坏后重同步、非法版本、
  乱序/重复分片、分片超时、在飞上限、并发发送、重连重放去重、
  跨重连补全消息等场景。
