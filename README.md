# embedded-frame-protocol-go

一套可本地运行的嵌入式二进制帧协议栈（包名 `efp`），用于在字节流通道
（串口、TCP、`net.Pipe` 等任意 `io.ReadWriteCloser`）上传输带序号的应用
消息，支持分片/重组、坏帧拒绝与重同步、断线重连与重复投递抑制、建连
能力对齐与运行期统计。

## 帧格式

所有整数均为大端序，定长 18 字节头部：

```
+----------+-----+-------+--------+--------+----------+----------+-----+=========+--------+
| MAGIC(2) | VER | FLAGS | SEQ(4) | MSGID  | FRAG_IDX | FRAG_CNT | LEN | PAYLOAD | CRC32  |
| EF A5    | (1) | (1)   |        | (4)    | (2)      | (2)      | (2) | (LEN)   | (4)    |
+----------+-----+-------+--------+--------+----------+----------+-----+=========+--------+
```

- `MAGIC`：固定 `0xEF 0xA5`，用于字节流中的帧定界与重同步。
- `VER`：协议版本，当前为 `1`；不支持的版本直接拒绝。
- `FLAGS`：bit0 `FlagFragment` 表示该帧是多分片消息的一片；bit1
  `FlagControl` 表示会话层控制帧（目前仅用于能力对齐的 hello）。
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
| `ErrMessageTooLarge`      | 消息超出可分片上限         |
| `ErrExceedsPeerLimit`     | 消息超出对端通告的接收上限（未发送）|
| `ErrReassemblyOverflow`   | 重组资源超限（在飞数/字节数）|
| `ErrReassemblyTimeout`    | 分片超时未齐               |
| `ErrSessionClosed`        | 会话已关闭                 |
| `ErrDuplicateFragment`    | 重复分片事件（仅信息，非失败）|

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

## 会话与重连语义

`Session` 支持并发读写（`SendMessage` 可从多个 goroutine 调用，单帧字节
不会在在线路上交错）：

- 发送序号 `SEQ` 与消息 ID 在 `Attach`（重连）之间保持连续，不复位。
- 接收侧维护大小为 1024 的已交付 MSGID 去重窗口：重连后对端重放的
  已交付消息会被丢弃，**上层不会重复消费**。
- **接收边界规则**：每次 `Attach` 都会重置流解码器的缓冲，新连接上解析
  出的任何一帧都只由该连接自己的字节组成——上一段连接残留的半帧字节
  会被丢弃，绝不会与新连接的字节拼成一个"看着完整、内容错乱"的帧。
- 与接收边界相对，**消息级重组状态跨重连保留**：上一条连接里还没凑齐的
  大消息，可以由新连接上到达的剩余分片补完（对端无需重发已收到的分片，
  重发的分片按重复分片规则忽略）。
- 被 `Attach`/`Close` 替换掉的旧传输产生的读错误不会作为伪错误上报。
- 全部共享状态由互斥锁保护，测试通过 `go test -race`。

## 能力对齐（建连协商）

每次 `Attach`（含每次重连）后，会话会异步向对端发送一个 hello 控制帧
（`FlagControl`），通告本端的接收能力：单条消息最大字节数
（`MaxMessageSize`）与最大并发在拼消息数（`MaxInFlight`），取值来自
`SessionConfig.Reassembly`。hello 载荷固定 7 字节：

```
KIND(1)=1  MAX_MSG_SIZE(4)  MAX_IN_FLIGHT(2)   （大端）
```

- 对端的 hello 到达后，`Session.PeerCaps()` 返回其通告值且
  `Negotiated=true`，**新上限立即生效**，无需重启进程。
- `SendMessage` 在写出任何字节**之前**检查消息大小：超过对端上限时当场
  失败，错误可用 `errors.Is(err, ErrExceedsPeerLimit)` 区分（与本地分片
  上限 `ErrMessageTooLarge` 不同），消息不会发出、也不会被对端静默丢弃。
- **兼容范围**：对端未实现能力对齐（旧固件，从不发 hello）时，连接照常
  工作，`PeerCaps()` 返回 `DefaultPeerCaps()`（64 KiB / 8，与
  `DefaultReassemblyConfig` 一致）且 `Negotiated=false`；超过默认上限的
  消息同样在发送侧失败，不会假装发送成功。每次重连都会重新协商：
  新连接先回到默认上限，收到新 hello 后切换到新值。

## 运行期统计与事件

`Session.Stats()` 返回一份一致的计数快照（`Stats`），可从任意 goroutine
随时调用，无数据竞争：

| 字段                 | 含义                                   |
|----------------------|----------------------------------------|
| `FramesDecoded`      | 成功解码的帧数                         |
| `DecodeErrors`       | 解码器拒绝的坏帧/坏字节批次数          |
| `NoiseBytesSkipped`  | 重同步时跳过的噪声字节数               |
| `DuplicateFragments` | 被忽略的重复分片数                     |
| `FragmentTimeouts`   | 超时未齐被清理的消息数                 |
| `InFlightRejected`   | 因在拼消息过多被拦下的新消息数         |

重复分片除计数外，还会以 `DuplicateFragmentEvent`（含 `MsgID`、
`FragIdx`）的形式投递到 `Session.Events()` 通道，可用
`errors.Is(&ev, ErrDuplicateFragment)` 判定。事件仅为信息上报：重复分片
仍只计一片，消息仍恰好交付一次。事件通道满时丢弃事件而不阻塞读循环，
计数不受影响。

## 本地验证

```sh
# 全部单元测试（日志打印关键输入与判定依据，需 -v 查看）
go test ./... -v

# 竞态检测
go test ./... -race -count=1
```

测试覆盖：半包逐字节喂入、噪声注入、CRC 损坏后重同步、非法版本、
乱序/重复分片、分片超时、在飞上限、并发发送、重连重放去重、
跨重连补全消息（仅补剩余分片）、重连接收边界（跨连接字节不拼帧）、
能力对齐与超限发送、无协商对端的默认上限降级、重连后重新协商、
重复分片事件与去重、统计各项计数及并发读取等场景。
