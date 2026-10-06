// Package efp implements an embedded binary frame protocol:
// framing, fragmentation/reassembly and reconnectable sessions.
package efp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// Frame format (all integers big-endian):
//
//	MAGIC(2) VER(1) FLAGS(1) SEQ(4) MSGID(4) FRAG_IDX(2) FRAG_CNT(2) LEN(2) PAYLOAD(LEN) CRC32(4)
//
// CRC32 (IEEE) covers header + payload.
const (
	Magic0       = 0xEF
	Magic1       = 0xA5
	Version      = 1
	HeaderLen    = 18
	CRCLen       = 4
	MaxPayload   = 1024
	MaxFrameLen  = HeaderLen + MaxPayload + CRCLen
	FlagFragment = 1 << 0 // frame is one fragment of a multi-fragment message
	FlagControl  = 1 << 1 // frame is a session-layer control frame (e.g. hello)
)

// ErrKind classifies frame decoding failures so callers can distinguish them.
type ErrKind int

const (
	ErrKindNone ErrKind = iota
	ErrKindBadMagic
	ErrKindUnsupportedVersion
	ErrKindBadLength
	ErrKindCRCMismatch
)

func (k ErrKind) String() string {
	switch k {
	case ErrKindBadMagic:
		return "bad magic"
	case ErrKindUnsupportedVersion:
		return "unsupported version"
	case ErrKindBadLength:
		return "bad length"
	case ErrKindCRCMismatch:
		return "crc mismatch"
	default:
		return "no error"
	}
}

// FrameError is a distinguishable, inspectable decode error.
type FrameError struct {
	Kind   ErrKind
	Detail string
}

func (e *FrameError) Error() string {
	if e.Detail == "" {
		return "efp: " + e.Kind.String()
	}
	return fmt.Sprintf("efp: %s: %s", e.Kind, e.Detail)
}

// Is reports whether target is a FrameError with the same Kind.
func (e *FrameError) Is(target error) bool {
	t, ok := target.(*FrameError)
	return ok && t.Kind == e.Kind
}

// Sentinel errors for errors.Is checks.
var (
	ErrBadMagic           = &FrameError{Kind: ErrKindBadMagic}
	ErrUnsupportedVersion = &FrameError{Kind: ErrKindUnsupportedVersion}
	ErrBadLength          = &FrameError{Kind: ErrKindBadLength}
	ErrCRCMismatch        = &FrameError{Kind: ErrKindCRCMismatch}

	ErrMessageTooLarge    = errors.New("efp: message too large to fragment")
	ErrReassemblyOverflow = errors.New("efp: reassembly resource limit exceeded")
	ErrReassemblyTimeout  = errors.New("efp: reassembly timed out")
	ErrSessionClosed      = errors.New("efp: session closed")

	// ErrDuplicateFragment classifies informational duplicate-fragment
	// events. It is never a failure: the fragment is ignored and the
	// message is still delivered exactly once.
	ErrDuplicateFragment = errors.New("efp: duplicate fragment")

	// ErrExceedsPeerLimit is returned by SendMessage when the payload is
	// larger than the receive limit the peer advertised (or the default
	// limit, if the peer does not negotiate). The message is NOT sent.
	ErrExceedsPeerLimit = errors.New("efp: message exceeds peer's receive limit")
)

// versionAccepted reports whether a frame carrying the given version and
// flags may be decoded by this stack.
//
// Data frames ride at Version. Capability-negotiation control frames ride
// at capsVersion (see caps.go): receivers from before the caps feature
// only accept Version, so they reject a hello at decode time — the hello
// can never surface in a legacy peer's application as a phantom short
// message. A capsVersion frame without the control flag is rejected like
// any other unsupported version.
func versionAccepted(ver uint8, flags uint8) bool {
	if ver == Version {
		return true
	}
	return ver == capsVersion && flags&FlagControl != 0
}

// Frame is one decoded protocol frame.
type Frame struct {
	Version uint8
	Flags   uint8
	Seq     uint32 // per-sender monotonically increasing frame sequence
	MsgID   uint32 // identifies the application message this frame belongs to
	FragIdx uint16 // fragment index within the message (0 when unfragmented)
	FragCnt uint16 // total fragment count (1 when unfragmented)
	Payload []byte
}

// Fragmented reports whether the frame is part of a multi-fragment message.
func (f *Frame) Fragmented() bool { return f.Flags&FlagFragment != 0 }

// Encode serializes the frame into a newly allocated buffer.
func (f *Frame) Encode() ([]byte, error) {
	if len(f.Payload) > MaxPayload {
		return nil, &FrameError{Kind: ErrKindBadLength,
			Detail: fmt.Sprintf("payload %d bytes exceeds max %d", len(f.Payload), MaxPayload)}
	}
	if f.FragCnt == 0 {
		return nil, &FrameError{Kind: ErrKindBadLength, Detail: "FragCnt must be >= 1"}
	}
	if f.FragIdx >= f.FragCnt {
		return nil, &FrameError{Kind: ErrKindBadLength,
			Detail: fmt.Sprintf("FragIdx %d >= FragCnt %d", f.FragIdx, f.FragCnt)}
	}
	ver := f.Version
	if ver == 0 {
		ver = Version
	}
	buf := make([]byte, HeaderLen+len(f.Payload)+CRCLen)
	buf[0] = Magic0
	buf[1] = Magic1
	buf[2] = ver
	buf[3] = f.Flags
	binary.BigEndian.PutUint32(buf[4:], f.Seq)
	binary.BigEndian.PutUint32(buf[8:], f.MsgID)
	binary.BigEndian.PutUint16(buf[12:], f.FragIdx)
	binary.BigEndian.PutUint16(buf[14:], f.FragCnt)
	binary.BigEndian.PutUint16(buf[16:], uint16(len(f.Payload)))
	copy(buf[HeaderLen:], f.Payload)
	crc := crc32.ChecksumIEEE(buf[:HeaderLen+len(f.Payload)])
	binary.BigEndian.PutUint32(buf[HeaderLen+len(f.Payload):], crc)
	return buf, nil
}

// DecodeFrame parses exactly one frame from buf, which must contain a whole frame.
func DecodeFrame(buf []byte) (*Frame, error) {
	if len(buf) < HeaderLen+CRCLen {
		return nil, &FrameError{Kind: ErrKindBadLength,
			Detail: fmt.Sprintf("need at least %d bytes, got %d", HeaderLen+CRCLen, len(buf))}
	}
	if buf[0] != Magic0 || buf[1] != Magic1 {
		return nil, &FrameError{Kind: ErrKindBadMagic,
			Detail: fmt.Sprintf("magic=%02x %02x", buf[0], buf[1])}
	}
	if !versionAccepted(buf[2], buf[3]) {
		return nil, &FrameError{Kind: ErrKindUnsupportedVersion,
			Detail: fmt.Sprintf("version=%d supported=%d", buf[2], Version)}
	}
	declared := int(binary.BigEndian.Uint16(buf[16:]))
	if declared != len(buf)-HeaderLen-CRCLen {
		return nil, &FrameError{Kind: ErrKindBadLength,
			Detail: fmt.Sprintf("LEN=%d but buffer holds %d payload bytes",
				declared, len(buf)-HeaderLen-CRCLen)}
	}
	body := buf[:len(buf)-CRCLen]
	want := binary.BigEndian.Uint32(buf[len(buf)-CRCLen:])
	got := crc32.ChecksumIEEE(body)
	if want != got {
		return nil, &FrameError{Kind: ErrKindCRCMismatch,
			Detail: fmt.Sprintf("crc want=%08x got=%08x", want, got)}
	}
	payload := make([]byte, declared)
	copy(payload, body[HeaderLen:])
	f := &Frame{
		Version: buf[2],
		Flags:   buf[3],
		Seq:     binary.BigEndian.Uint32(buf[4:]),
		MsgID:   binary.BigEndian.Uint32(buf[8:]),
		FragIdx: binary.BigEndian.Uint16(buf[12:]),
		FragCnt: binary.BigEndian.Uint16(buf[14:]),
		Payload: payload,
	}
	if f.FragCnt == 0 {
		return nil, &FrameError{Kind: ErrKindBadLength, Detail: "FragCnt == 0"}
	}
	if f.FragIdx >= f.FragCnt {
		return nil, &FrameError{Kind: ErrKindBadLength,
			Detail: fmt.Sprintf("FragIdx %d >= FragCnt %d", f.FragIdx, f.FragCnt)}
	}
	if f.FragCnt > 1 && f.Flags&FlagFragment == 0 {
		return nil, &FrameError{Kind: ErrKindBadLength,
			Detail: "FragCnt > 1 without fragment flag"}
	}
	return f, nil
}
