// Package frame defines the binary wire frame format and a resynchronizing
// decoder for noisy byte streams.
//
// Wire layout (all integers little-endian):
//
//	offset  size  field
//	0       2     magic 0xA5 0x5A
//	2       1     version (currently 1)
//	3       1     flags (reserved, must be 0 on encode, ignored on decode)
//	4       2     payload length N (0 .. MaxPayloadLength)
//	6       8     session id
//	14      4     sequence number
//	18      N     payload
//	18+N    4     CRC-32 (IEEE) over bytes [0, 18+N)
//
// Total frame size = HeaderLength + N + ChecksumLength.
package frame

import "errors"

// Protocol constants.
const (
	Magic0         byte = 0xA5
	Magic1         byte = 0x5A
	Version1       byte = 1
	HeaderLength        = 18
	ChecksumLength      = 4
	// MaxPayloadLength is the largest payload accepted by the frame layer.
	MaxPayloadLength = 65000
)

// Distinguishable frame errors. Use errors.Is to classify failures.
var (
	// ErrShortBuffer means more bytes are needed before a frame can be parsed.
	ErrShortBuffer = errors.New("frame: short buffer")
	// ErrSyncLost means the magic preamble was lost; bytes were discarded
	// while resynchronizing. Not fatal: decoding continues after the event.
	ErrSyncLost = errors.New("frame: resynchronized after losing magic preamble")
	// ErrIllegalLength means the declared payload length is out of range.
	ErrIllegalLength = errors.New("frame: illegal payload length")
	// ErrUnsupportedVersion means the frame version is not supported.
	ErrUnsupportedVersion = errors.New("frame: unsupported protocol version")
	// ErrChecksum means the CRC-32 checksum did not match.
	ErrChecksum = errors.New("frame: checksum mismatch")
)

// FrameError wraps a frame-level failure with the offset in the input stream
// at which the rejected frame started.
type FrameError struct {
	Op     string // operation, e.g. "decode"
	Offset int    // stream offset where the offending data began
	Kind   error  // one of the sentinel errors above
	Detail string // human readable detail
}

func (e *FrameError) Error() string {
	msg := "frame error"
	if e.Kind != nil {
		msg = e.Kind.Error()
	}
	if e.Op != "" {
		msg += " [" + e.Op + "]"
	}
	if e.Detail != "" {
		msg += " " + e.Detail
	}
	msg += " at offset " + itoa(e.Offset)
	return msg
}

func (e *FrameError) Unwrap() error { return e.Kind }

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
