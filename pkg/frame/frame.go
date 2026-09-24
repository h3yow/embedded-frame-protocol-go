package frame

import (
	"encoding/binary"
	"hash/crc32"
	"strconv"
)

// Frame is one decoded (or to-be-encoded) wire frame.
type Frame struct {
	Version  byte
	Flags    byte
	Session  uint64
	Sequence uint32
	Payload  []byte
}

// Header is the fixed-size prefix of a frame.
type Header struct {
	Version  byte
	Flags    byte
	Length   uint16
	Session  uint64
	Sequence uint32
}

// Config configures frame codec limits.
type Config struct {
	// MaxPayload overrides MaxPayloadLength when > 0.
	MaxPayload int
}

func (c Config) maxPayload() int {
	if c.MaxPayload > 0 {
		return c.MaxPayload
	}
	return MaxPayloadLength
}

var crcTable = crc32.MakeTable(crc32.IEEE)

// Checksum returns the CRC-32 (IEEE) checksum of b.
func Checksum(b []byte) uint32 {
	return crc32.Checksum(b, crcTable)
}

// TotalSize returns the wire size of a frame carrying payloadLen bytes.
func TotalSize(payloadLen int) int {
	return HeaderLength + payloadLen + ChecksumLength
}

// Encode serializes f into a freshly allocated byte slice.
func Encode(f *Frame) ([]byte, error) {
	size := TotalSize(len(f.Payload))
	buf := make([]byte, size)
	if _, err := EncodeInto(buf, f); err != nil {
		return nil, err
	}
	return buf, nil
}

// EncodeInto serializes f at the front of dst. dst must have capacity for at
// least TotalSize(len(f.Payload)); the returned slice is the frame.
func EncodeInto(dst []byte, f *Frame) ([]byte, error) {
	if len(f.Payload) > MaxPayloadLength {
		return nil, &FrameError{Op: "encode", Kind: ErrIllegalLength,
			Detail: "payload length " + itoa(len(f.Payload))}
	}
	out := dst[:TotalSize(len(f.Payload))]
	out[0] = Magic0
	out[1] = Magic1
	version := f.Version
	if version == 0 {
		version = Version1
	}
	out[2] = version
	out[3] = f.Flags
	binary.LittleEndian.PutUint16(out[4:6], uint16(len(f.Payload)))
	binary.LittleEndian.PutUint64(out[6:14], f.Session)
	binary.LittleEndian.PutUint32(out[14:18], f.Sequence)
	copy(out[HeaderLength:], f.Payload)
	sum := Checksum(out[:HeaderLength+len(f.Payload)])
	binary.LittleEndian.PutUint32(out[HeaderLength+len(f.Payload):], sum)
	return out, nil
}

// ParseHeader parses a fixed-size header. The slice must be at least
// HeaderLength bytes.
func ParseHeader(b []byte) (Header, error) {
	if len(b) < HeaderLength {
		return Header{}, &FrameError{Op: "parse-header", Kind: ErrShortBuffer}
	}
	if b[0] != Magic0 || b[1] != Magic1 {
		return Header{}, &FrameError{Op: "parse-header", Kind: ErrSyncLost,
			Detail: "bad magic"}
	}
	h := Header{
		Version:  b[2],
		Flags:    b[3],
		Length:   binary.LittleEndian.Uint16(b[4:6]),
		Session:  binary.LittleEndian.Uint64(b[6:14]),
		Sequence: binary.LittleEndian.Uint32(b[14:18]),
	}
	return h, nil
}

// ParseFrame parses one complete frame from b (header+payload+checksum).
func ParseFrame(b []byte) (*Frame, error) {
	h, err := ParseHeader(b)
	if err != nil {
		return nil, err
	}
	if len(b) < TotalSize(int(h.Length)) {
		return nil, &FrameError{Op: "decode", Kind: ErrShortBuffer}
	}
	if h.Version != Version1 {
		return nil, &FrameError{Op: "decode", Kind: ErrUnsupportedVersion,
			Detail: "version=" + strconv.Itoa(int(h.Version))}
	}
	payloadEnd := HeaderLength + int(h.Length)
	want := binary.LittleEndian.Uint32(b[payloadEnd : payloadEnd+ChecksumLength])
	got := Checksum(b[:payloadEnd])
	if want != got {
		return nil, &FrameError{Op: "decode", Kind: ErrChecksum}
	}
	payload := make([]byte, h.Length)
	copy(payload, b[HeaderLength:payloadEnd])
	return &Frame{
		Version:  h.Version,
		Flags:    h.Flags,
		Session:  h.Session,
		Sequence: h.Sequence,
		Payload:  payload,
	}, nil
}
