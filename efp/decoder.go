package efp

import (
	"encoding/binary"
	"fmt"
)

// Decoder incrementally splits a byte stream into frames.
// On corrupt data it resynchronizes by scanning for the next magic
// instead of getting stuck. Decoder is NOT safe for concurrent use.
type Decoder struct {
	buf []byte
	// Stats for observability in tests and logs.
	FramesDecoded int
	BytesDropped  int
	Errors        int
}

// NewDecoder creates an empty stream decoder.
func NewDecoder() *Decoder { return &Decoder{} }

// Reset discards all buffered undecoded bytes. Counters are kept.
//
// A transport switch (reconnect) MUST Reset the decoder: bytes buffered
// from a dead connection are a partial frame at best, and completing them
// with bytes from the new connection would fabricate a frame that looks
// well-formed but is garbage. Message-level reassembly state is a separate
// layer and is intentionally NOT reset (see Session.Attach).
func (d *Decoder) Reset() { d.buf = d.buf[:0] }

// Buffered returns the number of undecoded bytes currently held (a
// partial frame waiting for the rest of its bytes).
func (d *Decoder) Buffered() int { return len(d.buf) }

// Feed appends data to the internal buffer and decodes as many complete
// frames as possible. Corrupt bytes are skipped (resync) and reported via
// the returned errors; undecodable trailing bytes (half packets) are kept
// for the next Feed call.
func (d *Decoder) Feed(data []byte) (frames []*Frame, errs []error) {
	d.buf = append(d.buf, data...)
	for {
		// Need at least a header to know the frame length.
		if len(d.buf) < HeaderLen {
			return frames, errs
		}
		// Resync: find the magic prefix.
		if d.buf[0] != Magic0 || d.buf[1] != Magic1 {
			idx := indexMagic(d.buf)
			if idx < 0 {
				// No magic at all: drop everything but keep a trailing
				// byte if it could be the start of a magic pair.
				drop := len(d.buf)
				if d.buf[len(d.buf)-1] == Magic0 {
					drop = len(d.buf) - 1
				}
				d.record(drop, &errs)
				d.buf = d.buf[drop:]
				return frames, errs
			}
			d.record(idx, &errs)
			d.buf = d.buf[idx:]
			if len(d.buf) < HeaderLen {
				return frames, errs
			}
		}
		// Header present: validate version before trusting LEN.
		if !versionAccepted(d.buf[2], d.buf[3]) {
			errs = append(errs, &FrameError{Kind: ErrKindUnsupportedVersion,
				Detail: fmt.Sprintf("version=%d supported=%d", d.buf[2], Version)})
			d.Errors++
			// Skip one byte and resync; the magic may be noise.
			d.buf = d.buf[1:]
			d.BytesDropped++
			continue
		}
		declared := int(binary.BigEndian.Uint16(d.buf[16:]))
		if declared > MaxPayload {
			errs = append(errs, &FrameError{Kind: ErrKindBadLength,
				Detail: fmt.Sprintf("LEN=%d exceeds max %d", declared, MaxPayload)})
			d.Errors++
			d.buf = d.buf[1:]
			d.BytesDropped++
			continue
		}
		total := HeaderLen + declared + CRCLen
		if len(d.buf) < total {
			// Half packet: wait for more data.
			return frames, errs
		}
		f, err := DecodeFrame(d.buf[:total])
		if err != nil {
			errs = append(errs, err)
			d.Errors++
			// Corrupt frame: skip one byte and resync, never stall.
			d.buf = d.buf[1:]
			d.BytesDropped++
			continue
		}
		frames = append(frames, f)
		d.FramesDecoded++
		d.buf = d.buf[total:]
	}
}

// record counts dropped bytes and, when anything was dropped, appends a
// bad-magic error unless the last error already is one.
func (d *Decoder) record(n int, errs *[]error) {
	if n == 0 {
		return
	}
	d.BytesDropped += n
	d.Errors++
	if len(*errs) == 0 {
		*errs = append(*errs, &FrameError{Kind: ErrKindBadMagic,
			Detail: fmt.Sprintf("resync: skipped %d noise bytes", n)})
	}
}

// indexMagic finds the first offset where a magic pair starts.
func indexMagic(buf []byte) int {
	for i := 0; i+1 < len(buf); i++ {
		if buf[i] == Magic0 && buf[i+1] == Magic1 {
			return i
		}
	}
	return -1
}
