package frame

import (
	"errors"
	"io"
)

// DecodeResult is the outcome of feeding bytes to a Decoder.
type DecodeResult struct {
	Frame  *Frame // decoded frame; nil when Err is set
	Err    error  // rejected-frame / resync information; nil on success
	Offset int    // stream offset at which the result applies
}

// Decoder incrementally extracts frames from a noisy byte stream.
//
// After a corrupted frame it resynchronizes on the next magic preamble
// instead of permanently stalling. A Decoder is not safe for concurrent use;
// feed bytes from a single goroutine.
type Decoder struct {
	buf      []byte
	consumed int // total stream bytes already discarded or emitted
	maxPay   int
}

// NewDecoder creates a decoder with the given configuration.
func NewDecoder(cfg Config) *Decoder {
	return &Decoder{maxPay: cfg.maxPayload()}
}

// Offset returns the number of stream bytes consumed so far.
func (d *Decoder) Offset() int { return d.consumed }

// Reset discards all buffered partial data and resets the stream offset.
func (d *Decoder) Reset() {
	d.buf = d.buf[:0]
	d.consumed = 0
}

// Feed appends incoming bytes and returns every frame and reject event that
// can be derived from the accumulated stream. Results are ordered by stream
// offset; partial trailing frames are retained until more bytes arrive.
func (d *Decoder) Feed(p []byte) []DecodeResult {
	d.buf = append(d.buf, p...)
	var out []DecodeResult

	for len(d.buf) > 0 {
		if d.buf[0] != Magic0 {
			// Garbage prefix: locate the next magic preamble.
			i := scanMagic(d.buf)
			switch {
			case i >= 0:
				out = d.skip(out, i, "noise before magic")
			case d.buf[len(d.buf)-1] == Magic0:
				// Might be the first half of a magic pair; retain it.
				out = d.skip(out, len(d.buf)-1, "noise before candidate magic")
			default:
				out = d.skip(out, len(d.buf), "noise, no magic found")
			}
			continue
		}

		if len(d.buf) == 1 {
			break // wait: candidate first magic byte
		}
		if d.buf[1] != Magic1 {
			out = d.skip(out, 1, "lone magic byte")
			continue
		}
		if len(d.buf) < HeaderLength {
			break // half header: wait for more bytes
		}

		h, err := ParseHeader(d.buf[:HeaderLength])
		if err != nil {
			// Magic verified two bytes ago, so this cannot be ErrSyncLost.
			out = d.reject(out, errors.Unwrap(err))
			d.advance(2) // cannot trust length; drop magic and resync
			continue
		}
		if int(h.Length) > d.maxPay {
			out = d.reject(out, &FrameError{
				Op:     "decode",
				Offset: d.consumed,
				Kind:   ErrIllegalLength,
				Detail: "declared length " + itoa(int(h.Length)) +
					" exceeds limit " + itoa(d.maxPay),
			})
			d.advance(2) // boundary unknowable; drop magic and resync
			continue
		}

		total := TotalSize(int(h.Length))
		if len(d.buf) < total {
			break // half frame: wait for more bytes
		}

		f, perr := ParseFrame(d.buf[:total])
		if perr != nil {
			out = d.reject(out, perr)
			d.advance(total) // length trusted for version/checksum failures
			continue
		}
		out = append(out, DecodeResult{Frame: f, Offset: d.consumed})
		d.advance(total)
	}
	return out
}

// advance drops the first n buffered bytes and accounts the stream offset.
func (d *Decoder) advance(n int) {
	remaining := copy(d.buf, d.buf[n:])
	d.buf = d.buf[:remaining]
	d.consumed += n
}

// skip emits one ErrSyncLost event (when n>0) and discards n bytes.
func (d *Decoder) skip(out []DecodeResult, n int, detail string) []DecodeResult {
	if n > 0 {
		out = append(out, DecodeResult{
			Err: &FrameError{
				Op:     "resync",
				Offset: d.consumed,
				Kind:   ErrSyncLost,
				Detail: detail + ", skipped " + itoa(n) + " byte(s)",
			},
		})
		d.advance(n)
	}
	return out
}

// reject normalizes an error into a DecodeResult carrying the current offset.
func (d *Decoder) reject(out []DecodeResult, err error) []DecodeResult {
	fe, ok := err.(*FrameError)
	if !ok || fe == nil {
		fe = &FrameError{Op: "decode", Kind: err}
	}
	fe.Offset = d.consumed
	return append(out, DecodeResult{Err: fe, Offset: d.consumed})
}

// scanMagic returns the index of the first complete magic pair, or -1.
func scanMagic(b []byte) int {
	for i := 0; i+1 < len(b); i++ {
		if b[i] == Magic0 && b[i+1] == Magic1 {
			return i
		}
	}
	return -1
}

// StreamReader pulls frames from an io.Reader using an internal Decoder.
type StreamReader struct {
	r     io.Reader
	dec   *Decoder
	buf   []byte
	ready []*Frame
	errs  []error
}

// NewStreamReader wraps r with a resynchronizing frame decoder.
func NewStreamReader(r io.Reader, cfg Config) *StreamReader {
	return &StreamReader{
		r:   r,
		dec: NewDecoder(cfg),
		buf: make([]byte, 4096),
	}
}

// ReadFrame blocks until a frame is decoded or the underlying reader fails.
// Reject/resync events that occur while scanning are queued; inspect them
// with NextError. A reader error is returned after all readable bytes are
// processed.
func (s *StreamReader) ReadFrame() (*Frame, error) {
	for len(s.ready) == 0 {
		n, rerr := s.r.Read(s.buf)
		if n > 0 {
			for _, res := range s.dec.Feed(s.buf[:n]) {
				if res.Frame != nil {
					s.ready = append(s.ready, res.Frame)
				} else if res.Err != nil {
					s.errs = append(s.errs, res.Err)
				}
			}
		}
		if len(s.ready) > 0 {
			break
		}
		if rerr != nil {
			return nil, rerr
		}
	}
	f := s.ready[0]
	s.ready = s.ready[1:]
	return f, nil
}

// NextError returns the next recorded reject/resync event, if any.
func (s *StreamReader) NextError() error {
	if len(s.errs) == 0 {
		return nil
	}
	e := s.errs[0]
	s.errs = s.errs[1:]
	return e
}
