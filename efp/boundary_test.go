package efp

import (
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// TestDecoderResetIsolatesStreams proves at the decoder level that bytes
// carried over from a dead connection are never concatenated with the new
// connection's stream.
func TestDecoderResetIsolatesStreams(t *testing.T) {
	d := NewDecoder()

	// Frame F on connection 1, only its first 10 bytes arrive before the
	// link dies mid-frame.
	f := mustEncode(t, &Frame{Seq: 1, MsgID: 101, FragCnt: 1, Payload: []byte("connection-one")})
	frames, errs := d.Feed(f[:10])
	if len(frames) != 0 || len(errs) != 0 {
		t.Fatalf("half frame decoded frames=%d errs=%v", len(frames), errs)
	}

	// New connection starts: the session resets the decoder.
	d.Reset()

	// Frame G of connection 2 arrives whole. It must decode on its own,
	// with no contamination and no errors.
	g := mustEncode(t, &Frame{Seq: 2, MsgID: 202, FragCnt: 1, Payload: []byte("connection-two")})
	frames, errs = d.Feed(g)
	if len(errs) != 0 {
		t.Fatalf("new connection produced errors: %v", errs)
	}
	if len(frames) != 1 || string(frames[0].Payload) != "connection-two" {
		t.Fatalf("got %d frames payload=%v, want single connection-two frame", len(frames), frames[0].Payload)
	}
	t.Logf("reset: 10 stale half-frame bytes discarded; new connection decoded %q cleanly", frames[0].Payload)
}

// TestSessionReconnectReceiveBoundary is the session-level version: a half
// frame left by a dead connection must never be stitched to the new
// connection's bytes, even when the concatenation LOOKS length-consistent.
func TestSessionReconnectReceiveBoundary(t *testing.T) {
	s := NewSession(SessionConfig{})
	t.Cleanup(func() { s.Close() })
	drainErrors(t, s)

	// F and G share LEN. F-header ++ G-payload/CRC is length-consistent but
	// has a broken CRC: without a boundary reset the decoder would parse a
	// corrupt mixed frame out of two connections' bytes.
	f := mustEncode(t, &Frame{Seq: 1, MsgID: 101, FragCnt: 1, Payload: []byte("AAAAAAAAAAAAAAAA")}) // 16 bytes
	g := mustEncode(t, &Frame{Seq: 2, MsgID: 202, FragCnt: 1, Payload: []byte("BBBBBBBBBBBBBBBB")})
	if len(f) != len(g) {
		t.Fatalf("setup: frame lengths differ %d vs %d", len(f), len(g))
	}

	// Connection 1: only F's 18-byte header arrives (LEN points at 16 bytes
	// that never come on this connection).
	c1a, c1b := net.Pipe()
	if err := s.Attach(c1a); err != nil {
		t.Fatalf("attach 1: %v", err)
	}
	if _, err := c1b.Write(f[:HeaderLen]); err != nil {
		t.Fatalf("write half header: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	before := s.SnapshotStats()
	if before.FramesDecoded != 0 {
		t.Fatalf("half header must not decode, FramesDecoded=%d", before.FramesDecoded)
	}

	// Connection 2: G arrives in full. The decoder must have forgotten the
	// F prefix: exactly G decodes, with zero errors.
	c2a, c2b := net.Pipe()
	if err := s.Attach(c2a); err != nil {
		t.Fatalf("attach 2: %v", err)
	}
	if _, err := c2b.Write(g); err != nil {
		t.Fatalf("write g: %v", err)
	}

	deadline := time.After(2 * time.Second)
	for {
		select {
		case m, ok := <-s.Delivered():
			if !ok {
				t.Fatal("delivered channel closed")
			}
			if m.MsgID != 202 || string(m.Data) != "BBBBBBBBBBBBBBBB" {
				t.Fatalf("cross-connection contamination: got msgID=%d payload=%q", m.MsgID, m.Data)
			}
			after := s.SnapshotStats()
			if after.DecodeErrors != before.DecodeErrors {
				t.Fatalf("mixed connections produced %d new decode errors", after.DecodeErrors-before.DecodeErrors)
			}
			t.Logf("receive boundary: G (msgID=202) decoded alone after reconnect; DecodeErrors stayed %d",
				after.DecodeErrors)
			return
		case err := <-s.Errors():
			t.Fatalf("unexpected error across boundary: %v", err)
		case <-deadline:
			t.Fatal("timed out waiting for connection-2 message")
		}
	}
}

// lateConn simulates a transport whose Close does NOT interrupt a blocked
// Read (some serial/embedded stacks behave like this). It lets the test
// deliver bytes from a replaced transport after the session moved on.
type lateConn struct {
	chunks    chan []byte
	endReads  chan struct{}
	closeOnce sync.Once
	wmu       sync.Mutex
	written   []byte
}

func newLateConn() *lateConn {
	return &lateConn{chunks: make(chan []byte, 8), endReads: make(chan struct{})}
}

func (c *lateConn) Read(p []byte) (int, error) {
	select {
	case b := <-c.chunks:
		if len(b) > len(p) {
			panic("test chunk larger than read buffer")
		}
		return copy(p, b), nil
	case <-c.endReads:
		return 0, io.EOF
	}
}

func (c *lateConn) Write(b []byte) (int, error) {
	c.wmu.Lock()
	c.written = append(c.written, b...)
	c.wmu.Unlock()
	return len(b), nil
}

func (c *lateConn) Close() error {
	c.closeOnce.Do(func() {}) // intentionally does not unblock Read
	return nil
}

// TestSessionStaleTransportBytesIgnored proves the connection epoch guard:
// bytes surfacing from a replaced transport are discarded without entering
// the decoder at all.
func TestSessionStaleTransportBytesIgnored(t *testing.T) {
	s := NewSession(SessionConfig{})
	drainErrors(t, s)

	stale := newLateConn()
	if err := s.Attach(stale); err != nil {
		t.Fatalf("attach stale: %v", err)
	}
	// Cleanup order matters (LIFO): s.Close is registered first so it runs
	// LAST — the stale transport's blocked Read must be released first,
	// otherwise Close would wait forever on the parked read loop.
	t.Cleanup(func() { s.Close() })
	t.Cleanup(func() { close(stale.endReads) })
	// Let the read loop park in Read.
	time.Sleep(50 * time.Millisecond)

	// Reconnect with a normal pipe; stale.Close() above did not wake its
	// read loop (by design of this mock).
	c2a, c2b := net.Pipe()
	if err := s.Attach(c2a); err != nil {
		t.Fatalf("attach 2: %v", err)
	}
	g := mustEncode(t, &Frame{Seq: 9, MsgID: 909, FragCnt: 1, Payload: []byte("fresh epoch")})
	if _, err := c2b.Write(g); err != nil {
		t.Fatalf("write fresh: %v", err)
	}

	// Meanwhile, garbage appears on the DEAD transport after it was
	// replaced. Under no circumstances may this enter the decoder.
	stale.chunks <- []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x11, 0x22}
	noiseFrame := mustEncode(t, &Frame{Seq: 1, MsgID: 111, FragCnt: 1, Payload: []byte("stale-epoch")})
	stale.chunks <- noiseFrame

	m, ok := <-s.Delivered()
	if !ok || m.MsgID != 909 || string(m.Data) != "fresh epoch" {
		t.Fatalf("want only msgID=909 fresh epoch, got %+v ok=%v", m, ok)
	}
	// Give the stale read loop a moment to process the injected chunks.
	time.Sleep(50 * time.Millisecond)
	st := s.SnapshotStats()
	if st.BytesDropped != 0 {
		t.Fatalf("stale bytes entered the decoder: BytesDropped=%d", st.BytesDropped)
	}
	if st.FramesDecoded != 1 {
		t.Fatalf("FramesDecoded=%d, want exactly 1 (fresh only)", st.FramesDecoded)
	}
	t.Logf("epoch guard: %d bytes + 1 frame from dead transport dropped before decode; fresh frame delivered",
		6+len(noiseFrame))

	// The stale read loop's eventual EOF (when endReads is closed by
	// cleanup) is quiet because the transport is no longer current.
	select {
	case err := <-s.Errors():
		t.Fatalf("stale transport produced a surfaced error: %v", err)
	case <-time.After(100 * time.Millisecond):
		t.Log("no error surfaced from dead transport bytes or read exit")
	}
}

// TestSessionReconnectFragmentContinuation documents and verifies the
// cross-connection completion rule: a message unfinished on one connection
// is completed by fragments arriving on the next one; replayed fragments
// count once and delivery happens exactly once.
func TestSessionReconnectFragmentContinuation(t *testing.T) {
	s := NewSession(SessionConfig{})
	t.Cleanup(func() { s.Close() })
	drainErrors(t, s)

	payload := bytes.Repeat([]byte("span"), 600) // 2400 bytes -> 3 fragments
	frames, _, err := Fragment(303, payload, 0)
	if err != nil {
		t.Fatalf("Fragment: %v", err)
	}
	enc := make([][]byte, len(frames))
	for i, f := range frames {
		enc[i] = mustEncode(t, f)
	}

	// Connection 1: fragment 0 only, then the link dies.
	c1a, c1b := net.Pipe()
	if err := s.Attach(c1a); err != nil {
		t.Fatalf("attach 1: %v", err)
	}
	if _, err := c1b.Write(enc[0]); err != nil {
		t.Fatalf("frag0 write: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	select {
	case m := <-s.Delivered():
		t.Fatalf("incomplete message delivered across boundary: %+v", m)
	default:
	}

	// Connection 2: peer replays frag 0 and sends frag 1 and frag 2.
	c2a, c2b := net.Pipe()
	if err := s.Attach(c2a); err != nil {
		t.Fatalf("attach 2: %v", err)
	}
	for _, b := range enc {
		if _, err := c2b.Write(b); err != nil {
			t.Fatalf("rewrite frag: %v", err)
		}
	}

	deadline := time.After(2 * time.Second)
	for {
		select {
		case m, ok := <-s.Delivered():
			if !ok {
				t.Fatal("channel closed")
			}
			if m.MsgID != 303 || !bytes.Equal(m.Data, payload) {
				t.Fatalf("reassembled mismatch: id=%d len=%d", m.MsgID, len(m.Data))
			}
			// Exactly once: wait briefly for an accidental second delivery.
			select {
			case extra := <-s.Delivered():
				t.Fatalf("message delivered twice: %+v", extra)
			case <-time.After(100 * time.Millisecond):
			}
			st := s.SnapshotStats()
			t.Logf("continuation: 1 frag on conn1 + replay+frags on conn2 -> delivered once, %d bytes; DuplicateFragments=%d",
				len(m.Data), st.DuplicateFragments)
			return
		case err := <-s.Errors():
			t.Fatalf("unexpected error: %v", err)
		case <-deadline:
			t.Fatal("timed out waiting for cross-connection completion")
		}
	}
}
