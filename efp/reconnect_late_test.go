package efp

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

// scriptedConn is an in-memory transport whose reads are controlled
// explicitly by the test, which makes "bytes of the old connection are
// read only after the switch to the new one" reproducible instead of
// timing-dependent:
//
//   - feed(b) stages bytes that a blocked or future Read returns
//     immediately (normal traffic);
//   - armFinal(b) stages bytes that are returned by the Read that
//     observes Close — modelling a transport that hands its final
//     buffered bytes to the reader together with the close (Read
//     returns n>0, possibly with io.EOF). Arming happens before Close,
//     so the interleaving is deterministic.
type scriptedConn struct {
	readCh chan []byte
	done   chan struct{}
	once   sync.Once

	mu      sync.Mutex
	pending []byte // remainder of a fed chunk larger than one Read buffer
	final   []byte // staged by armFinal, delivered as the last read(s)
}

func newScriptedConn() *scriptedConn {
	return &scriptedConn{readCh: make(chan []byte, 16), done: make(chan struct{})}
}

// feed enqueues bytes to be returned by future Read calls.
func (c *scriptedConn) feed(b []byte) { c.readCh <- b }

// armFinal stages bytes to be returned by the Read that observes Close.
func (c *scriptedConn) armFinal(b []byte) {
	c.mu.Lock()
	c.final = b
	c.mu.Unlock()
}

func (c *scriptedConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	if len(c.pending) > 0 {
		n := copy(p, c.pending)
		c.pending = c.pending[n:]
		c.mu.Unlock()
		return n, nil
	}
	c.mu.Unlock()
	select {
	case b := <-c.readCh:
		n := copy(p, b)
		if n < len(b) {
			c.mu.Lock()
			c.pending = append(c.pending, b[n:]...)
			c.mu.Unlock()
		}
		return n, nil
	case <-c.done:
		// Closed: deliver the armed final bytes (if any) before
		// reporting EOF, like a transport flushing its last buffer.
		c.mu.Lock()
		defer c.mu.Unlock()
		if len(c.final) > 0 {
			n := copy(p, c.final)
			c.final = c.final[n:]
			if len(c.final) == 0 {
				return n, io.EOF
			}
			return n, nil
		}
		return 0, io.EOF
	}
}

// Write discards (the session's hello frames land here).
func (c *scriptedConn) Write(p []byte) (int, error) { return len(p), nil }

func (c *scriptedConn) Close() error {
	c.once.Do(func() { close(c.done) })
	return nil
}

// TestSessionReconnectLateBytesFromOldConnection: the old connection's
// read loop is still blocked in Read when the session switches to a new
// transport; its Read returns the connection's final buffered bytes only
// AFTER the switch. Those bytes must be dropped whole: not parsed, not
// delivered, and never combined with the new connection's bytes.
func TestSessionReconnectLateBytesFromOldConnection(t *testing.T) {
	s := NewSession(SessionConfig{})
	t.Cleanup(func() { s.Close() })

	frameOld := mustEncode(t, &Frame{Seq: 0, MsgID: 511, FragCnt: 1, Payload: []byte("stale-from-conn-1")})
	t.Logf("input: connection 1 has a complete %d-byte frame buffered; it surfaces only after the switch",
		len(frameOld))

	c1 := newScriptedConn()
	if err := s.Attach(c1); err != nil {
		t.Fatalf("attach 1: %v", err)
	}
	// Stage the old connection's last bytes, then switch. The old read
	// loop reads them only now, already belonging to a stale generation.
	c1.armFinal(frameOld)
	c2 := newScriptedConn()
	if err := s.Attach(c2); err != nil {
		t.Fatalf("attach 2: %v", err)
	}
	t.Log("switched to connection 2; only now connection 1's blocked Read returns its final bytes")

	waitFor(t, "late bytes dropped", 2*time.Second, func() bool {
		return s.Stats().StaleBytesDropped == uint64(len(frameOld))
	})
	t.Logf("connection 1: %d late bytes dropped whole (Stats.StaleBytesDropped)", len(frameOld))

	select {
	case m := <-s.Delivered():
		t.Fatalf("stale frame from old connection delivered: msgID=%d data=%q", m.MsgID, m.Data)
	case err := <-s.Errors():
		t.Fatalf("stale bytes must be dropped silently, got error: %v", err)
	default:
		t.Log("connection 1: stale frame neither delivered nor parsed (correct)")
	}
	if got := s.Stats().FramesDecoded; got != 0 {
		t.Fatalf("FramesDecoded=%d, want 0: stale bytes must not reach the decoder", got)
	}

	// The new connection's decoder must be unpolluted: a frame on
	// connection 2 decodes exactly as sent.
	frameNew := mustEncode(t, &Frame{Seq: 1, MsgID: 512, FragCnt: 1, Payload: []byte("fresh-on-conn-2")})
	c2.feed(frameNew)
	got := recvN(t, s, 1, 2*time.Second)
	if got[0].MsgID != 512 || string(got[0].Data) != "fresh-on-conn-2" {
		t.Fatalf("delivered %+v, want msgID=512 payload=fresh-on-conn-2", got[0])
	}
	t.Logf("verdict: only connection 2's own frame delivered (%q); stale bytes never entered its stream",
		got[0].Data)
}

// TestSessionReconnectLateHalfFrameCannotComplete: the first half of a
// frame surfaces late on the old connection (read after the switch); the
// matching second half arrives on the new connection. The two halves
// must NOT combine into a complete frame: the late half is dropped, the
// orphan half on the new connection is noise.
func TestSessionReconnectLateHalfFrameCannotComplete(t *testing.T) {
	s := NewSession(SessionConfig{})
	t.Cleanup(func() { s.Close() })

	frameA := mustEncode(t, &Frame{Seq: 0, MsgID: 521, FragCnt: 1, Payload: []byte("AAAA-split-across-conns")})
	half := len(frameA) / 2
	t.Logf("input: frame A (%d bytes) split at %d; first half late on conn 1, second half on conn 2",
		len(frameA), half)

	c1 := newScriptedConn()
	if err := s.Attach(c1); err != nil {
		t.Fatalf("attach 1: %v", err)
	}
	c1.armFinal(frameA[:half])
	c2 := newScriptedConn()
	if err := s.Attach(c2); err != nil {
		t.Fatalf("attach 2: %v", err)
	}
	waitFor(t, "late half dropped", 2*time.Second, func() bool {
		return s.Stats().StaleBytesDropped == uint64(half)
	})
	t.Logf("connection 1: %d late bytes dropped whole after the switch", half)

	// Second half arrives on the NEW connection: without the dropped
	// first half it is just noise, not a completable frame. Then a
	// complete valid frame B on connection 2.
	c2.feed(frameA[half:])
	frameB := mustEncode(t, &Frame{Seq: 1, MsgID: 522, FragCnt: 1, Payload: []byte("BBBB-on-conn-2")})
	c2.feed(frameB)
	t.Log("connection 2: second half of frame A (must be noise) + complete frame B")

	var sawNoise bool
	deadline := time.After(2 * time.Second)
	for {
		select {
		case m := <-s.Delivered():
			if m.MsgID == 521 || bytes.Contains(m.Data, []byte("AAAA")) {
				t.Fatalf("frame fabricated across connections: msgID=%d data=%q", m.MsgID, m.Data)
			}
			if m.MsgID != 522 || string(m.Data) != "BBBB-on-conn-2" {
				t.Fatalf("delivered %+v, want msgID=522 payload=BBBB-on-conn-2", m)
			}
			if !sawNoise {
				t.Fatal("expected the orphan half-frame on connection 2 to be reported as noise")
			}
			t.Logf("verdict: only frame B delivered (%q); split frame A never materialized", m.Data)
			return
		case err := <-s.Errors():
			if errors.Is(err, ErrBadMagic) {
				sawNoise = true
				t.Logf("connection 2: orphan half-frame skipped as noise: %v", err)
			}
		case <-deadline:
			t.Fatal("timeout waiting for frame B")
		}
	}
}

// TestSessionReconnectLateBytesKeepInflightMessage: dropping the old
// connection's late bytes must NOT drop the incomplete message that
// connection was reassembling. The remaining fragments arrive on the new
// connection and complete it; it is delivered exactly once.
func TestSessionReconnectLateBytesKeepInflightMessage(t *testing.T) {
	s := NewSession(SessionConfig{})
	t.Cleanup(func() { s.Close() })

	payload := bytes.Repeat([]byte("chunk"), 500) // 2500 bytes -> 3 fragments
	frames, _, err := Fragment(601, payload, 0)
	if err != nil {
		t.Fatalf("Fragment: %v", err)
	}
	enc := make([][]byte, len(frames))
	for i, f := range frames {
		enc[i] = mustEncode(t, f)
	}
	t.Logf("input: message 601 = %d bytes in %d fragments; frag 0 on conn 1, "+
		"frag 1 late on conn 1 (dropped), frags 1..2 on conn 2", len(payload), len(enc))

	// Connection 1 delivers fragment 0 before the switch.
	c1 := newScriptedConn()
	if err := s.Attach(c1); err != nil {
		t.Fatalf("attach 1: %v", err)
	}
	c1.feed(enc[0])
	waitFor(t, "fragment 0 decoded", 2*time.Second, func() bool {
		return s.Stats().FramesDecoded == 1
	})
	select {
	case m := <-s.Delivered():
		t.Fatalf("incomplete message delivered: %+v", m)
	default:
		t.Logf("connection 1: fragment 0/%d buffered, nothing delivered (correct)", frames[0].FragCnt)
	}

	// Stage fragment 1 as the old connection's final bytes and switch:
	// it is read after the switch and must be dropped at the byte
	// boundary — while the incomplete message 601 survives.
	c1.armFinal(enc[1])
	c2 := newScriptedConn()
	if err := s.Attach(c2); err != nil {
		t.Fatalf("attach 2: %v", err)
	}
	waitFor(t, "late fragment dropped", 2*time.Second, func() bool {
		return s.Stats().StaleBytesDropped == uint64(len(enc[1]))
	})
	t.Logf("connection 1: late fragment 1 (%d bytes) dropped at the byte boundary", len(enc[1]))

	// The peer resends fragment 1 and finishes with fragment 2 on the new
	// connection; the incomplete message from connection 1 must complete.
	c2.feed(enc[1])
	c2.feed(enc[2])
	t.Log("connection 2: fragments 1..2 complete the cross-connection message")

	got := recvN(t, s, 1, 2*time.Second)
	if got[0].MsgID != 601 || !bytes.Equal(got[0].Data, payload) {
		t.Fatalf("reassembled mismatch: id=%d len=%d", got[0].MsgID, len(got[0].Data))
	}
	select {
	case m := <-s.Delivered():
		t.Fatalf("message delivered twice: msgID=%d", m.MsgID)
	case <-time.After(150 * time.Millisecond):
	}
	t.Logf("verdict: message 601 completed across connections (%d bytes), delivered exactly once, "+
		"late old-connection bytes dropped without killing it", len(got[0].Data))
}
