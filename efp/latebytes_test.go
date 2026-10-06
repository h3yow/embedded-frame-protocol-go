package efp

import (
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// scriptConn is a test transport whose reads are fully scripted by the
// test: Read blocks until the test pushes a readResult. Crucially, Close
// does NOT unblock a pending Read — this models a transport whose
// already-in-flight read returns bytes only AFTER the session has moved
// on to a new connection, the exact reconnect race under test. Writes
// are recorded and always succeed.
type scriptConn struct {
	reads   chan readResult
	mu      sync.Mutex
	written [][]byte
	closed  bool
}

type readResult struct {
	data []byte
	err  error
}

func newScriptConn() *scriptConn {
	// Buffered so pushes never block even if the read loop already exited.
	return &scriptConn{reads: make(chan readResult, 16)}
}

func (c *scriptConn) Read(p []byte) (int, error) {
	rr, ok := <-c.reads
	if !ok {
		return 0, io.ErrClosedPipe
	}
	return copy(p, rr.data), rr.err
}

func (c *scriptConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b := make([]byte, len(p))
	copy(b, p)
	c.written = append(c.written, b)
	return len(p), nil
}

// Close only flips the flag; pending and future Reads keep following the
// script until the test closes the reads channel.
func (c *scriptConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

// feed scripts one read result (bytes the connection "receives").
func (c *scriptConn) feed(data []byte) { c.reads <- readResult{data: data} }

// endReads terminates the read script: the read loop sees a closed
// transport and exits.
func (c *scriptConn) endReads() { close(c.reads) }

// TestSessionReconnectLateBytesFromOldConnection reproduces, with no
// sleeps or luck involved, the race where bytes of the OLD connection
// are read only AFTER the session switched to a new connection. Every
// such byte must be dropped whole: it may neither complete the half
// frame buffered from the old connection nor enter the new connection's
// stream, and even a complete well-formed frame arriving late on the old
// connection must not be delivered.
func TestSessionReconnectLateBytesFromOldConnection(t *testing.T) {
	s := NewSession(SessionConfig{})
	t.Cleanup(func() { s.Close() })

	frameA := mustEncode(t, &Frame{Seq: 0, MsgID: 501, FragCnt: 1, Payload: []byte("AAAA-half-buffered")})
	half := len(frameA) / 2

	// Connection 1 (scripted): buffer the first half of frame A in the
	// decoder. PartialFrameBytes reaching len(half) proves the read loop
	// consumed it — no sleeps, no guessing.
	c1 := newScriptConn()
	t.Cleanup(c1.endReads)
	if err := s.Attach(c1); err != nil {
		t.Fatalf("attach 1: %v", err)
	}
	c1.feed(frameA[:half])
	waitFor(t, "half of frame A buffered in decoder", 2*time.Second, func() bool {
		return s.Stats().PartialFrameBytes == uint64(half)
	})
	select {
	case m := <-s.Delivered():
		t.Fatalf("half frame must not deliver anything, got %+v", m)
	default:
		t.Logf("connection 1: %d bytes (half of frame A) sit buffered in the decoder", half)
	}

	// Switch to connection 2. The decoder is reset; c1 is closed, but
	// its scripted read stays pending — the late bytes below are read
	// strictly AFTER the switch, in program order.
	c2a, c2b := net.Pipe()
	drainPeer(t, c2b)
	if err := s.Attach(c2a); err != nil {
		t.Fatalf("attach 2: %v", err)
	}
	if n := s.Stats().PartialFrameBytes; n != 0 {
		t.Fatalf("decoder buffer not reset at Attach: %d bytes remain", n)
	}

	// Late bytes on the OLD connection: first the second half of frame A
	// (would complete a cross-connection Franken-frame if it reached the
	// decoder), then a complete well-formed frame A2.
	frameA2 := mustEncode(t, &Frame{Seq: 2, MsgID: 501, FragCnt: 1, Payload: []byte("AAAA-complete-late")})
	c1.feed(frameA[half:])
	c1.feed(frameA2)
	late := len(frameA) - half + len(frameA2)
	t.Logf("after switch: old connection delivers %d late bytes (rest of frame A + complete frame A2)", late)

	waitFor(t, "late bytes dropped", 2*time.Second, func() bool {
		return s.Stats().StaleConnBytesDropped == uint64(late)
	})
	t.Logf("stats: StaleConnBytesDropped=%d (all late bytes rejected)", s.Stats().StaleConnBytesDropped)

	select {
	case m := <-s.Delivered():
		t.Fatalf("late bytes leaked into delivery: msgID=%d data=%q", m.MsgID, m.Data)
	default:
		t.Log("nothing delivered from the old connection's late bytes (correct)")
	}

	// The new connection is unaffected: frame B decodes exactly as sent.
	frameB := mustEncode(t, &Frame{Seq: 3, MsgID: 502, FragCnt: 1, Payload: []byte("BBBB-from-conn-2")})
	if _, err := c2b.Write(frameB); err != nil {
		t.Fatalf("write frame B: %v", err)
	}
	got := recvN(t, s, 1, 2*time.Second)
	if got[0].MsgID != 502 || string(got[0].Data) != "BBBB-from-conn-2" {
		t.Fatalf("got %+v, want msgID=502 payload=BBBB-from-conn-2", got[0])
	}
	t.Logf("verdict: only frame B delivered (%q); frame A never materialized in any form", got[0].Data)

	// Stale drops are silent (counted, not errors).
	select {
	case err := <-s.Errors():
		t.Fatalf("stale-byte drop must not surface as an error, got %v", err)
	default:
	}
}

// TestSessionReconnectLateBytesDoNotCompleteInflightMessage: the
// late-byte discard must not take down a big message still being
// reassembled — it survives the switch and is completed by the remaining
// fragments on the new connection, delivered exactly once. A forged
// "remaining fragment" read late from the OLD connection must NOT
// complete it (it carries corrupted payload to make any leak visible).
func TestSessionReconnectLateBytesDoNotCompleteInflightMessage(t *testing.T) {
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
	ping := mustEncode(t, &Frame{Seq: 9, MsgID: 600, FragCnt: 1, Payload: []byte("sync")})
	t.Logf("input: message 601 = %d bytes in %d fragments; fragment 0 arrives on connection 1", len(payload), len(enc))

	// Connection 1: fragment 0 of message 601, then a sync frame whose
	// delivery proves fragment 0 was consumed into the reassembler.
	c1 := newScriptConn()
	t.Cleanup(c1.endReads)
	if err := s.Attach(c1); err != nil {
		t.Fatalf("attach 1: %v", err)
	}
	c1.feed(enc[0])
	c1.feed(ping)
	got := recvN(t, s, 1, 2*time.Second)
	if got[0].MsgID != 600 {
		t.Fatalf("got %+v, want sync msgID=600", got[0])
	}
	t.Logf("connection 1: fragment 0/%d of message 601 in flight across the reconnect", frames[0].FragCnt)

	// Switch connections.
	c2a, c2b := net.Pipe()
	drainPeer(t, c2b)
	if err := s.Attach(c2a); err != nil {
		t.Fatalf("attach 2: %v", err)
	}

	// Late on the OLD connection: a forged fragment 1 of message 601
	// with CORRUPTED content (valid CRC — it is a well-formed frame, just
	// not from this connection). If it leaked into the reassembler, the
	// message would complete with garbage.
	forged := mustEncode(t, &Frame{
		Seq: 10, MsgID: 601, Flags: FlagFragment,
		FragIdx: 1, FragCnt: frames[0].FragCnt,
		Payload: bytes.Repeat([]byte("EVIL"), 256),
	})
	c1.feed(forged)
	t.Logf("after switch: old connection delivers a forged fragment 1 (%d bytes, corrupted content)", len(forged))
	waitFor(t, "forged fragment dropped", 2*time.Second, func() bool {
		return s.Stats().StaleConnBytesDropped == uint64(len(forged))
	})
	select {
	case m := <-s.Delivered():
		t.Fatalf("forged late fragment completed message 601: id=%d len=%d", m.MsgID, len(m.Data))
	default:
		t.Log("forged fragment dropped; message 601 still incomplete (correct)")
	}

	// The new connection supplies ONLY the genuine remaining fragments.
	for _, b := range enc[1:] {
		if _, err := c2b.Write(b); err != nil {
			t.Fatalf("write remaining frag: %v", err)
		}
	}
	t.Log("connection 2: genuine fragments 1..2 sent")
	got = recvN(t, s, 1, 2*time.Second)
	if got[0].MsgID != 601 || !bytes.Equal(got[0].Data, payload) {
		t.Fatalf("reassembled mismatch: id=%d len=%d (want id=601 len=%d)",
			got[0].MsgID, len(got[0].Data), len(payload))
	}
	t.Logf("verdict: message 601 completed across connections, %d bytes intact, delivered exactly once",
		len(got[0].Data))

	select {
	case m := <-s.Delivered():
		t.Fatalf("message 601 delivered twice: id=%d len=%d", m.MsgID, len(m.Data))
	case <-time.After(150 * time.Millisecond):
	}
}
