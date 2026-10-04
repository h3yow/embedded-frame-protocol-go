package efp

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"
)

// TestSessionReconnectReceiveBoundary: bytes buffered from a dead
// connection must never combine with bytes of the new connection into a
// frame. A half frame received on connection 1 is discarded at Attach;
// its second half arriving on connection 2 is treated as noise, and a
// complete frame on connection 2 decodes exactly as sent.
func TestSessionReconnectReceiveBoundary(t *testing.T) {
	s := NewSession(SessionConfig{})
	t.Cleanup(func() { s.Close() })

	frameA := mustEncode(t, &Frame{Seq: 0, MsgID: 501, FragCnt: 1, Payload: []byte("AAAA-from-conn-1")})
	half := len(frameA) / 2
	t.Logf("input: connection 1 receives first %d of %d bytes of frame A", half, len(frameA))

	c1a, c1b := net.Pipe()
	drainPeer(t, c1b)
	if err := s.Attach(c1a); err != nil {
		t.Fatalf("attach 1: %v", err)
	}
	if _, err := c1b.Write(frameA[:half]); err != nil {
		t.Fatalf("write half frame: %v", err)
	}
	time.Sleep(100 * time.Millisecond) // let the read loop buffer the half frame
	select {
	case m := <-s.Delivered():
		t.Fatalf("half frame must not deliver anything, got %+v", m)
	default:
		t.Log("connection 1: half frame buffered, nothing delivered (correct)")
	}

	// Reconnect. The second half of frame A now arrives on connection 2:
	// it must NOT complete a frame (that would fabricate a message from
	// two connections' bytes).
	c2a, c2b := net.Pipe()
	drainPeer(t, c2b)
	if err := s.Attach(c2a); err != nil {
		t.Fatalf("attach 2: %v", err)
	}
	if _, err := c2b.Write(frameA[half:]); err != nil {
		t.Fatalf("write second half: %v", err)
	}
	t.Logf("input: connection 2 receives the remaining %d bytes of frame A (must be noise)", len(frameA)-half)

	// Then a complete, valid frame B on connection 2.
	frameB := mustEncode(t, &Frame{Seq: 1, MsgID: 502, FragCnt: 1, Payload: []byte("BBBB-from-conn-2")})
	if _, err := c2b.Write(frameB); err != nil {
		t.Fatalf("write frame B: %v", err)
	}

	var sawNoise bool
	deadline := time.After(2 * time.Second)
	for {
		select {
		case m := <-s.Delivered():
			if m.MsgID == 501 || bytes.Contains(m.Data, []byte("AAAA")) {
				t.Fatalf("frame fabricated across connections: msgID=%d data=%q", m.MsgID, m.Data)
			}
			if m.MsgID != 502 || string(m.Data) != "BBBB-from-conn-2" {
				t.Fatalf("delivered %+v, want msgID=502 payload=BBBB-from-conn-2", m)
			}
			if !sawNoise {
				t.Fatal("expected the cross-connection leftover bytes to be reported as noise")
			}
			t.Logf("verdict: only frame B delivered (%q); frame A never materialized", m.Data)
			return
		case err := <-s.Errors():
			if errors.Is(err, ErrBadMagic) {
				sawNoise = true
				t.Logf("connection 2: leftover bytes of frame A skipped as noise: %v", err)
			}
		case <-deadline:
			t.Fatal("timeout waiting for frame B")
		}
	}
}

// TestSessionReconnectCompletesWithRemainingFragments: an incomplete
// message from connection 1 is completed by ONLY the remaining fragments
// sent on connection 2 (the peer does not resend what it already sent).
func TestSessionReconnectCompletesWithRemainingFragments(t *testing.T) {
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
	t.Logf("input: message 601 = %d bytes in %d fragments", len(payload), len(enc))

	// Connection 1 delivers only fragment 0.
	c1a, c1b := net.Pipe()
	drainPeer(t, c1b)
	if err := s.Attach(c1a); err != nil {
		t.Fatalf("attach 1: %v", err)
	}
	if _, err := c1b.Write(enc[0]); err != nil {
		t.Fatalf("write frag0: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	select {
	case m := <-s.Delivered():
		t.Fatalf("incomplete message delivered: %+v", m)
	default:
		t.Logf("connection 1: fragment 0/%d buffered, nothing delivered (correct)", frames[0].FragCnt)
	}

	// Reconnect; the peer sends ONLY the remaining fragments 1..2.
	c2a, c2b := net.Pipe()
	drainPeer(t, c2b)
	if err := s.Attach(c2a); err != nil {
		t.Fatalf("attach 2: %v", err)
	}
	for _, b := range enc[1:] {
		if _, err := c2b.Write(b); err != nil {
			t.Fatalf("write remaining frag: %v", err)
		}
	}
	t.Log("connection 2: only remaining fragments 1..2 sent")

	got := recvN(t, s, 1, 2*time.Second)
	if got[0].MsgID != 601 || !bytes.Equal(got[0].Data, payload) {
		t.Fatalf("reassembled mismatch: id=%d len=%d", got[0].MsgID, len(got[0].Data))
	}
	t.Logf("verdict: message 601 completed across connections, %d bytes, delivered exactly once",
		len(got[0].Data))
}
