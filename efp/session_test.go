package efp

import (
	"bytes"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// pipePair returns two sessions connected by a net.Pipe.
func pipePair(t *testing.T) (*Session, *Session) {
	t.Helper()
	c1, c2 := net.Pipe()
	s1 := NewSession(SessionConfig{})
	s2 := NewSession(SessionConfig{})
	if err := s1.Attach(c1); err != nil {
		t.Fatalf("attach s1: %v", err)
	}
	if err := s2.Attach(c2); err != nil {
		t.Fatalf("attach s2: %v", err)
	}
	t.Cleanup(func() { s1.Close(); s2.Close() })
	return s1, s2
}

func recvN(t *testing.T, s *Session, n int, timeout time.Duration) []ReassembledMessage {
	t.Helper()
	var out []ReassembledMessage
	deadline := time.After(timeout)
	for len(out) < n {
		select {
		case m, ok := <-s.Delivered():
			if !ok {
				t.Fatalf("delivered channel closed after %d/%d messages", len(out), n)
			}
			out = append(out, m)
		case err := <-s.Errors():
			t.Fatalf("unexpected session error: %v", err)
		case <-deadline:
			t.Fatalf("timeout after %d/%d messages", len(out), n)
		}
	}
	return out
}

func TestSessionEndToEnd(t *testing.T) {
	s1, s2 := pipePair(t)
	payload := bytes.Repeat([]byte("end-to-end "), 300) // 3300 bytes -> fragmented
	id, err := s1.SendMessage(payload)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	msgs := recvN(t, s2, 1, 2*time.Second)
	if msgs[0].MsgID != id || !bytes.Equal(msgs[0].Data, payload) {
		t.Fatalf("delivered mismatch: id=%d len=%d", msgs[0].MsgID, len(msgs[0].Data))
	}
	t.Logf("end-to-end: msgID=%d, %d bytes delivered intact", id, len(msgs[0].Data))
}

func TestSessionConcurrentSend(t *testing.T) {
	s1, s2 := pipePair(t)
	const senders = 8
	const perSender = 5
	want := make(map[uint32][]byte)
	var mu sync.Mutex

	var wg sync.WaitGroup
	for g := 0; g < senders; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perSender; i++ {
				p := []byte(fmt.Sprintf("goroutine-%d-msg-%d", g, i))
				id, err := s1.SendMessage(p)
				if err != nil {
					t.Errorf("SendMessage: %v", err)
					return
				}
				mu.Lock()
				want[id] = p
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()

	got := recvN(t, s2, senders*perSender, 5*time.Second)
	seen := make(map[uint32]int)
	for _, m := range got {
		seen[m.MsgID]++
		mu.Lock()
		w, ok := want[m.MsgID]
		mu.Unlock()
		if !ok || !bytes.Equal(m.Data, w) {
			t.Fatalf("msgID=%d payload=%q, want %q", m.MsgID, m.Data, w)
		}
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("msgID=%d delivered %d times, want exactly 1", id, n)
		}
	}
	t.Logf("concurrent send: %d goroutines x %d messages, all %d delivered exactly once, no decode errors",
		senders, perSender, len(got))
}

// TestSessionReconnectReplay verifies that frames replayed after a
// reconnect are deduplicated and never delivered twice.
func TestSessionReconnectReplay(t *testing.T) {
	s := NewSession(SessionConfig{})
	t.Cleanup(func() { s.Close() })

	// Build the raw bytes of one application message as a peer would send.
	frames, _, err := Fragment(77, []byte("replay me"), 0)
	if err != nil {
		t.Fatalf("Fragment: %v", err)
	}
	var wire []byte
	for _, f := range frames {
		wire = append(wire, mustEncode(t, f)...)
	}

	// Connection 1: peer sends the message.
	c1a, c1b := net.Pipe()
	if err := s.Attach(c1a); err != nil {
		t.Fatalf("attach 1: %v", err)
	}
	if _, err := c1b.Write(wire); err != nil {
		t.Fatalf("write 1: %v", err)
	}
	first := recvN(t, s, 1, 2*time.Second)
	t.Logf("connection 1: delivered msgID=%d payload=%q", first[0].MsgID, first[0].Data)

	// "Reconnect": attach a fresh transport; the peer replays the same bytes.
	c2a, c2b := net.Pipe()
	if err := s.Attach(c2a); err != nil {
		t.Fatalf("attach 2: %v", err)
	}
	if _, err := c2b.Write(wire); err != nil {
		t.Fatalf("write 2: %v", err)
	}

	// The replayed message must NOT be delivered again.
	select {
	case m := <-s.Delivered():
		t.Fatalf("replayed message delivered twice: msgID=%d", m.MsgID)
	case err := <-s.Errors():
		t.Fatalf("unexpected error after replay: %v", err)
	case <-time.After(300 * time.Millisecond):
		t.Logf("reconnect replay: duplicate msgID=77 dropped by dedup window, not delivered twice")
	}

	// A NEW message on the new connection must still be delivered.
	frames2, _, _ := Fragment(78, []byte("fresh"), 1)
	var wire2 []byte
	for _, f := range frames2 {
		wire2 = append(wire2, mustEncode(t, f)...)
	}
	if _, err := c2b.Write(wire2); err != nil {
		t.Fatalf("write 3: %v", err)
	}
	got := recvN(t, s, 1, 2*time.Second)
	if got[0].MsgID != 78 || string(got[0].Data) != "fresh" {
		t.Fatalf("got %+v, want msgID=78 payload=fresh", got[0])
	}
	t.Logf("post-reconnect: new message msgID=78 delivered normally")
}

// TestSessionReconnectCompletesFragmentedMessage verifies that an
// unfinished message can be completed by fragments arriving on the new
// transport after reconnect.
func TestSessionReconnectCompletesFragmentedMessage(t *testing.T) {
	s := NewSession(SessionConfig{})
	t.Cleanup(func() { s.Close() })

	payload := bytes.Repeat([]byte("frag"), 600) // 2400 bytes -> 3 fragments
	frames, _, err := Fragment(88, payload, 0)
	if err != nil {
		t.Fatalf("Fragment: %v", err)
	}
	enc := make([][]byte, len(frames))
	for i, f := range frames {
		enc[i] = mustEncode(t, f)
	}

	// Connection 1 delivers only the first fragment.
	c1a, c1b := net.Pipe()
	if err := s.Attach(c1a); err != nil {
		t.Fatalf("attach 1: %v", err)
	}
	if _, err := c1b.Write(enc[0]); err != nil {
		t.Fatalf("write frag0: %v", err)
	}
	// Give the read loop a moment to consume fragment 0.
	time.Sleep(50 * time.Millisecond)
	select {
	case m := <-s.Delivered():
		t.Fatalf("incomplete message delivered: %+v", m)
	default:
		t.Logf("connection 1: fragment 0/%d received, nothing delivered (correct)", frames[0].FragCnt)
	}

	// Reconnect; the peer resends ALL fragments (including a replay of frag 0).
	c2a, c2b := net.Pipe()
	if err := s.Attach(c2a); err != nil {
		t.Fatalf("attach 2: %v", err)
	}
	for _, b := range enc {
		if _, err := c2b.Write(b); err != nil {
			t.Fatalf("rewrite: %v", err)
		}
	}
	got := recvN(t, s, 1, 2*time.Second)
	if got[0].MsgID != 88 || !bytes.Equal(got[0].Data, payload) {
		t.Fatalf("reassembled after reconnect mismatch: id=%d len=%d",
			got[0].MsgID, len(got[0].Data))
	}
	t.Logf("reconnect: message 88 completed from replayed fragments, %d bytes, delivered exactly once",
		len(got[0].Data))
}

func TestSessionSendWithoutTransport(t *testing.T) {
	s := NewSession(SessionConfig{})
	t.Cleanup(func() { s.Close() })
	_, err := s.SendMessage([]byte("no conn"))
	if err == nil {
		t.Fatal("expected error sending without transport")
	}
	t.Logf("send without transport rejected: %v", err)
}

func TestSessionCloseIdempotent(t *testing.T) {
	s := NewSession(SessionConfig{})
	if err := s.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := s.Close(); err != ErrSessionClosed {
		t.Fatalf("second Close: got %v, want ErrSessionClosed", err)
	}
	if _, err := s.SendMessage([]byte("x")); err != ErrSessionClosed {
		t.Fatalf("SendMessage after Close: got %v, want ErrSessionClosed", err)
	}
	t.Log("close is idempotent; post-close operations return ErrSessionClosed")
}

// Ensure io.EOF on the transport does not surface as a spurious error.
func TestSessionPeerCloseQuiet(t *testing.T) {
	c1, c2 := net.Pipe()
	s := NewSession(SessionConfig{})
	t.Cleanup(func() { s.Close() })
	if err := s.Attach(c1); err != nil {
		t.Fatalf("attach: %v", err)
	}
	c2.Close()
	select {
	case err := <-s.Errors():
		t.Fatalf("unexpected error after peer close: %v", err)
	case <-time.After(200 * time.Millisecond):
		t.Log("peer close (EOF) handled quietly")
	}
}
