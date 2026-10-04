package efp

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// waitFor polls cond until it holds or the timeout expires.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			t.Logf("condition %q satisfied", what)
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

// drainPeer discards everything the session writes to the peer end of the
// pipe (e.g. hello control frames) so writes never block.
func drainPeer(t *testing.T, c net.Conn) {
	t.Helper()
	go func() { _, _ = io.Copy(io.Discard, c) }()
	t.Cleanup(func() { c.Close() })
}

func capsSession(t *testing.T, maxMsgSize, maxInFlight int) *Session {
	t.Helper()
	cfg := SessionConfig{Reassembly: ReassemblyConfig{
		MaxMessageSize:  maxMsgSize,
		MaxInFlight:     maxInFlight,
		FragmentTimeout: 5 * time.Second,
	}}
	s := NewSession(cfg)
	t.Cleanup(func() { s.Close() })
	return s
}

// TestSessionCapabilityNegotiation: both ends learn each other's receive
// limits via hello frames after connect.
func TestSessionCapabilityNegotiation(t *testing.T) {
	c1, c2 := net.Pipe()
	s1 := capsSession(t, 4096, 4)
	s2 := capsSession(t, 2048, 2)
	if err := s1.Attach(c1); err != nil {
		t.Fatalf("attach s1: %v", err)
	}
	if err := s2.Attach(c2); err != nil {
		t.Fatalf("attach s2: %v", err)
	}
	t.Logf("input: s1 advertises {maxMsg=4096, inFlight=4}, s2 advertises {maxMsg=2048, inFlight=2}")

	waitFor(t, "s1 learns s2 caps", 2*time.Second, func() bool {
		return s1.PeerCaps().Negotiated
	})
	waitFor(t, "s2 learns s1 caps", 2*time.Second, func() bool {
		return s2.PeerCaps().Negotiated
	})

	pc1, pc2 := s1.PeerCaps(), s2.PeerCaps()
	t.Logf("s1 sees peer caps %+v; s2 sees peer caps %+v", pc1, pc2)
	if pc1.MaxMessageSize != 2048 || pc1.MaxInFlight != 2 {
		t.Fatalf("s1.PeerCaps()=%+v, want {2048 2 negotiated}", pc1)
	}
	if pc2.MaxMessageSize != 4096 || pc2.MaxInFlight != 4 {
		t.Fatalf("s2.PeerCaps()=%+v, want {4096 4 negotiated}", pc2)
	}
	t.Log("verdict: both sides negotiated each other's limits correctly")
}

// TestSessionSendExceedsPeerLimit: a message larger than the peer's
// advertised limit fails at send time with ErrExceedsPeerLimit; nothing
// is delivered on the other side. A message within the limit goes through.
func TestSessionSendExceedsPeerLimit(t *testing.T) {
	c1, c2 := net.Pipe()
	s1 := capsSession(t, 64*1024, 8)
	s2 := capsSession(t, 100, 8)
	if err := s1.Attach(c1); err != nil {
		t.Fatalf("attach s1: %v", err)
	}
	if err := s2.Attach(c2); err != nil {
		t.Fatalf("attach s2: %v", err)
	}
	waitFor(t, "s1 learns s2 limit 100", 2*time.Second, func() bool {
		return s1.PeerCaps().Negotiated
	})

	tooBig := make([]byte, 200)
	t.Logf("input: send %d bytes against peer limit %d", len(tooBig), s1.PeerCaps().MaxMessageSize)
	_, err := s1.SendMessage(tooBig)
	if !errors.Is(err, ErrExceedsPeerLimit) {
		t.Fatalf("oversize send: got %v, want ErrExceedsPeerLimit", err)
	}
	if errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("error must be distinguishable from ErrMessageTooLarge: %v", err)
	}
	t.Logf("oversize send rejected locally: %v", err)

	select {
	case m := <-s2.Delivered():
		t.Fatalf("oversize message must not be delivered, got msgID=%d", m.MsgID)
	case <-time.After(100 * time.Millisecond):
		t.Log("verdict: nothing delivered on peer (send failed before any write)")
	}

	ok := make([]byte, 50)
	id, err := s1.SendMessage(ok)
	if err != nil {
		t.Fatalf("in-limit send: %v", err)
	}
	got := recvN(t, s2, 1, 2*time.Second)
	if got[0].MsgID != id || len(got[0].Data) != 50 {
		t.Fatalf("in-limit message mismatch: %+v", got[0])
	}
	t.Logf("verdict: %d-byte message within limit delivered as msgID=%d", 50, id)
}

// TestSessionDefaultCapsWithoutNegotiation: a peer that never sends a
// hello (old firmware) gets DefaultPeerCaps; oversized messages still
// fail locally instead of being silently dropped by the receiver.
func TestSessionDefaultCapsWithoutNegotiation(t *testing.T) {
	c1, c2 := net.Pipe()
	drainPeer(t, c2) // raw peer: reads but never sends a hello
	s := NewSession(SessionConfig{})
	t.Cleanup(func() { s.Close() })
	if err := s.Attach(c1); err != nil {
		t.Fatalf("attach: %v", err)
	}

	pc := s.PeerCaps()
	def := DefaultPeerCaps()
	t.Logf("input: peer never negotiates; PeerCaps()=%+v, defaults=%+v", pc, def)
	if pc.Negotiated || pc.MaxMessageSize != def.MaxMessageSize || pc.MaxInFlight != def.MaxInFlight {
		t.Fatalf("PeerCaps()=%+v, want defaults %+v with Negotiated=false", pc, def)
	}

	tooBig := make([]byte, def.MaxMessageSize+1)
	_, err := s.SendMessage(tooBig)
	if !errors.Is(err, ErrExceedsPeerLimit) {
		t.Fatalf("send over default limit: got %v, want ErrExceedsPeerLimit", err)
	}
	t.Logf("verdict: %d-byte message against non-negotiating peer rejected locally: %v",
		len(tooBig), err)

	if _, err := s.SendMessage([]byte("small")); err != nil {
		t.Fatalf("small message to non-negotiating peer must still send: %v", err)
	}
	t.Log("verdict: connection works without negotiation; small message sent fine")
}

// TestSessionRenegotiateOnReconnect: limits are re-exchanged on every
// Attach; the new peer's limits take effect without restarting.
func TestSessionRenegotiateOnReconnect(t *testing.T) {
	s := capsSession(t, 64*1024, 8)

	// Connection 1: peer with limit 4096.
	c1a, c1b := net.Pipe()
	p1 := capsSession(t, 4096, 4)
	if err := s.Attach(c1a); err != nil {
		t.Fatalf("attach 1: %v", err)
	}
	if err := p1.Attach(c1b); err != nil {
		t.Fatalf("attach p1: %v", err)
	}
	waitFor(t, "limit 4096 from first peer", 2*time.Second, func() bool {
		return s.PeerCaps().Negotiated && s.PeerCaps().MaxMessageSize == 4096
	})
	t.Logf("connection 1: negotiated peer limit %d", s.PeerCaps().MaxMessageSize)

	// Reconnect to a different peer with limit 512.
	c2a, c2b := net.Pipe()
	p2 := capsSession(t, 512, 2)
	if err := s.Attach(c2a); err != nil {
		t.Fatalf("attach 2: %v", err)
	}
	if err := p2.Attach(c2b); err != nil {
		t.Fatalf("attach p2: %v", err)
	}
	waitFor(t, "limit 512 from second peer", 2*time.Second, func() bool {
		return s.PeerCaps().Negotiated && s.PeerCaps().MaxMessageSize == 512
	})
	t.Logf("connection 2 (reconnect): renegotiated peer limit %d", s.PeerCaps().MaxMessageSize)

	// The new limit is enforced immediately.
	big := make([]byte, 600)
	if _, err := s.SendMessage(big); !errors.Is(err, ErrExceedsPeerLimit) {
		t.Fatalf("600 bytes against new limit 512: got %v, want ErrExceedsPeerLimit", err)
	}
	t.Log("verdict: new limit enforced immediately after reconnect, no restart")
}
