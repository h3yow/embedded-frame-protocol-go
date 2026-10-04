package efp

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"
)

// drainErrors keeps the protocol moving by draining the non-message
// channels. Delivered messages are left untouched so tests can assert on
// them directly; the delivery channel is buffered (64) and the tests keep
// its depth tiny.
func drainErrors(t *testing.T, s *Session) {
	t.Helper()
	go func() {
		for {
			select {
			case _, ok := <-s.Errors():
				if !ok {
					return
				}
			case _, ok := <-s.Events():
				if !ok {
					return
				}
			}
		}
	}()
}

// drainSession keeps every background channel flowing (use only in tests
// that do not themselves assert on Delivered).
func drainSession(t *testing.T, s *Session) {
	t.Helper()
	go func() {
		for {
			select {
			case _, ok := <-s.Delivered():
				if !ok {
					return
				}
			case _, ok := <-s.Errors():
				if !ok {
					return
				}
			case _, ok := <-s.Events():
				if !ok {
					return
				}
			}
		}
	}()
}

// waitForPeerCaps polls until the session reports peer-advertised caps.
func waitForPeerCaps(t *testing.T, s *Session, wantMax uint32) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if caps, fromPeer := s.PeerCapabilities(); fromPeer && caps.MaxMessageSize == wantMax {
			t.Logf("negotiated peer caps: MaxMessageSize=%d MaxInFlight=%d", caps.MaxMessageSize, caps.MaxInFlight)
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	caps, fromPeer := s.PeerCapabilities()
	t.Fatalf("peer caps not negotiated: caps=%+v fromPeer=%v, want MaxMessageSize=%d", caps, fromPeer, wantMax)
}

func TestCapFrameEncodeDecode(t *testing.T) {
	in := Capabilities{MaxMessageSize: 4096, MaxInFlight: 4}
	f, err := EncodeCapFrame(in)
	if err != nil {
		t.Fatalf("EncodeCapFrame: %v", err)
	}
	b, err := f.Encode()
	if err != nil {
		t.Fatalf("cap frame encode: %v", err)
	}
	t.Logf("cap frame: version=%d flags=0x%x payloadLen=%d wireLen=%d", f.Version, f.Flags, len(f.Payload), len(b))
	out, err := DecodeFrame(b)
	if err != nil {
		t.Fatalf("DecodeFrame cap: %v", err)
	}
	got, err := DecodeCapFrame(out)
	if err != nil {
		t.Fatalf("DecodeCapFrame: %v", err)
	}
	if got != in {
		t.Fatalf("round trip mismatch: got=%+v want=%+v", got, in)
	}
}

func TestCapabilityNegotiationAndOverLimitSend(t *testing.T) {
	// Receiver advertises a small ceiling: 2048 bytes, 2 in flight.
	peerCfg := SessionConfig{
		LocalCapabilities: &Capabilities{MaxMessageSize: 2048, MaxInFlight: 2},
	}
	c1, c2 := net.Pipe()
	sender := NewSession(SessionConfig{})
	receiver := NewSession(peerCfg)
	t.Cleanup(func() { sender.Close(); receiver.Close() })
	drainSession(t, receiver)
	drainSession(t, sender)
	if err := receiver.Attach(c2); err != nil {
		t.Fatalf("receiver attach: %v", err)
	}
	if err := sender.Attach(c1); err != nil {
		t.Fatalf("sender attach: %v", err)
	}

	// Feed: peer advertised MaxMessageSize=2048.
	waitForPeerCaps(t, sender, 2048)

	// Expect: a 2048-byte message is accepted ...
	if id, err := sender.SendMessage(bytes.Repeat([]byte{'A'}, 2048)); err != nil {
		t.Fatalf("2048-byte send should fit peer limit, got err=%v", err)
	} else {
		t.Logf("2048-byte message accepted (msgID=%d)", id)
	}

	// ... and a 2049-byte message is refused LOCALLY with a distinguishable
	// error; nothing is written for it.
	_, err := sender.SendMessage(bytes.Repeat([]byte{'B'}, 2049))
	if err == nil {
		t.Fatal("expected over-limit send to fail, got nil error (fake success)")
	}
	if !errors.Is(err, ErrPeerLimitExceeded) {
		t.Fatalf("error %v does not wrap ErrPeerLimitExceeded", err)
	}
	if errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("over-limit error must differ from local fragmentation limit, got %v", err)
	}
	t.Logf("2049-byte send rejected before wire: %v", err)
}

func TestCapabilityLegacyPeerFallback(t *testing.T) {
	// A "legacy peer" is a raw pipe endpoint that never answers the
	// capability frame. The session must still connect using documented
	// conservative defaults and must NOT fake-send an oversized message.
	c1, _ := net.Pipe()
	s := NewSession(SessionConfig{})
	t.Cleanup(func() { s.Close() })
	drainSession(t, s)
	start := time.Now()
	if err := s.Attach(c1); err != nil {
		t.Fatalf("attach: %v", err)
	}
	t.Logf("Attach returned immediately at %v (does not block on legacy peer)", time.Since(start))

	// Initially the fallback is in force even before the timeout fires.
	caps, fromPeer := s.PeerCapabilities()
	if caps.MaxMessageSize != DefaultPeerMaxMessageSize || caps.MaxInFlight != DefaultPeerMaxInFlight {
		t.Fatalf("initial caps=%+v, want conservative defaults", caps)
	}
	t.Logf("legacy peer: initial fallback caps=%+v fromPeer=%v", caps, fromPeer)

	// A message larger than the default (1024) is refused with the same
	// distinguishable signal; the default never pretends to accept it.
	_, err := s.SendMessage(make([]byte, MaxPayload+1))
	if !errors.Is(err, ErrPeerLimitExceeded) {
		t.Fatalf("expected ErrPeerLimitExceeded against legacy default, got %v", err)
	}
	t.Logf("legacy peer: %d-byte message rejected: %v", MaxPayload+1, err)

	// After the negotiation timeout the fallback is recorded as such.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if st := s.SnapshotStats(); st.NegotiationFalls == 1 {
			t.Logf("legacy peer: NegotiationFalls=%d after %v", st.NegotiationFalls, time.Since(start))
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected NegotiationFalls=1, stats=%+v", s.SnapshotStats())
}

// TestCapabilityReconnectRenegotiates verifies that reconnecting applies
// the new peer's limits immediately, without restarting the process.
func TestCapabilityReconnectRenegotiates(t *testing.T) {
	c1a, c1b := net.Pipe()
	s := NewSession(SessionConfig{})
	peer1 := NewSession(SessionConfig{LocalCapabilities: &Capabilities{MaxMessageSize: 1500, MaxInFlight: 2}})
	t.Cleanup(func() { s.Close(); peer1.Close() })
	drainSession(t, s)
	drainSession(t, peer1)
	if err := peer1.Attach(c1b); err != nil {
		t.Fatalf("peer1 attach: %v", err)
	}
	if err := s.Attach(c1a); err != nil {
		t.Fatalf("attach 1: %v", err)
	}
	waitForPeerCaps(t, s, 1500)
	if _, err := s.SendMessage(make([]byte, 1500)); err != nil {
		t.Fatalf("1500-byte send under old limit: %v", err)
	}

	// Reconnect to a peer advertising a much larger ceiling.
	c2a, c2b := net.Pipe()
	peer2 := NewSession(SessionConfig{LocalCapabilities: &Capabilities{MaxMessageSize: 8192, MaxInFlight: 8}})
	t.Cleanup(func() { peer2.Close() })
	drainSession(t, peer2)
	if err := peer2.Attach(c2b); err != nil {
		t.Fatalf("peer2 attach: %v", err)
	}
	if err := s.Attach(c2a); err != nil {
		t.Fatalf("attach 2: %v", err)
	}
	waitForPeerCaps(t, s, 8192)

	// The old limit must no longer reject a message that fits the new one.
	if _, err := s.SendMessage(make([]byte, 4096)); err != nil {
		t.Fatalf("4096-byte send after reconnect: %v (new limits not applied)", err)
	}
	st := s.SnapshotStats()
	if st.Reconnects != 1 {
		t.Fatalf("Reconnects=%d, want 1", st.Reconnects)
	}
	t.Logf("reconnect renegotiation: 1500 -> 8192 effective immediately, Reconnects=1, Negotiations=%d", st.Negotiations)
}
