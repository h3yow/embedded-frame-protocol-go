package efp

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// legacyPeer emulates firmware from before the caps feature: it accepts
// only Version frames (anything else is skipped during resync) and hands
// every decoded payload to its "application". It lets tests observe
// exactly what an old peer's application would see on the wire.
type legacyPeer struct {
	msgs        chan []byte // "application" deliveries
	versionErrs atomic.Int32
	crcErrs     atomic.Int32
}

func newLegacyPeer() *legacyPeer { return &legacyPeer{msgs: make(chan []byte, 16)} }

// run reads c until it fails, decoding with strict pre-caps rules.
func (l *legacyPeer) run(c net.Conn) {
	go func() {
		var buf []byte
		tmp := make([]byte, 4096)
		for {
			n, err := c.Read(tmp)
			if n > 0 {
				buf = l.scan(append(buf, tmp[:n]...))
			}
			if err != nil {
				return
			}
		}
	}()
}

// scan extracts frames with the old firmware's rules: magic, Version
// only, LEN, CRC. Anything else is skipped byte-wise (resync).
func (l *legacyPeer) scan(buf []byte) []byte {
	for {
		if len(buf) < HeaderLen {
			return buf
		}
		if buf[0] != Magic0 || buf[1] != Magic1 {
			idx := indexMagic(buf)
			if idx < 0 {
				return buf[:0]
			}
			buf = buf[idx:]
			continue
		}
		if buf[2] != Version { // old firmware knows no other version
			l.versionErrs.Add(1)
			buf = buf[1:]
			continue
		}
		declared := int(binary.BigEndian.Uint16(buf[16:]))
		if declared > MaxPayload {
			buf = buf[1:]
			continue
		}
		total := HeaderLen + declared + CRCLen
		if len(buf) < total {
			return buf
		}
		want := binary.BigEndian.Uint32(buf[total-CRCLen:])
		if crc32.ChecksumIEEE(buf[:total-CRCLen]) != want {
			l.crcErrs.Add(1)
			buf = buf[1:]
			continue
		}
		l.msgs <- append([]byte(nil), buf[HeaderLen:total-CRCLen]...)
		buf = buf[total:]
	}
}

// TestSessionLegacyPeerNoPhantomMessage: against firmware that does not
// implement capability negotiation, connecting must not produce a
// phantom short message in the peer's application (our hello rides at
// capsVersion and is rejected at decode time), the connection still
// works, and our send limit falls back to DefaultPeerCaps with
// oversize sends failing as ErrExceedsPeerLimit.
func TestSessionLegacyPeerNoPhantomMessage(t *testing.T) {
	c1, c2 := net.Pipe()
	t.Cleanup(func() { c2.Close() })
	legacy := newLegacyPeer()
	legacy.run(c2)

	s := NewSession(SessionConfig{})
	t.Cleanup(func() { s.Close() })
	if err := s.Attach(c1); err != nil {
		t.Fatalf("attach: %v", err)
	}
	t.Log("input: attach to pre-caps firmware; our unsolicited hello goes out at capsVersion")

	// Positive proof the hello arrived and was rejected by the old
	// decoder (unsupported version), before the negative assertion.
	waitFor(t, "legacy peer rejected the hello at decode time", 2*time.Second, func() bool {
		return legacy.versionErrs.Load() >= 1
	})
	select {
	case m := <-legacy.msgs:
		t.Fatalf("legacy application got a phantom message: %q", m)
	default:
		t.Logf("legacy decoder rejected the hello (versionErrs=%d); application saw nothing",
			legacy.versionErrs.Load())
	}

	// No hello from the peer: defaults stay in effect.
	pc := s.PeerCaps()
	if pc.Negotiated || pc != DefaultPeerCaps() {
		t.Fatalf("PeerCaps()=%+v, want defaults with Negotiated=false", pc)
	}
	t.Logf("no hello from peer: PeerCaps stays at defaults %+v", pc)

	// Data still flows: a normal message reaches the legacy application.
	if _, err := s.SendMessage([]byte("real payload")); err != nil {
		t.Fatalf("send to legacy peer: %v", err)
	}
	select {
	case m := <-legacy.msgs:
		if string(m) != "real payload" {
			t.Fatalf("legacy application got %q, want %q", m, "real payload")
		}
		t.Logf("verdict: legacy application received exactly the real message %q, nothing else", m)
	case <-time.After(2 * time.Second):
		t.Fatal("legacy application did not receive the data message")
	}

	// Oversize against the default limit fails locally, distinguishably.
	tooBig := make([]byte, DefaultPeerCaps().MaxMessageSize+1)
	if _, err := s.SendMessage(tooBig); !errors.Is(err, ErrExceedsPeerLimit) {
		t.Fatalf("oversize send: got %v, want ErrExceedsPeerLimit", err)
	}
	t.Log("verdict: oversize send against default limit failed locally with ErrExceedsPeerLimit")
}

// TestSessionAnswersLegacyV1Hello: a peer from the first caps release
// sends its hello at Version and only accepts Version frames. We answer
// with exactly one Version hello, so negotiation with that release still
// completes, while our unsolicited hello stays at capsVersion.
func TestSessionAnswersLegacyV1Hello(t *testing.T) {
	c1, c2 := net.Pipe()
	t.Cleanup(func() { c2.Close() })

	s := NewSession(SessionConfig{})
	t.Cleanup(func() { s.Close() })
	if err := s.Attach(c1); err != nil {
		t.Fatalf("attach: %v", err)
	}

	// Peer side: decode every frame we send (both versions).
	frames := make(chan *Frame, 16)
	go func() {
		d := NewDecoder()
		tmp := make([]byte, 4096)
		for {
			n, err := c2.Read(tmp)
			if n > 0 {
				fs, _ := d.Feed(tmp[:n])
				for _, f := range fs {
					frames <- f
				}
			}
			if err != nil {
				return
			}
		}
	}()

	// Our unsolicited hello must be at capsVersion.
	select {
	case f := <-frames:
		if f.Flags&FlagControl == 0 || f.Version != capsVersion {
			t.Fatalf("first frame = version %d flags %#x, want capsVersion control hello", f.Version, f.Flags)
		}
		t.Logf("unsolicited hello rides at version %d (invisible to pre-caps firmware)", f.Version)
	case <-time.After(2 * time.Second):
		t.Fatal("no unsolicited hello sent")
	}

	// The peer (first caps release) sends its hello at Version.
	legacyHello := mustEncode(t, &Frame{
		Version: Version, Flags: FlagControl, Seq: 0, FragCnt: 1,
		Payload: helloPayload(t, 1234, 3),
	})
	if _, err := c2.Write(legacyHello); err != nil {
		t.Fatalf("write legacy hello: %v", err)
	}
	t.Log("input: peer (first caps release) sends a Version hello {maxMsg=1234, inFlight=3}")

	// We answer with exactly one Version hello carrying our own caps.
	select {
	case f := <-frames:
		if f.Flags&FlagControl == 0 || f.Version != Version {
			t.Fatalf("reply = version %d flags %#x, want Version control hello", f.Version, f.Flags)
		}
		caps, err := decodeHello(f.Payload)
		if err != nil {
			t.Fatalf("reply hello undecodable: %v", err)
		}
		def := DefaultReassemblyConfig()
		if caps.MaxMessageSize != def.MaxMessageSize || caps.MaxInFlight != def.MaxInFlight {
			t.Fatalf("reply hello caps = %+v, want our local limits %+v", caps, def)
		}
		t.Logf("answered with one Version hello carrying our caps %+v", caps)
	case <-time.After(2 * time.Second):
		t.Fatal("no Version hello reply to the legacy peer")
	}

	// And the peer's limits take effect on us.
	waitFor(t, "legacy peer caps applied", 2*time.Second, func() bool {
		pc := s.PeerCaps()
		return pc.Negotiated && pc.MaxMessageSize == 1234 && pc.MaxInFlight == 3
	})
	t.Logf("verdict: negotiation with first-release peer completed, PeerCaps=%+v", s.PeerCaps())

	// A second Version hello must not trigger another reply.
	if _, err := c2.Write(legacyHello); err != nil {
		t.Fatalf("rewrite legacy hello: %v", err)
	}
	select {
	case f := <-frames:
		t.Fatalf("unexpected second reply: version %d flags %#x", f.Version, f.Flags)
	case <-time.After(200 * time.Millisecond):
		t.Log("no duplicate reply to repeated legacy hellos (one per connection)")
	}
}

// helloPayload builds a raw hello payload for tests.
func helloPayload(t *testing.T, maxMsg, maxInFlight int) []byte {
	t.Helper()
	f, err := encodeHello(0, Version, PeerCaps{MaxMessageSize: maxMsg, MaxInFlight: maxInFlight})
	if err != nil {
		t.Fatalf("encodeHello: %v", err)
	}
	return f.Payload
}
