package efp

import (
	"bytes"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

// legacyPeer simulates pre-negotiation firmware on the other end of a
// transport: it decodes frames and pushes ALL of them — control flag
// included — into a reassembler, delivering complete messages to its
// "application". It never sends a hello itself.
type legacyPeer struct {
	dec *Decoder
	re  *Reassembler

	mu        sync.Mutex
	delivered []ReassembledMessage
}

func newLegacyPeer(cfg ReassemblyConfig) *legacyPeer {
	return &legacyPeer{dec: NewDecoder(), re: NewReassembler(cfg)}
}

// run consumes the transport until it closes; call in a goroutine.
func (p *legacyPeer) run(c net.Conn) {
	buf := make([]byte, 4096)
	for {
		n, err := c.Read(buf)
		if n > 0 {
			frames, _ := p.dec.Feed(buf[:n])
			p.mu.Lock()
			for _, f := range frames {
				// Legacy firmware has no control-frame handling:
				// every decoded frame goes into the reassembler.
				if msg, perr := p.re.Push(f); perr == nil && msg != nil {
					p.delivered = append(p.delivered, *msg)
				}
			}
			p.mu.Unlock()
		}
		if err != nil {
			return
		}
	}
}

func (p *legacyPeer) inFlight() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.re.InFlight()
}

// expire runs one reassembly sweep (the legacy firmware's janitor).
func (p *legacyPeer) expire() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.re.Expire()
}

func (p *legacyPeer) appMessages() []ReassembledMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]ReassembledMessage, len(p.delivered))
	copy(out, p.delivered)
	return out
}

// TestEncodeHelloShape: the hello control frame is shaped so that a
// legacy peer can never reassemble it into a deliverable message, while
// a negotiation-aware peer decodes the caps normally.
func TestEncodeHelloShape(t *testing.T) {
	f, err := encodeHello(7, PeerCaps{MaxMessageSize: 4096, MaxInFlight: 3})
	if err != nil {
		t.Fatalf("encodeHello: %v", err)
	}
	t.Logf("hello frame: flags=%#04x msgID=%#x frag=%d/%d payload=%d bytes",
		f.Flags, f.MsgID, f.FragIdx, f.FragCnt, len(f.Payload))
	if f.Flags&FlagControl == 0 {
		t.Fatal("hello must carry FlagControl")
	}
	if f.Flags&FlagFragment == 0 || f.FragCnt != 2 || f.FragIdx != 0 {
		t.Fatalf("hello must be an uncompletable fragment (0/2), got %d/%d flags=%#04x",
			f.FragIdx, f.FragCnt, f.Flags)
	}
	if f.MsgID != capsReservedMsgID {
		t.Fatalf("hello MsgID=%#x, want reserved %#x", f.MsgID, capsReservedMsgID)
	}
	caps, err := decodeHello(f.Payload)
	if err != nil {
		t.Fatalf("decodeHello: %v", err)
	}
	if !caps.Negotiated || caps.MaxMessageSize != 4096 || caps.MaxInFlight != 3 {
		t.Fatalf("decoded caps=%+v, want {4096 3 negotiated}", caps)
	}
	t.Log("verdict: hello is control|fragment 0/2 under reserved MsgID; caps decode intact")
}

// TestSessionHelloInvisibleToLegacyPeer: against old firmware that does
// not implement capability negotiation, the session's hello must not
// surface as a spurious application message on the peer; the connection
// still works both ways with default receive limits, and oversized sends
// fail locally with a distinguishable error.
func TestSessionHelloInvisibleToLegacyPeer(t *testing.T) {
	c1, c2 := net.Pipe()
	legacyCfg := DefaultReassemblyConfig()
	legacyCfg.FragmentTimeout = 150 * time.Millisecond // fast janitor for the test
	peer := newLegacyPeer(legacyCfg)
	go peer.run(c2)
	t.Cleanup(func() { c2.Close() })

	s := NewSession(SessionConfig{})
	t.Cleanup(func() { s.Close() })
	if err := s.Attach(c1); err != nil {
		t.Fatalf("attach: %v", err)
	}
	t.Log("input: session attached to legacy peer (no negotiation support); hello sent automatically")

	// The hello reaches the legacy peer and is buffered as an incomplete
	// message (fragment 0/2 under the reserved MsgID) — never delivered.
	waitFor(t, "legacy buffers hello as incomplete", 2*time.Second, func() bool {
		return peer.inFlight() == 1
	})
	t.Log("legacy peer: hello occupies one reassembly slot as an uncompletable fragment")

	// The session never learns peer caps: defaults stay in effect.
	pc, def := s.PeerCaps(), DefaultPeerCaps()
	t.Logf("session view: PeerCaps()=%+v (defaults %+v)", pc, def)
	if pc.Negotiated || pc.MaxMessageSize != def.MaxMessageSize || pc.MaxInFlight != def.MaxInFlight {
		t.Fatalf("PeerCaps()=%+v, want defaults %+v with Negotiated=false", pc, def)
	}

	// Application traffic works in both directions.
	id, err := s.SendMessage([]byte("to-legacy"))
	if err != nil {
		t.Fatalf("send to legacy peer: %v", err)
	}
	waitFor(t, "legacy app receives real message", 2*time.Second, func() bool {
		return len(peer.appMessages()) == 1
	})
	appMsgs := peer.appMessages()
	if appMsgs[0].MsgID != id || string(appMsgs[0].Data) != "to-legacy" {
		t.Fatalf("legacy app got %+v, want msgID=%d payload=to-legacy", appMsgs[0], id)
	}
	t.Logf("legacy app: exactly one message so far, the real one (msgID=%d)", id)

	frames, _, err := Fragment(42, []byte("from-legacy"), 0)
	if err != nil {
		t.Fatalf("Fragment: %v", err)
	}
	if _, err := c2.Write(mustEncode(t, frames[0])); err != nil {
		t.Fatalf("legacy write: %v", err)
	}
	got := recvN(t, s, 1, 2*time.Second)
	if got[0].MsgID != 42 || string(got[0].Data) != "from-legacy" {
		t.Fatalf("session got %+v, want msgID=42 payload=from-legacy", got[0])
	}
	t.Log("session: message from legacy peer delivered; connection established both ways")

	// Oversized send against the default limit fails locally, distinctly.
	tooBig := make([]byte, def.MaxMessageSize+1)
	if _, err := s.SendMessage(tooBig); !errors.Is(err, ErrExceedsPeerLimit) {
		t.Fatalf("oversize send: got %v, want ErrExceedsPeerLimit", err)
	} else {
		t.Logf("oversize send (%d bytes > default %d) rejected locally: %v",
			len(tooBig), def.MaxMessageSize, err)
	}

	// After the legacy peer's fragment timeout, the hello fragment is
	// discarded; the peer's application still has exactly the one real
	// message — the hello never leaked through.
	waitFor(t, "hello fragment expired on legacy peer", 2*time.Second, func() bool {
		peer.expire()
		return peer.inFlight() == 0
	})
	if msgs := peer.appMessages(); len(msgs) != 1 || !bytes.Equal(msgs[0].Data, []byte("to-legacy")) {
		t.Fatalf("legacy app messages = %+v, want exactly [to-legacy]", msgs)
	}
	t.Log("verdict: hello expired undelivered on legacy peer; exactly one real app message, " +
		"defaults in effect, oversize send rejected distinctly")
}
