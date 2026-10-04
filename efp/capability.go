package efp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Capabilities describes the receive limits one endpoint advertises to its
// peer during the per-connection capability handshake. Every value is a
// RECEIVE-side limit: the sender is expected to respect it.
type Capabilities struct {
	// MaxMessageSize is the largest reassembled application message, in
	// bytes, the advertiser is willing to accept. The sender refuses to
	// send a larger message instead of having it dropped on arrival.
	MaxMessageSize uint32
	// MaxInFlight is the maximum number of incomplete fragmented messages
	// the advertiser reassembles at the same time. Excess new messages are
	// rejected by the receiver; a well-behaved sender uses this value to
	// bound how many fragmented messages it keeps outstanding.
	MaxInFlight uint32
}

// Conservative limits assumed for a peer that never completes the
// capability handshake (older firmware without CapVersion support, or a
// peer that lost the handshake during a network hiccup). They stay within
// one MaxPayload-sized message so no conforming small message is affected,
// and they never imply that an oversized message was accepted.
const (
	DefaultPeerMaxMessageSize uint32 = MaxPayload // single, unfragmented frame
	DefaultPeerMaxInFlight    uint32 = 1
)

// DefaultCapabilities returns the conservative fallback used when the peer
// does not implement the capability handshake.
func DefaultCapabilities() Capabilities {
	return Capabilities{
		MaxMessageSize: DefaultPeerMaxMessageSize,
		MaxInFlight:    DefaultPeerMaxInFlight,
	}
}

// capPayloadLen is the fixed size of a capability frame payload:
// MaxMessageSize uint32 + MaxInFlight uint32.
const capPayloadLen = 8

// DefaultNegotiationTimeout bounds how long Attach waits for the peer's
// capability advertisement before falling back to the conservative
// defaults. It is short by design: field links with jitter must become
// usable even when the peer is old firmware.
const DefaultNegotiationTimeout = 750 * time.Millisecond

// ErrPeerLimitExceeded is returned by SendMessage when the message exceeds
// the receive limit the peer advertised (or the legacy-peer default). It
// is distinct from ErrMessageTooLarge (exceeds what the local sender can
// fragment at all) so the application can react differently: the message
// was never put on the wire, and retrying to another/fatter peer may help.
var ErrPeerLimitExceeded = errors.New("efp: message exceeds peer receive limit")

// EncodeCapFrame serializes a capability advertisement as one version-2
// control frame.
func EncodeCapFrame(c Capabilities) (*Frame, error) {
	p := make([]byte, capPayloadLen)
	binary.BigEndian.PutUint32(p[0:], c.MaxMessageSize)
	binary.BigEndian.PutUint32(p[4:], c.MaxInFlight)
	return &Frame{
		Version: CapVersion,
		Flags:   FlagControl,
		MsgID:   0,
		FragCnt: 1,
		Payload: p,
	}, nil
}

// DecodeCapFrame parses a capability advertisement from a control frame.
func DecodeCapFrame(f *Frame) (Capabilities, error) {
	var zero Capabilities
	if f == nil || f.Version != CapVersion || f.Flags&FlagControl == 0 {
		return zero, &FrameError{Kind: ErrKindBadLength, Detail: "not a capability control frame"}
	}
	if len(f.Payload) != capPayloadLen {
		return zero, &FrameError{Kind: ErrKindBadLength,
			Detail: fmt.Sprintf("cap payload len=%d, want %d", len(f.Payload), capPayloadLen)}
	}
	c := Capabilities{
		MaxMessageSize: binary.BigEndian.Uint32(f.Payload[0:]),
		MaxInFlight:    binary.BigEndian.Uint32(f.Payload[4:]),
	}
	if c.MaxMessageSize == 0 || c.MaxInFlight == 0 {
		return zero, &FrameError{Kind: ErrKindBadLength,
			Detail: fmt.Sprintf("cap advertises zero limit: %+v", c)}
	}
	return c, nil
}

// capExchange tracks one connection's capability handshake. Exactly one
// settle call wins; the winning limits and their origin are returned by
// wait.
type capExchange struct {
	mu       sync.Mutex
	done     chan struct{}
	settled  bool
	peer     Capabilities
	fromPeer bool
}

// newCapExchange returns an exchange initialized with fallback limits.
func newCapExchange(fallback Capabilities) *capExchange {
	return &capExchange{done: make(chan struct{}), peer: fallback}
}

// settlePeer records the peer-advertised limits and completes the exchange.
// Only the first settlement wins; duplicates (e.g. a peer that advertises
// twice) are ignored.
func (x *capExchange) settlePeer(c Capabilities) bool {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.settled {
		return false
	}
	x.settled = true
	x.peer = c
	x.fromPeer = true
	close(x.done)
	return true
}

// settleFallback completes the exchange on the fallback limits after a
// timeout. It loses the race if a peer advertisement arrived first.
func (x *capExchange) settleFallback() bool {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.settled {
		return false
	}
	x.settled = true
	x.fromPeer = false
	close(x.done)
	return true
}

// wait blocks until the handshake completes or timeout elapses, then
// returns the limits in force and whether they were advertised by the peer
// (true) or taken from the conservative fallback (false).
func (x *capExchange) wait(timeout time.Duration) (c Capabilities, fromPeer bool) {
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-x.done:
	case <-t.C:
		x.settleFallback()
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.peer, x.fromPeer
}
