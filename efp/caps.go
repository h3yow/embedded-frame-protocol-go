package efp

import (
	"encoding/binary"
	"fmt"
)

// Capability negotiation
//
// On every Attach (initial connect and every reconnect) a session sends a
// hello control frame advertising its receive capabilities, and expects
// the peer to do the same. Control frames are regular frames with
// FlagControl set and are handled by the session layer directly; they
// never reach the reassembler or the application.
//
// Hello payload layout (all integers big-endian):
//
//	KIND(1)=1  MAX_MSG_SIZE(4)  MAX_IN_FLIGHT(2)
//
// Compatibility with peers that do NOT implement negotiation (older
// firmware pushes every decoded frame, control flag included, into its
// reassembler): the hello is deliberately encoded as an UNCOMPLETABLE
// fragment — FlagFragment set, FragCnt=2, only fragment 0 ever sent,
// under the reserved MsgID capsReservedMsgID that application messages
// never use. A legacy peer buffers it as an incomplete message that can
// never finish, so it is never delivered to the peer's application; it
// occupies one reassembly slot until the peer's own fragment timeout
// discards it. A negotiation-aware peer handles the frame by its
// FlagControl bit before any reassembly and is unaffected.
//
// A peer that never sends a hello is handled by falling back to
// DefaultPeerCaps(); the connection works, and locally enforced send
// limits still apply, so oversized messages fail at send time instead of
// being silently dropped by the receiver.
const (
	capsKindHello   = 1
	capsHelloLen    = 7
	capsMaxInFlight = 1 << 16 // advertised in-flight limit is a uint16

	// capsReservedMsgID is reserved for control traffic; application
	// messages must never use it. Keeping the hello out of the
	// application MsgID space prevents its uncompletable fragment from
	// ever colliding with a real message in a legacy peer's reassembler.
	capsReservedMsgID = 0xFFFFFFFF
)

// PeerCaps describes what the peer is willing to receive.
type PeerCaps struct {
	MaxMessageSize int  // max bytes of one reassembled message the peer accepts
	MaxInFlight    int  // max messages the peer reassembles concurrently
	Negotiated     bool // false until a hello frame arrives (defaults in use)
}

// DefaultPeerCaps returns the conservative limits assumed before a hello
// arrives, and permanently for peers that do not implement negotiation.
// They mirror DefaultReassemblyConfig so two unconfigured stacks agree.
func DefaultPeerCaps() PeerCaps {
	cfg := DefaultReassemblyConfig()
	return PeerCaps{MaxMessageSize: cfg.MaxMessageSize, MaxInFlight: cfg.MaxInFlight}
}

// encodeHello builds the control frame advertising the given local caps.
// The frame is shaped so that a legacy peer (no control-frame handling)
// can never reassemble it into a deliverable application message: it
// carries FlagFragment with FragCnt=2 while only fragment 0 is ever
// sent, under the reserved MsgID. Negotiation-aware peers dispatch on
// FlagControl and ignore the fragment fields.
func encodeHello(seq uint32, caps PeerCaps) (*Frame, error) {
	if caps.MaxInFlight > capsMaxInFlight {
		caps.MaxInFlight = capsMaxInFlight
	}
	p := make([]byte, capsHelloLen)
	p[0] = capsKindHello
	binary.BigEndian.PutUint32(p[1:], uint32(caps.MaxMessageSize))
	binary.BigEndian.PutUint16(p[5:], uint16(caps.MaxInFlight))
	return &Frame{
		Version: Version,
		Flags:   FlagControl | FlagFragment,
		Seq:     seq,
		MsgID:   capsReservedMsgID,
		FragIdx: 0,
		FragCnt: 2, // fragment 1 is never sent: uncompletable by design
		Payload: p,
	}, nil
}

// decodeHello parses a control frame payload into PeerCaps.
func decodeHello(payload []byte) (PeerCaps, error) {
	var caps PeerCaps
	if len(payload) != capsHelloLen || payload[0] != capsKindHello {
		return caps, &FrameError{Kind: ErrKindBadLength,
			Detail: fmt.Sprintf("malformed hello: %d bytes", len(payload))}
	}
	caps.MaxMessageSize = int(binary.BigEndian.Uint32(payload[1:]))
	caps.MaxInFlight = int(binary.BigEndian.Uint16(payload[5:]))
	if caps.MaxMessageSize <= 0 || caps.MaxInFlight <= 0 {
		return caps, &FrameError{Kind: ErrKindBadLength,
			Detail: fmt.Sprintf("hello with non-positive limits: %+v", caps)}
	}
	caps.Negotiated = true
	return caps, nil
}
