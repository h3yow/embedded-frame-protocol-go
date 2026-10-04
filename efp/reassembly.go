package efp

import (
	"fmt"
	"time"
)

// ReassemblyConfig bounds resource usage of incomplete messages.
type ReassemblyConfig struct {
	MaxMessageSize  int           // max total bytes of a reassembled message
	MaxInFlight     int           // max concurrent incomplete messages
	FragmentTimeout time.Duration // max age of an incomplete message
}

// DefaultReassemblyConfig returns sane defaults for embedded use.
func DefaultReassemblyConfig() ReassemblyConfig {
	return ReassemblyConfig{
		MaxMessageSize:  64 * 1024,
		MaxInFlight:     8,
		FragmentTimeout: 5 * time.Second,
	}
}

// ReassembledMessage is a fully reassembled application message.
type ReassembledMessage struct {
	MsgID uint32
	Data  []byte
}

// ReassemblyError describes a deterministic reassembly failure
// (timeout, overflow, inconsistent fragments).
type ReassemblyError struct {
	MsgID  uint32
	Reason error
}

func (e *ReassemblyError) Error() string {
	return fmt.Sprintf("efp: reassembly of message %d failed: %v", e.MsgID, e.Reason)
}

// Unwrap exposes the underlying reason for errors.Is checks.
func (e *ReassemblyError) Unwrap() error { return e.Reason }

// assembly tracks one incomplete message.
type assembly struct {
	fragCnt   uint16
	frags     [][]byte // nil entry = not yet received
	received  int
	totalSize int
	created   time.Time
}

// reassemblyEventSink receives observability events emitted while pushing
// fragments. The Session implements it; standalone test users can leave it
// unset.
type reassemblyEventSink interface {
	onDuplicateFragment(msgID uint32, fragIdx uint16)
	onReassemblyRejected()
	onFragmentTimeout()
}

// Reassembler reassembles fragmented messages. It is NOT safe for
// concurrent use; callers (e.g. Session) must serialize access.
type Reassembler struct {
	cfg      ReassemblyConfig
	now      func() time.Time // injectable clock for deterministic tests
	inFlight map[uint32]*assembly
	sink     reassemblyEventSink
}

// NewReassembler creates a Reassembler with the given config.
func NewReassembler(cfg ReassemblyConfig) *Reassembler {
	return &Reassembler{cfg: cfg, now: time.Now, inFlight: make(map[uint32]*assembly)}
}

// SetEventSink attaches the observability sink (used by Session).
func (r *Reassembler) SetEventSink(sink reassemblyEventSink) { r.sink = sink }

// SetClock overrides the time source (tests only).
func (r *Reassembler) SetClock(now func() time.Time) { r.now = now }

// Push consumes one frame. Unfragmented frames are returned immediately.
// For fragments: out-of-order and duplicate fragments are handled
// deterministically (duplicates ignored, never delivered twice); when all
// fragments of a message arrive, the complete message is returned exactly
// once. Resource limits produce a *ReassemblyError instead of silent drops.
func (r *Reassembler) Push(f *Frame) (msg *ReassembledMessage, err error) {
	if !f.Fragmented() {
		data := make([]byte, len(f.Payload))
		copy(data, f.Payload)
		return &ReassembledMessage{MsgID: f.MsgID, Data: data}, nil
	}

	a, ok := r.inFlight[f.MsgID]
	if !ok {
		// Enforce limits before allocating anything.
		if len(r.inFlight) >= r.cfg.MaxInFlight {
			if r.sink != nil {
				r.sink.onReassemblyRejected()
			}
			return nil, &ReassemblyError{MsgID: f.MsgID, Reason: fmt.Errorf(
				"%w: %d messages already in flight", ErrReassemblyOverflow, len(r.inFlight))}
		}
		if int(f.FragCnt)*MaxPayload > r.cfg.MaxMessageSize {
			if r.sink != nil {
				r.sink.onReassemblyRejected()
			}
			return nil, &ReassemblyError{MsgID: f.MsgID, Reason: fmt.Errorf(
				"%w: %d fragments could exceed max message size %d",
				ErrMessageTooLarge, f.FragCnt, r.cfg.MaxMessageSize)}
		}
		a = &assembly{
			fragCnt: f.FragCnt,
			frags:   make([][]byte, f.FragCnt),
			created: r.now(),
		}
		r.inFlight[f.MsgID] = a
	}

	// Fragments of one message must agree on FragCnt.
	if f.FragCnt != a.fragCnt {
		delete(r.inFlight, f.MsgID)
		return nil, &ReassemblyError{MsgID: f.MsgID, Reason: fmt.Errorf(
			"%w: inconsistent FragCnt %d != %d", ErrBadLength, f.FragCnt, a.fragCnt)}
	}

	// Duplicate fragment: it still counts as one fragment and the message
	// is still delivered at most once; report the occurrence for
	// observability and ignore the bytes.
	if a.frags[f.FragIdx] != nil {
		if r.sink != nil {
			r.sink.onDuplicateFragment(f.MsgID, f.FragIdx)
		}
		return nil, nil
	}

	frag := make([]byte, len(f.Payload))
	copy(frag, f.Payload)
	a.frags[f.FragIdx] = frag
	a.received++
	a.totalSize += len(frag)
	if a.totalSize > r.cfg.MaxMessageSize {
		delete(r.inFlight, f.MsgID)
		if r.sink != nil {
			r.sink.onReassemblyRejected()
		}
		return nil, &ReassemblyError{MsgID: f.MsgID, Reason: fmt.Errorf(
			"%w: accumulated %d bytes > max %d", ErrReassemblyOverflow, a.totalSize, r.cfg.MaxMessageSize)}
	}

	if a.received < int(a.fragCnt) {
		return nil, nil
	}

	// Complete: assemble in fragment order and deliver exactly once.
	out := make([]byte, 0, a.totalSize)
	for _, frag := range a.frags {
		out = append(out, frag...)
	}
	delete(r.inFlight, f.MsgID)
	return &ReassembledMessage{MsgID: f.MsgID, Data: out}, nil
}

// Expire drops incomplete messages older than FragmentTimeout, returning
// one deterministic error per expired message (ordered by MsgID for
// stable output).
func (r *Reassembler) Expire() []error {
	var errs []error
	now := r.now()
	for id, a := range r.inFlight {
		if now.Sub(a.created) >= r.cfg.FragmentTimeout {
			if r.sink != nil {
				r.sink.onFragmentTimeout()
			}
			errs = append(errs, &ReassemblyError{MsgID: id, Reason: fmt.Errorf(
				"%w: %d/%d fragments after %s", ErrReassemblyTimeout, a.received, a.fragCnt,
				r.cfg.FragmentTimeout)})
			delete(r.inFlight, id)
		}
	}
	return errs
}

// InFlight returns the number of incomplete messages currently buffered.
func (r *Reassembler) InFlight() int { return len(r.inFlight) }

// Fragment splits payload into frames for message msgID, assigning sequence
// numbers starting at seqStart. It returns the frames and the next sequence
// number to use.
func Fragment(msgID uint32, payload []byte, seqStart uint32) (frames []*Frame, nextSeq uint32, err error) {
	if len(payload) > int(^uint16(0))*MaxPayload {
		return nil, seqStart, ErrMessageTooLarge
	}
	cnt := (len(payload) + MaxPayload - 1) / MaxPayload
	if cnt == 0 {
		cnt = 1 // empty message still gets one frame
	}
	seq := seqStart
	for i := 0; i < cnt; i++ {
		start := i * MaxPayload
		end := start + MaxPayload
		if end > len(payload) {
			end = len(payload)
		}
		flags := uint8(0)
		if cnt > 1 {
			flags |= FlagFragment
		}
		frames = append(frames, &Frame{
			Version: Version,
			Flags:   flags,
			Seq:     seq,
			MsgID:   msgID,
			FragIdx: uint16(i),
			FragCnt: uint16(cnt),
			Payload: payload[start:end],
		})
		seq++
	}
	return frames, seq, nil
}
