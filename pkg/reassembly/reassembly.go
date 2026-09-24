// Package reassembly splits large application messages into fixed-size
// fragments and deterministically reassembles them at the receiver.
//
// Fragment payload layout (carried inside a frame; session id and sequence
// number are taken from the enclosing frame and are not repeated here):
//
//	offset  size  field
//	0       1     flags (FlagFirst / FlagLast)
//	1       2     fragment index (0-based, little-endian)
//	3       2     total fragment count (little-endian)
//	5       ...   fragment data
package reassembly

import (
	"bytes"
	"encoding/binary"
	"sync"
	"time"
)

// bytesEqual and timeNow are indirections kept tiny on purpose.
func bytesEqual(a, b []byte) bool { return bytes.Equal(a, b) }

var timeNow = time.Now

// Fragment flags.
const (
	FlagNone  byte = 0
	FlagFirst byte = 1 << 0
	FlagLast  byte = 1 << 1
)

// MetaLength is the fixed per-fragment meta prefix inside the frame payload.
const MetaLength = 5

// Fragment is one piece of an application message.
type Fragment struct {
	Session  uint64
	Sequence uint32 // message sequence number
	Index    uint16 // fragment index, 0-based
	Total    uint16 // total fragment count
	Flags    byte
	Data     []byte
}

// Fragment-level distinguishable errors.
var (
	ErrTooManyFragments = reassemblyError("fragment total exceeds configured maximum")
	ErrTotalMismatch    = reassemblyError("fragment count mismatch within message")
	ErrIndexOutOfRange  = reassemblyError("fragment index out of range")
	ErrPayloadOverflow  = reassemblyError("fragment payload exceeds declared size")
	ErrPartialDuplicate = reassemblyError("duplicate fragment with different payload")
	ErrMessageTooLarge  = reassemblyError("message exceeds reassembly size limit")
	ErrTooManyPending   = reassemblyError("too many partially received messages")
	ErrInvalidFragment  = reassemblyError("malformed fragment metadata")
	ErrAssemblerClosed  = reassemblyError("reassembler closed")
)

// Default limits.
const (
	DefaultFragmentSize = 1024
	DefaultMaxTotal     = 64
	DefaultMaxPending   = 32
	DefaultTTL          = 30 * time.Second
)

// Config bounds reassembly resource usage.
type Config struct {
	FragmentSize int           // max payload bytes per fragment
	MaxTotal     int           // max fragments per message
	MaxPending   int           // max concurrently incomplete messages
	MaxMessage   int           // max assembled message size in bytes
	FragmentTTL  time.Duration // age after which an incomplete message times out
}

// DefaultConfig returns conservative bounded defaults.
func DefaultConfig() Config {
	return Config{
		FragmentSize: DefaultFragmentSize,
		MaxTotal:     DefaultMaxTotal,
		MaxPending:   DefaultMaxPending,
		MaxMessage:   DefaultFragmentSize * DefaultMaxTotal,
		FragmentTTL:  DefaultTTL,
	}
}

func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.FragmentSize <= 0 {
		c.FragmentSize = d.FragmentSize
	}
	if c.MaxTotal <= 0 {
		c.MaxTotal = d.MaxTotal
	}
	if c.MaxPending <= 0 {
		c.MaxPending = d.MaxPending
	}
	if c.MaxMessage <= 0 {
		c.MaxMessage = c.FragmentSize * c.MaxTotal
	}
	if c.FragmentTTL <= 0 {
		c.FragmentTTL = d.FragmentTTL
	}
	return c
}

// EventType classifies Assembler outcomes.
type EventType int

const (
	// EventProgress (zero value) means the fragment was accepted but the
	// message is not complete yet; nothing to deliver.
	EventProgress EventType = iota
	// EventCompleted means all fragments arrived and Data is the message.
	EventCompleted
	// EventDuplicate means an identical fragment was offered again; no state
	// changed and nothing is delivered twice.
	EventDuplicate
	// EventRejected means the fragment violated protocol or resource limits;
	// see Event.Err for the distinguishable cause.
	EventRejected
	// EventTimeout means the message was abandoned after FragmentTTL.
	EventTimeout
	// EventAborted means partial state was explicitly discarded (reset).
	EventAborted
)

func (t EventType) String() string {
	switch t {
	case EventCompleted:
		return "completed"
	case EventDuplicate:
		return "duplicate"
	case EventRejected:
		return "rejected"
	case EventTimeout:
		return "timeout"
	case EventAborted:
		return "aborted"
	default:
		return "unknown"
	}
}

// Event is a deterministic observable outcome of offering a fragment.
type Event struct {
	Type     EventType
	Sequence uint32
	Index    uint16
	Data     []byte // populated only for EventCompleted
	Err      error  // populated only for EventRejected / EventTimeout / EventAborted
}

type pending struct {
	total     uint16
	received  uint16
	chunks    [][]byte
	size      int
	firstSeen time.Time
}

// Assembler reassembles fragments into complete messages.
// It is safe for concurrent use.
type Assembler struct {
	cfg     Config
	mu      sync.Mutex
	pending map[uint32]*pending
	closed  bool
}

// NewAssembler creates an Assembler with the given configuration.
func NewAssembler(cfg Config) *Assembler {
	return &Assembler{cfg: cfg.withDefaults(), pending: make(map[uint32]*pending)}
}

// Offer ingests one fragment and returns zero or more deterministic events.
// Exactly one event is returned per offer.
func (a *Assembler) Offer(f *Fragment) Event {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return Event{Type: EventRejected, Sequence: f.Sequence, Index: f.Index, Err: ErrAssemblerClosed}
	}
	if err := validate(f); err != nil {
		return Event{Type: EventRejected, Sequence: f.Sequence, Index: f.Index, Err: err}
	}
	if int(f.Total) > a.cfg.MaxTotal {
		return Event{Type: EventRejected, Sequence: f.Sequence, Index: f.Index, Err: ErrTooManyFragments}
	}

	p, ok := a.pending[f.Sequence]
	if !ok {
		if len(a.pending) >= a.cfg.MaxPending {
			return Event{Type: EventRejected, Sequence: f.Sequence, Index: f.Index, Err: ErrTooManyPending}
		}
		p = &pending{
			total:  f.Total,
			chunks: make([][]byte, f.Total),
		}
		a.pending[f.Sequence] = p
	} else if p.total != f.Total {
		return Event{Type: EventRejected, Sequence: f.Sequence, Index: f.Index, Err: ErrTotalMismatch}
	}

	// Duplicate handling is deterministic: identical -> duplicate event;
	// conflicting -> rejection, state untouched.
	if existing := p.chunks[f.Index]; existing != nil {
		if bytesEqual(existing, f.Data) {
			return Event{Type: EventDuplicate, Sequence: f.Sequence, Index: f.Index}
		}
		return Event{Type: EventRejected, Sequence: f.Sequence, Index: f.Index, Err: ErrPartialDuplicate}
	}

	if p.size+len(f.Data) > a.cfg.MaxMessage {
		return Event{Type: EventRejected, Sequence: f.Sequence, Index: f.Index, Err: ErrPayloadOverflow}
	}

	now := timeNow()
	if p.firstSeen.IsZero() {
		p.firstSeen = now
	}
	chunk := make([]byte, len(f.Data))
	copy(chunk, f.Data)
	p.chunks[f.Index] = chunk
	p.size += len(chunk)
	p.received++

	if p.received != p.total {
		return Event{} // no event: fragment accepted, message incomplete
	}

	data := make([]byte, 0, p.size)
	for _, c := range p.chunks {
		data = append(data, c...)
	}
	delete(a.pending, f.Sequence)
	return Event{Type: EventCompleted, Sequence: f.Sequence, Data: data}
}

// PendingCount returns the number of incomplete messages held.
func (a *Assembler) PendingCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.pending)
}

// SweepExpired abandons messages whose first fragment is older than the TTL
// at time now. Returns one timeout event per abandoned message.
func (a *Assembler) SweepExpired(now time.Time) []Event {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []Event
	for seq, p := range a.pending {
		if now.Sub(p.firstSeen) > a.cfg.FragmentTTL {
			delete(a.pending, seq)
			out = append(out, Event{
				Type:     EventTimeout,
				Sequence: seq,
				Err:      ErrMessageIncomplete,
			})
		}
	}
	return out
}

// Reset drops all partial state (used after reconnect). Every abandoned
// message produces an EventAborted so the loss is never silent.
func (a *Assembler) Reset() []Event {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]Event, 0, len(a.pending))
	for seq := range a.pending {
		out = append(out, Event{
			Type:     EventAborted,
			Sequence: seq,
			Err:      ErrMessageIncomplete,
		})
	}
	a.pending = make(map[uint32]*pending)
	return out
}

// Close prevents further offers; pending state is retained until Reset.
func (a *Assembler) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closed = true
	return nil
}

func validate(f *Fragment) error {
	if f == nil || f.Total == 0 || int(f.Index) >= int(f.Total) {
		return ErrInvalidFragment
	}
	if f.Total == 1 {
		if f.Flags != FlagFirst|FlagLast {
			return ErrInvalidFragment
		}
		return nil
	}
	switch f.Index {
	case 0:
		if f.Flags != FlagFirst {
			return ErrInvalidFragment
		}
	case f.Total - 1:
		if f.Flags != FlagLast {
			return ErrInvalidFragment
		}
	default:
		if f.Flags != FlagNone {
			return ErrInvalidFragment
		}
	}
	return nil
}

// Splitter fragments application messages for sending.
type Splitter struct {
	cfg Config
}

// NewSplitter creates a Splitter.
func NewSplitter(cfg Config) *Splitter {
	return &Splitter{cfg: cfg.withDefaults()}
}

// Split breaks payload into deterministic fragments carrying sequence seq.
// An empty payload yields a single empty first|last fragment.
func (s *Splitter) Split(session uint64, seq uint32, payload []byte) ([]Fragment, error) {
	if len(payload) > s.cfg.MaxMessage {
		return nil, ErrMessageTooLarge
	}
	n := (len(payload) + s.cfg.FragmentSize - 1) / s.cfg.FragmentSize
	if n == 0 {
		n = 1
	}
	if n > s.cfg.MaxTotal {
		return nil, ErrTooManyFragments
	}
	out := make([]Fragment, 0, n)
	for i := 0; i < n; i++ {
		start := i * s.cfg.FragmentSize
		end := start + s.cfg.FragmentSize
		if end > len(payload) {
			end = len(payload)
		}
		var flags byte
		if i == 0 {
			flags |= FlagFirst
		}
		if i == n-1 {
			flags |= FlagLast
		}
		frag := Fragment{
			Session:  session,
			Sequence: seq,
			Index:    uint16(i),
			Total:    uint16(n),
			Flags:    flags,
			Data:     payload[start:end],
		}
		out = append(out, frag)
	}
	return out, nil
}

// EncodeFragment serializes fragment meta and data into a frame payload.
// Session and sequence are carried by the enclosing frame, not encoded here.
func EncodeFragment(f *Fragment) []byte {
	buf := make([]byte, MetaLength+len(f.Data))
	buf[0] = f.Flags
	binary.LittleEndian.PutUint16(buf[1:3], f.Index)
	binary.LittleEndian.PutUint16(buf[3:5], f.Total)
	copy(buf[MetaLength:], f.Data)
	return buf
}

// DecodeFragment parses a frame payload, attaching session and seq from the
// enclosing frame.
func DecodeFragment(session uint64, seq uint32, b []byte) (*Fragment, error) {
	if len(b) < MetaLength {
		return nil, ErrInvalidFragment
	}
	f := &Fragment{
		Session:  session,
		Sequence: seq,
		Flags:    b[0],
		Index:    binary.LittleEndian.Uint16(b[1:3]),
		Total:    binary.LittleEndian.Uint16(b[3:5]),
	}
	data := make([]byte, len(b)-MetaLength)
	copy(data, b[MetaLength:])
	f.Data = data
	return f, nil
}

type reassemblyError string

func (e reassemblyError) Error() string { return "reassembly: " + string(e) }

// ErrMessageIncomplete marks timeout/abort events for messages that never
// completed. It is intentionally the same sentinel for both so callers can
// distinguish "why" via Event.Type and "what" via errors.Is.
var ErrMessageIncomplete = reassemblyError("message incomplete")
