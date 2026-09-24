// Package session provides a concurrent, reconnectable application-message
// transport over a reliable byte stream carrying the frame protocol.
//
// Semantics:
//
//   - Send is safe for concurrent use. Outgoing sequence numbers are assigned
//     monotonically by the sender and keep advancing across reconnects.
//   - Recv delivers complete messages strictly in sequence-number order, each
//     at most once. Replayed duplicates (including after reconnect) are
//     filtered by a bounded receive window and never reach the caller twice.
//   - Frames with a foreign session id are rejected with an
//     EventFrameRejected event (wrapping ErrBadSessionID) but do not stop the
//     stream.
//   - On Reconnect all partial reassemblies are abandoned deterministically
//     (their loss is reported as EventMessageTimeout); the de-duplication
//     window survives so replays stay suppressed.
//   - All state access is serialized; the package is race free.
package session

import (
	"errors"
	"sync"
	"time"

	"github.com/h3yow/embedded-frame-protocol-go/pkg/frame"
	"github.com/h3yow/embedded-frame-protocol-go/pkg/reassembly"
)

// Session-level distinguishable errors.
var (
	ErrClosed          = sessionError("session closed")
	ErrReconnectFailed = sessionError("reconnect failed")
	ErrBadSessionID    = sessionError("frame from foreign session rejected")
	ErrMessageTooLarge = sessionError("message too large for configured limits")
	ErrNoTransport     = sessionError("no transport connected")
)

// EventType classifies session events.
type EventType int

const (
	// EventFrameRejected: a bad frame was dropped; Detail wraps a frame error.
	EventFrameRejected EventType = iota
	// EventReconnected: the transport was replaced by a dialed replacement.
	EventReconnected
	// EventMessageTimeout: a partial message was abandoned (TTL or reconnect).
	EventMessageTimeout
	// EventAssemblyRejected: a fragment violated reassembly rules/limits.
	EventAssemblyRejected
	// EventReadError: the reader observed a transport failure.
	EventReadError
)

func (t EventType) String() string {
	switch t {
	case EventFrameRejected:
		return "frame-rejected"
	case EventReconnected:
		return "reconnected"
	case EventMessageTimeout:
		return "message-timeout"
	case EventAssemblyRejected:
		return "assembly-rejected"
	case EventReadError:
		return "read-error"
	default:
		return "unknown"
	}
}

// Event is an observable session occurrence.
type Event struct {
	Type   EventType
	Detail error
	Seq    uint32
	When   time.Time
}

// Config for a Session.
type Config struct {
	Frame    frame.Config
	Assembly reassembly.Config
	Dial     func() (Transport, error) // reconnect dialer
	// SessionID pins the wire session id. Zero means a random id is chosen.
	SessionID uint64
	// StartSequence is the first outgoing sequence number (default 0).
	StartSequence uint32
	// EventBuffer sizes the Events channel (default 64).
	EventBuffer int
}

// Transport is one byte-stream connection.
type Transport interface {
	Read(p []byte) (int, error)
	Write(p []byte) (int, error)
	Close() error
}

// Message is a delivered application message.
type Message struct {
	Sequence uint32
	Data     []byte
}

// recvWindow is a bounded circular record of recently delivered sequence
// numbers. It handles uint32 wrap-around via relative (int32) distance.
type recvWindow struct {
	haveBase bool
	base     uint32 // next sequence expected for in-order delivery
	window   int
	seen     []bool
}

func newRecvWindow(window int) *recvWindow {
	if window <= 0 {
		window = 1024
	}
	return &recvWindow{window: window, seen: make([]bool, window)}
}

// seenSince reports whether seq was already delivered.
func (w *recvWindow) delivered(seq uint32) bool {
	if !w.haveBase {
		return false
	}
	d := int32(seq - w.base)
	if d < 0 || d >= int32(w.window) {
		return true // stale or outside the window on the far side
	}
	return w.seen[int(seq)%w.window]
}

// markDelivered records seq and advances the high-water mark.
func (w *recvWindow) markDelivered(seq uint32) {
	if !w.haveBase {
		w.base = seq
		w.haveBase = true
	}
	if int32(seq-w.base) >= 0 && int32(seq-w.base) < int32(w.window) {
		w.seen[int(seq)%w.window] = true
	}
	for w.seen[int(w.base)%w.window] {
		w.seen[int(w.base)%w.window] = false
		w.base++
	}
}

// Session is a concurrent reconnectable message transport.
type Session struct {
	cfg      Config
	id       uint64
	splitter *reassembly.Splitter

	writeMu sync.Mutex

	mu        sync.Mutex
	transport Transport
	gen       uint64 // incremented on every (re)connect
	closed    bool
	sendSeq   uint32
	cond      *sync.Cond
	// reconnecting is true while a reconnect is in progress and the reader
	// must wait rather than dial itself.
	reconnecting bool

	assembler *reassembly.Assembler
	window    *recvWindow
	ready     map[uint32]*Message // completed, awaiting in-order delivery
	nextSeq   uint32

	frames chan *frame.Frame
	msgs   chan *Message
	events chan Event
	done   chan struct{}

	wg sync.WaitGroup
}

// New creates a session over the initial transport and starts its goroutines.
func New(transport Transport, cfg Config) (*Session, error) {
	if transport == nil {
		return nil, ErrNoTransport
	}
	id := cfg.SessionID
	if id == 0 {
		id = uint64(time.Now().UnixNano())
	}
	buf := cfg.EventBuffer
	if buf <= 0 {
		buf = 64
	}
	s := &Session{
		cfg:       cfg,
		id:        id,
		splitter:  reassembly.NewSplitter(cfg.Assembly),
		transport: transport,
		assembler: reassembly.NewAssembler(cfg.Assembly),
		window:    newRecvWindow(1024),
		ready:     make(map[uint32]*Message),
		nextSeq:   cfg.StartSequence,
		sendSeq:   cfg.StartSequence,
		frames:    make(chan *frame.Frame, 256),
		msgs:      make(chan *Message, 64),
		events:    make(chan Event, buf),
		done:      make(chan struct{}),
	}
	s.cond = sync.NewCond(&s.mu)
	s.wg.Add(3)
	go s.readLoop()
	go s.dispatchLoop()
	go s.sweepLoop()
	return s, nil
}

// Send writes one application message, fragmenting it as needed.
func (s *Session) Send(data []byte) (uint32, error) {
	// writeMu both serializes wire writes and orders sequence assignment so
	// that the byte stream always presents monotonically increasing seqs.
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, ErrClosed
	}
	t := s.transport
	seq := s.sendSeq
	s.sendSeq++
	id := s.id
	s.mu.Unlock()

	if t == nil {
		return 0, ErrNoTransport
	}
	frags, err := s.splitter.Split(id, seq, data)
	if err != nil {
		if errors.Is(err, reassembly.ErrMessageTooLarge) {
			return 0, ErrMessageTooLarge
		}
		return 0, err
	}

	for i := range frags {
		payload := reassembly.EncodeFragment(&frags[i])
		raw, err := frame.Encode(&frame.Frame{
			Version:  frame.Version1,
			Session:  id,
			Sequence: seq,
			Payload:  payload,
		})
		if err != nil {
			return 0, err
		}
		if _, err := writeAll(t, raw); err != nil {
			return 0, err
		}
	}
	return seq, nil
}

// Recv blocks until the next complete, in-order, deduplicated message.
func (s *Session) Recv() (*Message, error) {
	select {
	case m, ok := <-s.msgs:
		if !ok {
			return nil, ErrClosed
		}
		return m, nil
	case <-s.done:
		return nil, ErrClosed
	}
}

// Events returns the channel on which non-fatal events are reported.
func (s *Session) Events() <-chan Event { return s.events }

// Reconnect closes the current transport and dials a replacement. Partial
// reassemblies are abandoned with events; the dedup window survives.
//
// It is safe to call concurrently with the reader loop: the old transport is
// closed first so a blocked Read returns, after which the dialed replacement
// is published under the lock and the single reader loop picks it up.
func (s *Session) Reconnect() error {
	if s.cfg.Dial == nil {
		return ErrReconnectFailed
	}

	// Snapshot and detach the old transport, then close it outside the lock
	// so the reader goroutine blocked in Read can return.
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	old := s.transport
	s.transport = nil
	s.reconnecting = true
	s.cond.Broadcast()
	s.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}

	// Dial with no lock held.
	nt, err := s.cfg.Dial()
	if err != nil {
		s.mu.Lock()
		s.reconnecting = false
		s.cond.Broadcast()
		s.mu.Unlock()
		return errors.Join(ErrReconnectFailed, err)
	}

	s.mu.Lock()
	if s.closed {
		s.reconnecting = false
		s.mu.Unlock()
		_ = nt.Close()
		return ErrClosed
	}
	s.transport = nt
	s.gen++
	s.reconnecting = false
	aborts := s.assembler.Reset()
	// Queued frames belong to the old byte stream; drop them.
drained:
	for {
		select {
		case <-s.frames:
		default:
			break drained
		}
	}
	seqs := make([]uint32, 0, len(aborts))
	for _, ev := range aborts {
		seqs = append(seqs, ev.Sequence)
	}
	// Abandoned sequences are marked as permanently consumed: replays stay
	// suppressed and the in-order cursor skips past them so later messages
	// are not held hostage by a gap at the head.
	deliverable := s.abandonLocked(seqs)
	s.cond.Broadcast()
	s.mu.Unlock()

	s.deliver(deliverable)
	now := time.Now()
	for _, ev := range aborts {
		s.emit(Event{Type: EventMessageTimeout, Detail: ev.Err, Seq: ev.Sequence, When: now})
	}
	s.emit(Event{Type: EventReconnected, When: now})
	return nil
}

// Close shuts the session down.
func (s *Session) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	s.closed = true
	t := s.transport
	s.transport = nil
	s.cond.Broadcast()
	s.mu.Unlock()

	// Signal shutdown first: loops blocked in select must observe done
	// before we wait for them. The reader blocked in Transport.Read is
	// released by closing the transport below.
	close(s.done)
	if t != nil {
		_ = t.Close()
	}
	s.wg.Wait()
	close(s.events)
	close(s.msgs)
	return nil
}

// ---- internals ----

func (s *Session) readLoop() {
	defer s.wg.Done()
	var readBuf [4096]byte
	dec := frame.NewDecoder(s.cfg.Frame)
	for {
		s.mu.Lock()
		for s.transport == nil && !s.closed {
			s.cond.Wait()
		}
		if s.closed {
			s.mu.Unlock()
			return
		}
		t := s.transport
		myGen := s.gen
		s.mu.Unlock()

		n, rerr := t.Read(readBuf[:])
		if n > 0 {
			buf := make([]byte, n)
			copy(buf, readBuf[:n])
			for _, r := range dec.Feed(buf) {
				switch {
				case r.Frame != nil:
					select {
					case s.frames <- r.Frame:
					case <-s.done:
						return
					}
				case r.Err != nil:
					s.emit(Event{Type: EventFrameRejected, Detail: r.Err, When: time.Now()})
				}
			}
		}
		if rerr == nil {
			continue
		}

		// Read failed. Serialize the reconnect decision under lock.
		s.mu.Lock()
		// If the session moved to a newer transport while we were blocked
		// in this Read, abandon this generation and re-read from the new one.
		if s.gen != myGen {
			s.mu.Unlock()
			continue
		}
		if s.closed {
			s.mu.Unlock()
			return
		}
		// This dead transport is still current. Detach it.
		s.transport = nil
		s.cond.Broadcast()
		autoDial := s.cfg.Dial != nil && !s.reconnecting
		waitGen := s.gen
		s.mu.Unlock()

		s.emit(Event{Type: EventReadError, Detail: rerr, When: time.Now()})
		dec.Reset()

		if !autoDial {
			// Either a manual Reconnect owns the replacement, or there is no
			// dialer. In both cases the reader waits until a newer transport
			// is published (gen advances) or the session closes.
			s.mu.Lock()
			for s.gen == waitGen && !s.closed {
				s.cond.Wait()
			}
			s.mu.Unlock()
			continue
		}

		// Reader owns auto-reconnect: dial and publish without re-entering
		// the public Reconnect bookkeeping.
		if err := s.dialAndPublish(); err != nil {
			return
		}
	}
}

// dialAndPublish dials a new transport using the configured Dial and installs
// it, abandoning partial assemblies. Used by the reader for auto-reconnect;
// it does not close the current transport (which already failed).
func (s *Session) dialAndPublish() error {
	if s.cfg.Dial == nil {
		return ErrReconnectFailed
	}
	nt, err := s.cfg.Dial()
	if err != nil {
		s.emit(Event{Type: EventReadError, Detail: errors.Join(ErrReconnectFailed, err), When: time.Now()})
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = nt.Close()
		return ErrClosed
	}
	s.transport = nt
	s.gen++
	aborts := s.assembler.Reset()
drained:
	for {
		select {
		case <-s.frames:
		default:
			break drained
		}
	}
	seqs := make([]uint32, 0, len(aborts))
	for _, ev := range aborts {
		seqs = append(seqs, ev.Sequence)
	}
	deliverable := s.abandonLocked(seqs)
	s.cond.Broadcast()
	s.mu.Unlock()

	s.deliver(deliverable)
	now := time.Now()
	for _, ev := range aborts {
		s.emit(Event{Type: EventMessageTimeout, Detail: ev.Err, Seq: ev.Sequence, When: now})
	}
	s.emit(Event{Type: EventReconnected, When: now})
	return nil
}

// dispatchLoop owns reassembly, dedup and in-order delivery state.
func (s *Session) dispatchLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.done:
			return
		case f := <-s.frames:
			s.dispatchFrame(f)
		}
	}
}

func (s *Session) dispatchFrame(f *frame.Frame) {
	if f.Session != s.id {
		s.emit(Event{
			Type:   EventFrameRejected,
			Detail: &foreignError{cause: ErrBadSessionID, got: f.Session, want: s.id},
			Seq:    f.Sequence,
			When:   time.Now(),
		})
		return
	}
	frag, err := reassembly.DecodeFragment(f.Session, f.Sequence, f.Payload)
	if err != nil {
		s.emit(Event{Type: EventAssemblyRejected, Detail: err, Seq: f.Sequence, When: time.Now()})
		return
	}

	s.mu.Lock()
	ev := s.assembler.Offer(frag)
	switch ev.Type {
	case reassembly.EventCompleted:
		// Duplicate of something already delivered? Suppress.
		if s.window.delivered(ev.Sequence) {
			s.mu.Unlock()
			return
		}
		s.ready[ev.Sequence] = &Message{Sequence: ev.Sequence, Data: ev.Data}
		deliverable := s.drainReadyLocked()
		s.mu.Unlock()
		s.deliver(deliverable)
	case reassembly.EventRejected:
		seq, err := ev.Sequence, ev.Err
		s.mu.Unlock()
		s.emit(Event{Type: EventAssemblyRejected, Detail: err, Seq: seq, When: time.Now()})
	default:
		s.mu.Unlock()
	}
}

// drainReadyLocked removes and returns messages starting at nextSeq while
// consecutive sequence numbers are present, recording them in the window.
func (s *Session) drainReadyLocked() []*Message {
	var out []*Message
	for {
		m, ok := s.ready[s.nextSeq]
		if !ok {
			break
		}
		delete(s.ready, s.nextSeq)
		s.window.markDelivered(s.nextSeq)
		out = append(out, m)
		s.nextSeq++
	}
	return out
}

// abandonLocked records seqs as never-to-be-delivered (timed out or aborted
// on reconnect), removes any completed-but-queued copy, and advances the
// in-order cursor past gaps. Returns messages that become deliverable.
func (s *Session) abandonLocked(seqs []uint32) []*Message {
	abandoned := make(map[uint32]bool, len(seqs))
	for _, seq := range seqs {
		if s.window.delivered(seq) {
			continue // never revoke an already delivered sequence
		}
		abandoned[seq] = true
		s.window.markDelivered(seq)
		delete(s.ready, seq)
	}
	if len(abandoned) == 0 {
		return s.drainReadyLocked()
	}
	// Advance the in-order cursor: at each step the next slot is either a
	// ready message (deliver it) or an abandoned slot (skip it). Stop at the
	// first slot that is neither, since that gap is not yet explained.
	var out []*Message
	for {
		if m, ok := s.ready[s.nextSeq]; ok {
			delete(s.ready, s.nextSeq)
			s.window.markDelivered(s.nextSeq)
			out = append(out, m)
			s.nextSeq++
			continue
		}
		if abandoned[s.nextSeq] {
			delete(abandoned, s.nextSeq)
			s.nextSeq++
			continue
		}
		break
	}
	return out
}

// deliver sends messages to Recv in order, honoring shutdown. It must be
// called without s.mu held.
func (s *Session) deliver(msgs []*Message) {
	for _, m := range msgs {
		select {
		case s.msgs <- m:
		case <-s.done:
			return
		}
	}
}

func (s *Session) sweepLoop() {
	defer s.wg.Done()
	interval := s.cfg.Assembly.FragmentTTL
	if interval <= 0 {
		interval = reassembly.DefaultTTL
	}
	ticker := time.NewTicker(interval / 2)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case now := <-ticker.C:
			s.mu.Lock()
			evs := s.assembler.SweepExpired(now)
			seqs := make([]uint32, 0, len(evs))
			for _, ev := range evs {
				seqs = append(seqs, ev.Sequence)
			}
			deliverable := s.abandonLocked(seqs)
			s.mu.Unlock()
			s.deliver(deliverable)
			for _, ev := range evs {
				s.emit(Event{
					Type:   EventMessageTimeout,
					Detail: ev.Err,
					Seq:    ev.Sequence,
					When:   now,
				})
			}
		}
	}
}

func (s *Session) emit(e Event) {
	select {
	case s.events <- e:
	case <-s.done:
	default:
	}
}

func writeAll(w Transport, p []byte) (int, error) {
	written := 0
	for written < len(p) {
		n, err := w.Write(p[written:])
		written += n
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

type sessionError string

func (e sessionError) Error() string { return "session: " + string(e) }

type foreignError struct {
	cause     error
	got, want uint64
}

func (e *foreignError) Error() string {
	return "session: frame session id mismatch: got " +
		uitoa(e.got) + " want " + uitoa(e.want)
}
func (e *foreignError) Unwrap() error { return e.cause }

func uitoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
