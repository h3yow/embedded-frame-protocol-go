package efp

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// dedupWindow is the number of recently delivered message IDs remembered
// for duplicate suppression across reconnects.
const dedupWindow = 1024

// Session manages a framed, sequenced, reconnectable conversation over a
// byte-stream transport. It is safe for concurrent use.
//
// Reconnect semantics: the session keeps its send sequence/message counters
// and its receive dedup state across Attach calls. A peer that replays
// already delivered messages after reconnect will have them dropped by the
// dedup window, so the application never consumes a message twice.
// Incomplete incoming messages survive a reconnect and may be completed by
// fragments arriving on the new transport.
type Session struct {
	cfg SessionConfig

	// tx state: guarded by mu.
	mu       sync.Mutex
	conn     io.ReadWriteCloser
	closed   bool
	sendSeq  uint32
	sendMsg  uint32

	// rx state: only touched by the read loop and the janitor,
	// serialized by rxMu.
	rxMu        sync.Mutex
	decoder     *Decoder
	reassembler *Reassembler
	delivered   map[uint32]struct{}
	delivOrder  []uint32 // FIFO ring of delivered MsgIDs, bounded by dedupWindow

	delivCh chan ReassembledMessage
	errCh   chan error

	done   chan struct{}
	wg     sync.WaitGroup
}

// SessionConfig configures a Session.
type SessionConfig struct {
	Reassembly ReassemblyConfig
}

// NewSession creates a session without a transport. Call Attach to bind one.
func NewSession(cfg SessionConfig) *Session {
	if cfg.Reassembly.MaxInFlight == 0 {
		cfg.Reassembly = DefaultReassemblyConfig()
	}
	s := &Session{
		cfg:         cfg,
		decoder:     NewDecoder(),
		reassembler: NewReassembler(cfg.Reassembly),
		delivered:   make(map[uint32]struct{}),
		delivCh:     make(chan ReassembledMessage, 64),
		errCh:       make(chan error, 64),
		done:        make(chan struct{}),
	}
	s.wg.Add(1)
	go s.janitor()
	return s
}

// Attach binds a new transport (initial connect or reconnect) and starts
// the read loop. Any previous transport is closed. Unfinished incoming
// messages are kept; replayed duplicates are dropped by MsgID dedup.
func (s *Session) Attach(conn io.ReadWriteCloser) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrSessionClosed
	}
	old := s.conn
	s.conn = conn
	s.mu.Unlock()
	if old != nil {
		old.Close()
	}
	s.wg.Add(1)
	go s.readLoop(conn)
	return nil
}

// SendMessage fragments (if needed) and transmits one application message.
// It returns the message ID assigned to it.
func (s *Session) SendMessage(payload []byte) (msgID uint32, err error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, ErrSessionClosed
	}
	conn := s.conn
	if conn == nil {
		s.mu.Unlock()
		return 0, errors.New("efp: no transport attached")
	}
	msgID = s.sendMsg
	s.sendMsg++
	frames, nextSeq, err := Fragment(msgID, payload, s.sendSeq)
	if err != nil {
		s.mu.Unlock()
		return 0, err
	}
	s.sendSeq = nextSeq
	s.mu.Unlock()

	// Encode outside the lock, write under a short critical section per
	// frame so concurrent SendMessage calls never interleave bytes of a
	// single frame on the wire.
	encoded := make([][]byte, 0, len(frames))
	for _, f := range frames {
		b, err := f.Encode()
		if err != nil {
			return 0, err
		}
		encoded = append(encoded, b)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrSessionClosed
	}
	for _, b := range encoded {
		if _, err := conn.Write(b); err != nil {
			return 0, fmt.Errorf("efp: write frame: %w", err)
		}
	}
	return msgID, nil
}

// Delivered returns a channel of fully reassembled, deduplicated messages.
func (s *Session) Delivered() <-chan ReassembledMessage { return s.delivCh }

// Errors returns a channel of protocol-level errors (bad frames,
// reassembly failures, transport read errors).
func (s *Session) Errors() <-chan error { return s.errCh }

// Close shuts the session down and closes the current transport.
func (s *Session) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrSessionClosed
	}
	s.closed = true
	conn := s.conn
	s.conn = nil
	s.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
	close(s.done)
	s.wg.Wait()
	close(s.delivCh)
	close(s.errCh)
	return nil
}

// readLoop consumes the transport until it fails or the session closes.
func (s *Session) readLoop(conn io.ReadWriteCloser) {
	defer s.wg.Done()
	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			s.consume(buf[:n])
		}
		if err != nil {
			// Report only if this connection is still the active one;
			// errors from a transport replaced by Attach (reconnect) or
			// closed by Close are expected and not surfaced.
			if !errors.Is(err, io.EOF) && s.isCurrent(conn) {
				s.reportErr(fmt.Errorf("efp: transport read: %w", err))
			}
			return
		}
	}
}

// isCurrent reports whether conn is still the attached transport.
func (s *Session) isCurrent(conn io.ReadWriteCloser) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.closed && s.conn == conn
}

// consume feeds bytes through decoder, reassembler and dedup.
func (s *Session) consume(data []byte) {
	s.rxMu.Lock()
	defer s.rxMu.Unlock()
	frames, errs := s.decoder.Feed(data)
	for _, err := range errs {
		s.reportErr(err)
	}
	for _, f := range frames {
		msg, err := s.reassembler.Push(f)
		if err != nil {
			s.reportErr(err)
			continue
		}
		if msg == nil {
			continue
		}
		if s.isDuplicate(msg.MsgID) {
			continue // replayed after reconnect: drop, never deliver twice
		}
		s.markDelivered(msg.MsgID)
		s.delivCh <- *msg
	}
}

// janitor periodically expires stale reassemblies.
func (s *Session) janitor() {
	defer s.wg.Done()
	interval := s.cfg.Reassembly.FragmentTimeout / 2
	if interval <= 0 {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
			s.rxMu.Lock()
			for _, err := range s.reassembler.Expire() {
				s.reportErr(err)
			}
			s.rxMu.Unlock()
		}
	}
}

// isDuplicate reports whether msgID was already delivered.
// Callers must hold rxMu.
func (s *Session) isDuplicate(msgID uint32) bool {
	_, ok := s.delivered[msgID]
	return ok
}

// markDelivered records msgID, evicting the oldest entry beyond dedupWindow.
// Callers must hold rxMu.
func (s *Session) markDelivered(msgID uint32) {
	s.delivered[msgID] = struct{}{}
	s.delivOrder = append(s.delivOrder, msgID)
	if len(s.delivOrder) > dedupWindow {
		delete(s.delivered, s.delivOrder[0])
		s.delivOrder = s.delivOrder[1:]
	}
}

// reportErr sends err to the error channel without ever blocking the
// read loop; if the channel is full the error is dropped (last-resort).
func (s *Session) reportErr(err error) {
	select {
	case s.errCh <- err:
	default:
	}
}
