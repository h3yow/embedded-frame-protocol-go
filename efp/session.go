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
	peerCaps PeerCaps // receive limits advertised by the peer (or defaults)

	// wrMu serializes writes to the transport (data frames and control
	// frames) so bytes of one frame never interleave on the wire. It is
	// held across blocking writes; state locks (mu) are only taken
	// briefly inside, never the other way around.
	wrMu sync.Mutex

	// rx state: only touched by the read loop and the janitor,
	// serialized by rxMu.
	rxMu        sync.Mutex
	decoder     *Decoder
	reassembler *Reassembler
	delivered   map[uint32]struct{}
	delivOrder  []uint32 // FIFO ring of delivered MsgIDs, bounded by dedupWindow

	// rxGen identifies the current receive generation. It is incremented
	// on every Attach, atomically with the decoder reset (both under
	// rxMu). Bytes read from a transport of an older generation are
	// dropped whole in consume, no matter when the read completed.
	rxGen             uint64
	staleBytesDropped uint64

	delivCh chan ReassembledMessage
	errCh   chan error
	eventCh chan DuplicateFragmentEvent

	done chan struct{}
	wg   sync.WaitGroup
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
		peerCaps:    DefaultPeerCaps(),
		delivCh:     make(chan ReassembledMessage, 64),
		errCh:       make(chan error, 64),
		eventCh:     make(chan DuplicateFragmentEvent, 64),
		done:        make(chan struct{}),
	}
	s.reassembler.OnDuplicateFragment = func(msgID uint32, fragIdx uint16) {
		// Informational only; never block the read loop on a full channel.
		select {
		case s.eventCh <- DuplicateFragmentEvent{MsgID: msgID, FragIdx: fragIdx}:
		default:
		}
	}
	s.wg.Add(1)
	go s.janitor()
	return s
}

// Attach binds a new transport (initial connect or reconnect) and starts
// the read loop. Any previous transport is closed. Unfinished incoming
// messages are kept; replayed duplicates are dropped by MsgID dedup.
//
// Receive boundary rule: every Attach atomically (under rxMu) resets the
// stream decoder AND advances the receive generation. A frame decoded on
// the new connection is therefore built exclusively from bytes of that
// connection:
//   - bytes still buffered in the decoder are discarded by the reset;
//   - bytes of the old connection that are read only AFTER the switch
//     (the old read loop was blocked in Read, or its consume was still
//     waiting for rxMu) carry the old generation and are dropped whole
//     in consume — they never enter the new connection's decoder and can
//     never combine with its bytes into a plausible-looking frame.
//
// Message-level reassembly state, in contrast, deliberately survives
// Attach, so an incomplete message can still be completed by fragments
// arriving on the new transport.
func (s *Session) Attach(conn io.ReadWriteCloser) error {
	// Lock order rxMu -> mu matches consume/handleControl. Holding both
	// makes the transport swap and the receive-boundary reset one atomic
	// step: no read loop can observe the new connection with the old
	// decoder state, or the old generation with the new connection.
	s.rxMu.Lock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.rxMu.Unlock()
		return ErrSessionClosed
	}
	old := s.conn
	s.conn = conn
	// Renegotiate on every new connection: fall back to defaults until the
	// peer's hello arrives, then the advertised limits take effect at once.
	s.peerCaps = DefaultPeerCaps()
	s.rxGen++
	s.decoder.Reset()
	gen := s.rxGen
	s.mu.Unlock()
	s.rxMu.Unlock()
	if old != nil {
		old.Close()
	}
	s.wg.Add(2)
	go s.readLoop(conn, gen)
	go s.sendHello(conn)
	return nil
}

// sendHello makes a best-effort announcement of our receive capabilities
// on the new transport. It runs asynchronously because a write may block
// on synchronous transports (net.Pipe) until the peer reads; a blocked
// hello is abandoned when the transport is replaced or closed.
func (s *Session) sendHello(conn io.ReadWriteCloser) {
	defer s.wg.Done()
	s.mu.Lock()
	if s.closed || s.conn != conn {
		s.mu.Unlock()
		return
	}
	seq := s.sendSeq
	s.sendSeq++
	local := PeerCaps{
		MaxMessageSize: s.cfg.Reassembly.MaxMessageSize,
		MaxInFlight:    s.cfg.Reassembly.MaxInFlight,
	}
	s.mu.Unlock()
	f, err := encodeHello(seq, local)
	if err != nil {
		return
	}
	b, err := f.Encode()
	if err != nil {
		return
	}
	s.wrMu.Lock()
	defer s.wrMu.Unlock()
	if !s.isCurrent(conn) {
		return
	}
	// Best effort: if the write fails, defaults simply stay in effect.
	_, _ = conn.Write(b)
}

// SendMessage fragments (if needed) and transmits one application message.
// It returns the message ID assigned to it.
//
// The payload is checked against the peer's advertised receive limit
// (PeerCaps.MaxMessageSize) BEFORE anything is written: an oversized
// message fails immediately with an error matching ErrExceedsPeerLimit,
// instead of being sent and silently dropped by the receiver.
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
	if limit := s.peerCaps.MaxMessageSize; len(payload) > limit {
		pc := s.peerCaps
		s.mu.Unlock()
		return 0, fmt.Errorf("%w: %d bytes > limit %d (negotiated=%v)",
			ErrExceedsPeerLimit, len(payload), limit, pc.Negotiated)
	}
	msgID = s.sendMsg
	s.sendMsg++
	if s.sendMsg == capsReservedMsgID {
		s.sendMsg++ // 0xFFFFFFFF is reserved for control traffic
	}
	frames, nextSeq, err := Fragment(msgID, payload, s.sendSeq)
	if err != nil {
		s.mu.Unlock()
		return 0, err
	}
	s.sendSeq = nextSeq
	s.mu.Unlock()

	// Encode outside the lock, write under the write lock so concurrent
	// SendMessage calls and control frames never interleave bytes of a
	// single frame on the wire.
	encoded := make([][]byte, 0, len(frames))
	for _, f := range frames {
		b, err := f.Encode()
		if err != nil {
			return 0, err
		}
		encoded = append(encoded, b)
	}
	s.wrMu.Lock()
	defer s.wrMu.Unlock()
	if !s.isCurrent(conn) {
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

// PeerCaps returns the receive limits currently in effect for the peer:
// the values from its hello frame, or DefaultPeerCaps() if no hello has
// arrived on the current connection. Safe to call from any goroutine.
func (s *Session) PeerCaps() PeerCaps {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peerCaps
}

// Stats returns a snapshot of the session's runtime counters. It is safe
// to call from any goroutine at any time; the snapshot is internally
// consistent because all counted state is mutated under rxMu.
func (s *Session) Stats() Stats {
	s.rxMu.Lock()
	defer s.rxMu.Unlock()
	dup, timeouts, inflightRej := s.reassembler.counters()
	return Stats{
		FramesDecoded:      uint64(s.decoder.FramesDecoded),
		DecodeErrors:       uint64(s.decoder.Errors),
		NoiseBytesSkipped:  uint64(s.decoder.BytesDropped),
		DuplicateFragments: dup,
		FragmentTimeouts:   timeouts,
		InFlightRejected:   inflightRej,
		StaleBytesDropped:  s.staleBytesDropped,
	}
}

// Errors returns a channel of protocol-level errors (bad frames,
// reassembly failures, transport read errors).
func (s *Session) Errors() <-chan error { return s.errCh }

// Events returns a channel of informational duplicate-fragment events.
// Each event corresponds to exactly one ignored duplicate fragment (also
// counted in Stats.DuplicateFragments); delivery semantics are unaffected.
func (s *Session) Events() <-chan DuplicateFragmentEvent { return s.eventCh }

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
	close(s.eventCh)
	return nil
}

// readLoop consumes the transport until it fails or the session closes.
// gen is the receive generation this loop belongs to; it is stamped on
// every chunk handed to consume.
func (s *Session) readLoop(conn io.ReadWriteCloser, gen uint64) {
	defer s.wg.Done()
	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			s.consume(gen, buf[:n])
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
//
// Bytes stamped with a stale generation (read from a transport that a
// later Attach has already replaced) are dropped whole, regardless of
// when the read happened: they belong to the previous connection's byte
// stream and must never be parsed as part of the new one. The drop is
// counted in Stats.StaleBytesDropped. This is a byte-level boundary only
// — message-level reassembly state is untouched and survives.
func (s *Session) consume(gen uint64, data []byte) {
	s.rxMu.Lock()
	defer s.rxMu.Unlock()
	if gen != s.rxGen {
		s.staleBytesDropped += uint64(len(data))
		return
	}
	frames, errs := s.decoder.Feed(data)
	for _, err := range errs {
		s.reportErr(err)
	}
	for _, f := range frames {
		if f.Flags&FlagControl != 0 {
			s.handleControl(f)
			continue
		}
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

// handleControl applies a session-layer control frame. Called from
// consume (rxMu held); takes mu only briefly (lock order: rxMu before mu).
func (s *Session) handleControl(f *Frame) {
	caps, err := decodeHello(f.Payload)
	if err != nil {
		s.reportErr(err)
		return
	}
	s.mu.Lock()
	s.peerCaps = caps
	s.mu.Unlock()
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
