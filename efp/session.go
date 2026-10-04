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
	cfg       SessionConfig
	localCaps Capabilities

	// tx state: guarded by mu.
	mu      sync.Mutex
	writeMu sync.Mutex // serializes wire writes; never held while acquiring mu
	conn    io.ReadWriteCloser
	closed  bool
	sendSeq uint32
	sendMsg uint32
	// Per-connection capability negotiation state, guarded by capMu.
	peerCaps   Capabilities
	peerFrom   bool          // true once the peer (not the fallback) supplied limits
	capSettled bool          // handshake finished either way; late CAP is ignored
	capDone    chan struct{} // closed when the current connection handshake settled
	capMu      sync.Mutex

	// rx state: only touched by the read loop and the janitor,
	// serialized by rxMu.
	rxMu        sync.Mutex
	rxEpoch     uint64 // connection generation whose data the rx pipeline accepts
	rxDropped   int    // decoder.BytesDropped already folded into stats (delta tracking)
	decoder     *Decoder
	reassembler *Reassembler
	delivered   map[uint32]struct{}
	delivOrder  []uint32 // FIFO ring of delivered MsgIDs, bounded by dedupWindow

	stats   stats
	delivCh chan ReassembledMessage
	errCh   chan error
	eventCh chan DuplicateFragmentEvent

	done chan struct{}
	wg   sync.WaitGroup
}

// SessionConfig configures a Session.
type SessionConfig struct {
	Reassembly ReassemblyConfig
	// NegotiationTimeout bounds how long Attach waits for the peer's
	// capability advertisement before applying conservative defaults.
	// Zero means DefaultNegotiationTimeout.
	NegotiationTimeout time.Duration
	// LocalCapabilities optionally overrides the limits this session
	// advertises. Zero values are filled from the Reassembly config.
	LocalCapabilities *Capabilities
}

// NewSession creates a session without a transport. Call Attach to bind one.
func NewSession(cfg SessionConfig) *Session {
	if cfg.Reassembly.MaxInFlight == 0 {
		cfg.Reassembly = DefaultReassemblyConfig()
	}
	if cfg.NegotiationTimeout <= 0 {
		cfg.NegotiationTimeout = DefaultNegotiationTimeout
	}
	local := Capabilities{
		MaxMessageSize: uint32(cfg.Reassembly.MaxMessageSize),
		MaxInFlight:    uint32(cfg.Reassembly.MaxInFlight),
	}
	if cfg.LocalCapabilities != nil {
		if cfg.LocalCapabilities.MaxMessageSize > 0 {
			local.MaxMessageSize = cfg.LocalCapabilities.MaxMessageSize
		}
		if cfg.LocalCapabilities.MaxInFlight > 0 {
			local.MaxInFlight = cfg.LocalCapabilities.MaxInFlight
		}
	}
	s := &Session{
		cfg:         cfg,
		localCaps:   local,
		decoder:     NewDecoder(),
		reassembler: NewReassembler(cfg.Reassembly),
		delivered:   make(map[uint32]struct{}),
		delivCh:     make(chan ReassembledMessage, 64),
		errCh:       make(chan error, 64),
		eventCh:     make(chan DuplicateFragmentEvent, 64),
		done:        make(chan struct{}),
	}
	s.reassembler.SetEventSink(s)
	s.wg.Add(1)
	go s.janitor()
	return s
}

// --- reassemblyEventSink: all calls happen on the rx side (read loop or
// janitor); the counters themselves are atomic so only the event channel
// needs rx serialization to be non-blocking. ---

func (s *Session) onDuplicateFragment(msgID uint32, fragIdx uint16) {
	s.stats.addDuplicateFragment()
	select {
	case s.eventCh <- DuplicateFragmentEvent{MsgID: msgID, FragIdx: fragIdx}:
	default: // never block the read loop if the application is not reading events
	}
}

func (s *Session) onReassemblyRejected() { s.stats.addReassemblyRejected() }
func (s *Session) onFragmentTimeout()    { s.stats.addFragmentTimeout() }

// Attach binds a new transport (initial connect or reconnect) and starts
// the read loop. Any previous transport is closed.
//
// Connection boundaries are strict: the decoder buffer is reset and a new
// connection epoch begins, so a half frame left on the dead transport can
// never be concatenated with the new transport's bytes into a corrupt
// frame. Incoming reassembly state is intentionally kept across connects:
// the peer may replay fragments and finish an oversized message on the new
// transport.
//
// Right after binding, a capability advertisement is exchanged so the
// session knows the peer's receive limits. Attach does NOT block on the
// peer: a legacy peer (older firmware that never sends the advertisement)
// settles on conservative defaults after NegotiationTimeout. The first
// SendMessage waits for that settlement before choosing the effective
// limit.
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
		s.stats.addReconnect()
	}

	// Begin a fresh receive epoch: drop any half frame from the dead
	// transport so connections' bytes can never mix.
	s.rxMu.Lock()
	s.rxEpoch++
	epoch := s.rxEpoch
	s.decoder.Reset()
	s.rxMu.Unlock()

	// Fresh per-connection handshake state.
	done := make(chan struct{})
	s.capMu.Lock()
	s.peerCaps = DefaultCapabilities()
	s.peerFrom = false
	s.capSettled = false
	s.capDone = done
	s.capMu.Unlock()

	// Announce our own receive limits. Writing on a blocking transport
	// must not delay Attach; a dead transport simply fails and the read
	// loop exits, while SendMessage falls back to defaults. The writer is
	// tracked by wg so Close waits for (or, via transport close, unblocks)
	// it and it never touches the session after shutdown.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.writeCapAdvertisement(conn, epoch)
	}()

	s.wg.Add(1)
	go s.readLoop(conn, epoch)

	// Settle the handshake in the background (peer CAP or timeout). This
	// goroutine exits once done is closed or the session shuts down; it
	// never holds a frame in the application-facing path.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.settleHandshake(done)
	}()
	return nil
}

// writeCapAdvertisement sends this session's receive limits once.
// The wire write takes writeMu (never mu) so Attach can still swap and
// close the transport if the peer stops draining its socket; that close
// unblocks this write with an error.
func (s *Session) writeCapAdvertisement(conn io.ReadWriteCloser, epoch uint64) {
	f, err := EncodeCapFrame(s.localCaps)
	if err != nil {
		return
	}
	b, err := f.Encode()
	if err != nil {
		return
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	if s.closed || s.conn != conn {
		s.mu.Unlock()
		return // stale transport: a newer Attach already replaced it
	}
	s.mu.Unlock()
	_, _ = conn.Write(b)
}

// settleHandshake waits for the peer advertisement deadline and records
// whichever outcome won; settlement via a received CAP frame happens in
// consume(), which closes done itself.
func (s *Session) settleHandshake(done chan struct{}) {
	t := time.NewTimer(s.cfg.NegotiationTimeout)
	defer t.Stop()
	select {
	case <-done:
		return
	case <-t.C:
	case <-s.done:
		return
	}
	s.capMu.Lock()
	defer s.capMu.Unlock()
	if s.capDone != done {
		return // a newer connection superseded this handshake
	}
	if !s.capSettled {
		s.capSettled = true
		s.peerCaps = DefaultCapabilities()
		s.stats.addNegotiationFallback()
		close(done)
	}
}

// applyPeerCaps records the peer advertisement on the current handshake.
// Callers must hold rxMu (so the connection epoch cannot change) and must
// have verified the frame belongs to the current epoch.
func (s *Session) applyPeerCaps(c Capabilities) bool {
	s.capMu.Lock()
	defer s.capMu.Unlock()
	if s.capSettled {
		return false // timeout won or duplicate advertisement
	}
	s.capSettled = true
	s.peerCaps = c
	s.peerFrom = true
	s.stats.addNegotiation()
	close(s.capDone)
	return true
}

// SendMessage fragments (if needed) and transmits one application message.
// It returns the message ID assigned to it.
//
// Before anything touches the wire, the message is checked against the
// receive limit negotiated with the peer on the current connection: a
// message larger than what the peer advertised (or larger than the
// conservative default for legacy peers) fails immediately with an error
// wrapping ErrPeerLimitExceeded. No bytes are sent, so the peer cannot
// silently drop an over-limit message and the caller gets a signal it can
// distinguish from transport errors.
func (s *Session) SendMessage(payload []byte) (msgID uint32, err error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, ErrSessionClosed
	}
	conn := s.conn
	done := s.capDone
	s.mu.Unlock()
	if conn == nil {
		return 0, errors.New("efp: no transport attached")
	}

	// Wait for this connection's capability settlement (fast path: it is
	// usually already closed). If a reconnect races in here, bail out so
	// the caller retries against the new limits instead of writing stale.
	select {
	case <-done:
	case <-s.done:
		return 0, ErrSessionClosed
	}
	if !s.isCurrent(conn) {
		return 0, errors.New("efp: transport changed during capability negotiation")
	}
	peer := s.peerLimits()
	if uint64(len(payload)) > uint64(peer.MaxMessageSize) {
		return 0, fmt.Errorf("%w: message %d bytes > peer max %d",
			ErrPeerLimitExceeded, len(payload), peer.MaxMessageSize)
	}

	s.mu.Lock()
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
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, ErrSessionClosed
	}
	if s.conn != conn {
		s.mu.Unlock()
		return 0, errors.New("efp: transport changed while sending")
	}
	s.mu.Unlock()
	for _, b := range encoded {
		if _, err := conn.Write(b); err != nil {
			return 0, fmt.Errorf("efp: write frame: %w", err)
		}
	}
	return msgID, nil
}

// peerLimits returns the limits currently in force on the connection.
func (s *Session) peerLimits() Capabilities {
	s.capMu.Lock()
	defer s.capMu.Unlock()
	return s.peerCaps
}

// PeerCapabilities returns the receive limits currently in force on the
// active connection. Before the first handshake settles, conservative
// defaults are returned; fromPeer is false whenever the value did not come
// from the peer itself (initial state, legacy peer, negotiation timeout).
func (s *Session) PeerCapabilities() (caps Capabilities, fromPeer bool) {
	s.capMu.Lock()
	defer s.capMu.Unlock()
	return s.peerCaps, s.peerFrom
}

// LocalCapabilities returns the limits this session advertises on every
// connection.
func (s *Session) LocalCapabilities() Capabilities { return s.localCaps }

// SnapshotStats returns a point-in-time copy of runtime counters. Safe to
// call concurrently with protocol activity; it never allocates shared
// state and cannot race with the read loop or janitor.
func (s *Session) SnapshotStats() Stats { return s.stats.snapshot() }

// Events returns the channel of distinguished protocol events. Currently
// it emits one DuplicateFragmentEvent per received duplicate fragment; a
// duplicate still counts as one fragment and the message is delivered at
// most once. The channel is non-blocking for the protocol: if the
// application does not drain it, events are dropped, never the data path.
func (s *Session) Events() <-chan DuplicateFragmentEvent { return s.eventCh }

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
	close(s.eventCh)
	return nil
}

// readLoop consumes the transport until it fails or the session closes.
// Every byte is tagged with the connection epoch captured at Attach; data
// read from a transport that was since replaced is discarded in consume.
func (s *Session) readLoop(conn io.ReadWriteCloser, epoch uint64) {
	defer s.wg.Done()
	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			s.consume(buf[:n], epoch)
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

// consume feeds bytes through decoder, reassembler and dedup. All bytes
// originating from a stale connection (an Attach replaced the transport
// after the read had already returned) are discarded without touching the
// decoder, guaranteeing a strict per-connection receive boundary.
func (s *Session) consume(data []byte, epoch uint64) {
	s.rxMu.Lock()
	defer s.rxMu.Unlock()
	if epoch != s.rxEpoch {
		return // late bytes from a replaced transport: never mixed in
	}
	frames, derrs := s.decoder.Feed(data)
	s.stats.addDecodeErrors(len(derrs))
	if d := s.decoder.BytesDropped - s.rxDropped; d > 0 {
		s.stats.addBytesDropped(d)
		s.rxDropped = s.decoder.BytesDropped
	}
	for _, err := range derrs {
		s.reportErr(err)
	}
	s.stats.addFramesDecoded(len(frames))
	for _, f := range frames {
		// Control frames terminate the capability handshake and never
		// reach reassembly or the application.
		if f.Flags&FlagControl != 0 {
			caps, err := DecodeCapFrame(f)
			if err != nil {
				s.reportErr(err)
				continue
			}
			s.applyPeerCaps(caps)
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
		s.stats.addMessageDelivered()
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
