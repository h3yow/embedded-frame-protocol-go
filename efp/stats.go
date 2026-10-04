package efp

import "sync/atomic"

// Stats is a point-in-time snapshot of session runtime counters.
// It is a plain value type: reading it concurrently with session activity
// never races with the producing goroutines (all underlying counters are
// atomics), and a snapshot never changes after it is returned.
type Stats struct {
	FramesDecoded      uint64 // frames successfully decoded on any connection
	BytesDropped       uint64 // noise bytes skipped while resynchronizing
	DecodeErrors       uint64 // bad magic / version / length / CRC errors
	DuplicateFragments uint64 // repeated fragments received (ignored, count only)
	FragmentTimeouts   uint64 // incomplete messages expired before completion
	ReassemblyRejects  uint64 // new messages refused (in-flight / size limits)
	MessagesDelivered  uint64 // complete messages delivered to the application
	Reconnects         uint64 // Attach calls that replaced a previous transport
	Negotiations       uint64 // capability handshakes completed with the peer
	NegotiationFalls   uint64 // handshakes that timed out and used defaults
}

// DuplicateFragmentEvent reports a received duplicate fragment. The
// duplicate still counts as only one fragment and the message is still
// delivered at most once; the event is purely informational and lets the
// application distinguish duplicate occurrences from the aggregate counter.
type DuplicateFragmentEvent struct {
	MsgID   uint32
	FragIdx uint16
}

// stats holds the live counters behind a Stats snapshot. Every field is
// updated atomically so producers (read loop, janitor, senders) and
// concurrent Snapshot readers never trigger a data race.
type stats struct {
	framesDecoded      atomic.Uint64
	bytesDropped       atomic.Uint64
	decodeErrors       atomic.Uint64
	duplicateFragments atomic.Uint64
	fragmentTimeouts   atomic.Uint64
	reassemblyRejects  atomic.Uint64
	messagesDelivered  atomic.Uint64
	reconnects         atomic.Uint64
	negotiations       atomic.Uint64
	negotiationFalls   atomic.Uint64
}

func (s *stats) addFramesDecoded(n int)  { s.framesDecoded.Add(uint64(n)) }
func (s *stats) addBytesDropped(n int)   { s.bytesDropped.Add(uint64(n)) }
func (s *stats) addDecodeErrors(n int)   { s.decodeErrors.Add(uint64(n)) }
func (s *stats) addDuplicateFragment()   { s.duplicateFragments.Add(1) }
func (s *stats) addFragmentTimeout()     { s.fragmentTimeouts.Add(1) }
func (s *stats) addReassemblyRejected()  { s.reassemblyRejects.Add(1) }
func (s *stats) addMessageDelivered()    { s.messagesDelivered.Add(1) }
func (s *stats) addReconnect()           { s.reconnects.Add(1) }
func (s *stats) addNegotiation()         { s.negotiations.Add(1) }
func (s *stats) addNegotiationFallback() { s.negotiationFalls.Add(1) }

// snapshot returns a consistent copy of the counters. Safe to call at any
// time, from any goroutine.
func (s *stats) snapshot() Stats {
	return Stats{
		FramesDecoded:      s.framesDecoded.Load(),
		BytesDropped:       s.bytesDropped.Load(),
		DecodeErrors:       s.decodeErrors.Load(),
		DuplicateFragments: s.duplicateFragments.Load(),
		FragmentTimeouts:   s.fragmentTimeouts.Load(),
		ReassemblyRejects:  s.reassemblyRejects.Load(),
		MessagesDelivered:  s.messagesDelivered.Load(),
		Reconnects:         s.reconnects.Load(),
		Negotiations:       s.negotiations.Load(),
		NegotiationFalls:   s.negotiationFalls.Load(),
	}
}
