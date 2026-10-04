package efp

import (
	"bytes"
	"net"
	"sync"
	"testing"
	"time"
)

// TestDuplicateFragmentEventAndDedup verifies that a repeated fragment is
// counted, emitted as a DISTINGUISHABLE event, yet still counts as one
// fragment and the message is delivered exactly once.
func TestDuplicateFragmentEventAndDedup(t *testing.T) {
	s := NewSession(SessionConfig{})
	t.Cleanup(func() { s.Close() })
	// Drain errors but leave Delivered/Events for assertions.
	go func() {
		for range s.Errors() {
		}
	}()

	c1a, c1b := net.Pipe()
	if err := s.Attach(c1a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	payload := bytes.Repeat([]byte("dup"), 600) // 1800 bytes -> 2 fragments
	frames, _, err := Fragment(404, payload, 0)
	if err != nil {
		t.Fatalf("Fragment: %v", err)
	}
	enc := [][]byte{mustEncode(t, frames[0]), mustEncode(t, frames[1])}

	// Phase 1: feed fragment 0 three times while the message stays
	// incomplete. The first copy is new, the next two are deterministic
	// duplicates: exactly two events, no delivery (scheduling-independent,
	// because a complete delivery is impossible without fragment 1).
	for i := 0; i < 3; i++ {
		if _, err := c1b.Write(enc[0]); err != nil {
			t.Fatal(err)
		}
	}
	var events []DuplicateFragmentEvent
	deadline := time.After(2 * time.Second)
	for len(events) < 2 {
		select {
		case e := <-s.Events():
			events = append(events, e)
		case m := <-s.Delivered():
			t.Fatalf("incomplete message delivered during duplicate phase: %+v", m)
		case <-deadline:
			t.Fatalf("phase 1: expected 2 dup events, got %d", len(events))
		}
	}
	for _, e := range events {
		if e != (DuplicateFragmentEvent{MsgID: 404, FragIdx: 0}) {
			t.Fatalf("unexpected event %+v, want {404 0}", e)
		}
	}

	// Phase 2: complete the message with fragment 1: exactly one delivery.
	if _, err := c1b.Write(enc[1]); err != nil {
		t.Fatal(err)
	}
	var got ReassembledMessage
	var deliveries int
	deadline = time.After(2 * time.Second)
	for {
		select {
		case m := <-s.Delivered():
			got = m
			deliveries++
			if deliveries > 1 {
				t.Fatalf("message delivered %d times, want exactly 1", deliveries)
			}
		case e := <-s.Events():
			events = append(events, e)
		case <-time.After(150 * time.Millisecond):
			if deliveries == 1 {
				goto done
			}
			t.Fatalf("phase 2: deliveries=%d events=%d", deliveries, len(events))
		case <-deadline:
			t.Fatalf("phase 2 timed out: deliveries=%d", deliveries)
		}
	}
done:
	if got.MsgID != 404 || !bytes.Equal(got.Data, payload) {
		t.Fatalf("delivered mismatch: id=%d len=%d", got.MsgID, len(got.Data))
	}
	if len(events) != 2 {
		t.Fatalf("events=%+v, want exactly 2 duplicate events (both frag 0)", events)
	}
	for _, e := range events {
		if e != (DuplicateFragmentEvent{MsgID: 404, FragIdx: 0}) {
			t.Fatalf("event=%+v, want {MsgID:404 FragIdx:0}", e)
		}
	}
	st := s.SnapshotStats()
	if st.DuplicateFragments != 2 {
		t.Fatalf("DuplicateFragments=%d, want 2", st.DuplicateFragments)
	}
	if st.MessagesDelivered != 1 {
		t.Fatalf("MessagesDelivered=%d, want 1", st.MessagesDelivered)
	}
	t.Logf("dup events=%+v, DuplicateFragments=%d, MessagesDelivered=%d (%d bytes reassembled once)",
		events, st.DuplicateFragments, st.MessagesDelivered, len(got.Data))
}

// TestStatsReassemblyRejectAndTimeout checks the in-flight rejection and
// timeout counters at session level.
func TestStatsReassemblyRejectAndTimeout(t *testing.T) {
	cfg := SessionConfig{}
	cfg.Reassembly = DefaultReassemblyConfig()
	cfg.Reassembly.MaxInFlight = 1
	cfg.Reassembly.FragmentTimeout = 100 * time.Millisecond
	s := NewSession(cfg)
	t.Cleanup(func() { s.Close() })
	go func() {
		for range s.Errors() {
		}
	}()

	c1a, c1b := net.Pipe()
	if err := s.Attach(c1a); err != nil {
		t.Fatalf("attach: %v", err)
	}
	mkFrag := func(msgID uint32) []byte {
		fs, _, _ := Fragment(msgID, bytes.Repeat([]byte{'z'}, 2000), 0)
		return mustEncode(t, fs[0])
	}
	if _, err := c1b.Write(mkFrag(1)); err != nil {
		t.Fatal(err)
	}
	// Second concurrent incomplete message is rejected by the in-flight cap.
	if _, err := c1b.Write(mkFrag(2)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.SnapshotStats().ReassemblyRejects == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if st := s.SnapshotStats(); st.ReassemblyRejects != 1 {
		t.Fatalf("ReassemblyRejects=%d, want 1", st.ReassemblyRejects)
	}

	// Wait for the janitor to expire message 1.
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.SnapshotStats().FragmentTimeouts == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	st := s.SnapshotStats()
	if st.FragmentTimeouts != 1 {
		t.Fatalf("FragmentTimeouts=%d, want 1", st.FragmentTimeouts)
	}
	t.Logf("limits observed: ReassemblyRejects=%d (in-flight full), FragmentTimeouts=%d",
		st.ReassemblyRejects, st.FragmentTimeouts)
}

// TestStatsNoiseAndReconnectCounters drives noise bytes and a reconnect and
// checks the corresponding counters.
func TestStatsNoiseAndReconnectCounters(t *testing.T) {
	s := NewSession(SessionConfig{})
	t.Cleanup(func() { s.Close() })
	drainErrors(t, s)

	c1a, c1b := net.Pipe()
	if err := s.Attach(c1a); err != nil {
		t.Fatalf("attach 1: %v", err)
	}
	noise := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	good := mustEncode(t, &Frame{Seq: 1, MsgID: 50, FragCnt: 1, Payload: []byte("n")})
	if _, err := c1b.Write(append(noise, good...)); err != nil {
		t.Fatal(err)
	}
	m := <-s.Delivered()
	if m.MsgID != 50 {
		t.Fatalf("got msgID=%d, want 50", m.MsgID)
	}

	c2a, c2b := net.Pipe()
	if err := s.Attach(c2a); err != nil {
		t.Fatalf("attach 2: %v", err)
	}
	if _, err := c2b.Write(mustEncode(t, &Frame{Seq: 2, MsgID: 51, FragCnt: 1, Payload: []byte("m")})); err != nil {
		t.Fatal(err)
	}
	if m2 := <-s.Delivered(); m2.MsgID != 51 {
		t.Fatalf("got msgID=%d, want 51", m2.MsgID)
	}
	st := s.SnapshotStats()
	if st.BytesDropped != uint64(len(noise)) {
		t.Fatalf("BytesDropped=%d, want %d", st.BytesDropped, len(noise))
	}
	if st.DecodeErrors < 1 {
		t.Fatalf("DecodeErrors=%d, want >= 1", st.DecodeErrors)
	}
	if st.FramesDecoded != 2 {
		t.Fatalf("FramesDecoded=%d, want 2", st.FramesDecoded)
	}
	if st.MessagesDelivered != 2 {
		t.Fatalf("MessagesDelivered=%d, want 2", st.MessagesDelivered)
	}
	if st.Reconnects != 1 {
		t.Fatalf("Reconnects=%d, want 1", st.Reconnects)
	}
	t.Logf("counters after noise+reconnect: %+v", st)
}

// TestStatsConcurrentRead hammers SnapshotStats and Events from many
// goroutines while the session processes traffic. It primarily exists to be
// run under -race: snapshots must be race-free and always return consistent
// values.
func TestStatsConcurrentRead(t *testing.T) {
	c1, c2 := net.Pipe()
	s1 := NewSession(SessionConfig{})
	s2 := NewSession(SessionConfig{})
	t.Cleanup(func() { s1.Close(); s2.Close() })
	go func() {
		for range s2.Errors() {
		}
	}()
	// Keep the non-blocking-but-buffered event channel drained so a burst
	// of duplicates can never stall the read loop.
	go func() {
		for range s2.Events() {
		}
	}()
	if err := s1.Attach(c1); err != nil {
		t.Fatal(err)
	}
	if err := s2.Attach(c2); err != nil {
		t.Fatal(err)
	}
	waitForPeerCaps(t, s1, uint32(DefaultReassemblyConfig().MaxMessageSize))
	waitForPeerCaps(t, s2, uint32(DefaultReassemblyConfig().MaxMessageSize))

	const readers = 12
	const total = 100
	var wg sync.WaitGroup
	stopReaders := make(chan struct{})
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
					_ = s2.SnapshotStats()
					_ = s1.SnapshotStats()
				}
			}
		}(i)
	}

	// Receiver drains messages concurrently with the producer so the
	// delivery channel can never fill and stall the read loop.
	const wantMsgs = total
	var recvMu sync.Mutex
	var received int
	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		for received < wantMsgs {
			select {
			case <-s2.Delivered():
				recvMu.Lock()
				received++
				recvMu.Unlock()
			case err := <-s2.Errors():
				t.Errorf("unexpected err during concurrent stats test: %v", err)
				return
			case <-time.After(5 * time.Second):
				t.Errorf("receiver stalled at %d/%d", received, wantMsgs)
				return
			}
		}
	}()

	// Producer: 1500-byte messages (each fragments into 2 frames).
	for i := 0; i < total; i++ {
		payload := bytes.Repeat([]byte{byte(i)}, 1500)
		if _, err := s1.SendMessage(payload); err != nil {
			t.Fatalf("producer send %d: %v", i, err)
		}
	}
	<-recvDone
	if received != wantMsgs {
		t.Fatalf("received=%d, want %d", received, wantMsgs)
	}
	close(stopReaders)
	wg.Wait()
	final := s2.SnapshotStats()
	t.Logf("concurrent stats: produced=%d MessagesDelivered=%d FramesDecoded=%d; %d concurrent snapshot readers, no race",
		total, final.MessagesDelivered, final.FramesDecoded, readers)
}
