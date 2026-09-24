package session

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/embedded-frame-protocol-go/pkg/frame"
	"github.com/h3yow/embedded-frame-protocol-go/pkg/reassembly"
)

const testSessionID uint64 = 777

func testCfg(dial func() (Transport, error)) Config {
	return Config{
		SessionID: testSessionID,
		Frame:     frame.Config{MaxPayload: 4096},
		Assembly: reassembly.Config{
			FragmentSize: 4,
			MaxTotal:     16,
			MaxPending:   8,
			MaxMessage:   64,
			FragmentTTL:  300 * time.Millisecond,
		},
		Dial: dial,
	}
}

// waitEvent pulls an event matching pred within the deadline.
func waitEvent(t *testing.T, s *Session, pred func(Event) bool) Event {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case ev := <-s.Events():
			if pred(ev) {
				return ev
			}
		case <-deadline:
			t.Fatalf("timed out waiting for event")
		}
	}
}

func drainEvents(s *Session) {
	for {
		select {
		case <-s.Events():
		default:
			return
		}
	}
}

// TestEndToEnd establishes two sessions over a pipe pair and exchanges
// single and fragmented messages.
func TestEndToEnd(t *testing.T) {
	ca, cb := sessionPipes()
	a, err := New(ca, testCfg(nil))
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(cb, testCfg(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	defer b.Close()

	payloads := []string{"tiny", "", "this message is definitely larger than four bytes"}
	for i, p := range payloads {
		seq, err := a.Send([]byte(p))
		if err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
		t.Logf("input: a.Send(seq=%d, %q)", seq, p)
		m, err := b.Recv()
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		if m.Sequence != seq || string(m.Data) != p {
			t.Fatalf("judgement: got seq=%d data=%q, want seq=%d %q", m.Sequence, m.Data, seq, p)
		}
		t.Logf("judgement: b delivered seq=%d %q exactly once", m.Sequence, m.Data)
	}
}

// TestHalfPacketDelivery splits every frame byte stream into 1-byte writes by
// wrapping the transport, proving half-packet reassembly end to end.
func TestHalfPacketDelivery(t *testing.T) {
	ca, cb := sessionPipes()
	slow := &slowTransport{inner: cb, delay: time.Millisecond}
	a, _ := New(ca, testCfg(nil))
	b, _ := New(slow, testCfg(nil))
	defer a.Close()
	defer b.Close()

	payload := "fragmented across many tiny reads!!"
	seq, err := a.Send([]byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input: %d-byte message released 1 byte at a time", len(payload))
	m, err := b.Recv()
	if err != nil || string(m.Data) != payload {
		t.Fatalf("judgement: got %q err=%v, want %q", m.Data, err, payload)
	}
	t.Logf("judgement: half-packet stream reconstructed into seq=%d (%d bytes)", m.Sequence, len(m.Data))
	_ = seq
}

type slowTransport struct {
	inner Transport
	delay time.Duration
}

func (s *slowTransport) Read(p []byte) (int, error) {
	// Only ever yield one byte at a time.
	var one [1]byte
	n, err := s.inner.Read(one[:])
	if n > 0 {
		p[0] = one[0]
	}
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	return n, err
}
func (s *slowTransport) Write(p []byte) (int, error) { return s.inner.Write(p) }
func (s *slowTransport) Close() error                { return s.inner.Close() }

// TestCorruptFrameSkipped injects noise + a checksum-bad frame before a good
// frame; the session must report a distinguishable reject and recover.
func TestCorruptFrameSkipped(t *testing.T) {
	ca, cb := sessionPipes()
	a, _ := New(ca, testCfg(nil))
	b, _ := New(cb, testCfg(nil))
	defer a.Close()
	defer b.Close()

	// Craft a good frame then corrupt a payload byte.
	raw, _ := frame.Encode(&frame.Frame{
		Version: frame.Version1, Session: testSessionID, Sequence: 0,
		Payload: reassembly.EncodeFragment(&reassembly.Fragment{
			Sequence: 0, Total: 1, Index: 0,
			Flags: reassembly.FlagFirst | reassembly.FlagLast, Data: []byte("bad"),
		}),
	})
	corrupt := append([]byte(nil), raw...)
	corrupt[frame.HeaderLength] ^= 0xFF
	cb.writeRaw([]byte{0x01, 0x02, 0x03}) // noise injected into B's reader
	cb.writeRaw(corrupt)                  // corrupted frame towards B

	ev1 := waitEvent(t, b, func(e Event) bool { return e.Type == EventFrameRejected })
	t.Logf("judgement: noise/corruption produced reject: %v", ev1.Detail)
	if !errors.Is(ev1.Detail, frame.ErrSyncLost) && !errors.Is(ev1.Detail, frame.ErrChecksum) {
		t.Fatalf("unexpected reject kind: %v", ev1.Detail)
	}

	// A real message must still arrive after resync.
	payload := "recovered"
	seq, _ := a.Send([]byte(payload))
	m, err := b.Recv()
	if err != nil || string(m.Data) != payload || m.Sequence != seq {
		t.Fatalf("post-corruption delivery failed: %+v err=%v", m, err)
	}
	t.Logf("judgement: decoder resynchronized and delivered seq=%d %q", seq, payload)
}

// TestOutOfOrderAtSessionLevel sends fragments to b in scrambled order using
// a raw injector transport on a's side.
func TestOutOfOrderAtSessionLevel(t *testing.T) {
	cb := newInjectTransport()
	cfg := testCfg(nil)
	b, _ := New(cb, cfg)
	defer b.Close()

	sp := reassembly.NewSplitter(cfg.Assembly)
	frags, _ := sp.Split(testSessionID, 0, []byte("abcdefgh")) // 2 fragments
	raw := func(i int) []byte {
		r, _ := frame.Encode(&frame.Frame{
			Version: frame.Version1, Session: testSessionID, Sequence: 0,
			Payload: reassembly.EncodeFragment(&frags[i]),
		})
		return r
	}
	cb.inject(raw(1)) // last fragment first
	cb.inject(raw(0)) // then first
	t.Logf("input: fragments of seq=0 delivered in order [1,0]")

	m, err := b.Recv()
	if err != nil || string(m.Data) != "abcdefgh" {
		t.Fatalf("judgement: got %q err=%v", m, err)
	}
	t.Logf("judgement: reassembled out-of-order fragments into %q", m.Data)
}

// TestReplayDedup delivers the same full message twice; Recv returns it once.
func TestReplayDedup(t *testing.T) {
	cb := newInjectTransport()
	b, _ := New(cb, testCfg(nil))
	defer b.Close()

	raw, _ := frame.Encode(&frame.Frame{
		Version: frame.Version1, Session: testSessionID, Sequence: 0,
		Payload: reassembly.EncodeFragment(&reassembly.Fragment{
			Sequence: 0, Total: 1, Index: 0,
			Flags: reassembly.FlagFirst | reassembly.FlagLast, Data: []byte("once"),
		}),
	})
	cb.inject(append([]byte(nil), raw...))
	m, err := b.Recv()
	if err != nil || string(m.Data) != "once" {
		t.Fatalf("first delivery: %v %v", m, err)
	}
	t.Logf("input: identical frame seq=0 injected a second time (simulated replay)")
	cb.inject(append([]byte(nil), raw...))

	// The next message seq=1 must come through; the duplicate must not.
	raw2, _ := frame.Encode(&frame.Frame{
		Version: frame.Version1, Session: testSessionID, Sequence: 1,
		Payload: reassembly.EncodeFragment(&reassembly.Fragment{
			Sequence: 1, Total: 1, Index: 0,
			Flags: reassembly.FlagFirst | reassembly.FlagLast, Data: []byte("twice-msg"),
		}),
	})
	cb.inject(raw2)
	m2, err := b.Recv()
	if err != nil || string(m2.Data) != "twice-msg" {
		t.Fatalf("judgement: expected seq=1 message, got %q err=%v", m2, err)
	}
	t.Logf("judgement: duplicate seq=0 suppressed; Recv advanced to seq=1 %q", m2.Data)
}

// TestForeignSessionRejected injects a frame with another session id.
func TestForeignSessionRejected(t *testing.T) {
	cb := newInjectTransport()
	b, _ := New(cb, testCfg(nil))
	defer b.Close()

	raw, _ := frame.Encode(&frame.Frame{
		Version: frame.Version1, Session: 999, Sequence: 0,
		Payload: []byte{0xFF},
	})
	cb.inject(raw)
	ev := waitEvent(t, b, func(e Event) bool {
		return e.Type == EventFrameRejected && errors.Is(e.Detail, ErrBadSessionID)
	})
	t.Logf("judgement: foreign-session frame rejected distinctly: %v", ev.Detail)

	// Own session still works.
	cb.inject(mustSingleFrame(t, 0, []byte("mine")))
	m, err := b.Recv()
	if err != nil || string(m.Data) != "mine" {
		t.Fatalf("delivery after foreign frame failed: %v %v", m, err)
	}
}

// TestReconnectReplay breaks the pipe, reconnects with a fresh pipe pair,
// replays an already-delivered message and confirms suppression + continued
// forward delivery.
func TestReconnectReplay(t *testing.T) {
	ca, cb := sessionPipes()
	var nextA, nextB *memPipe
	dialB := func() (Transport, error) {
		return nextB, nil
	}
	cfg := testCfg(nil)
	cfg.Dial = dialB

	a, _ := New(ca, testCfg(nil))
	b, _ := New(cb, cfg)
	defer a.Close()
	defer b.Close()

	seq0, _ := a.Send([]byte("first"))
	m, _ := b.Recv()
	t.Logf("input: delivered seq=%d %q before break", m.Sequence, m.Data)
	if m.Sequence != seq0 {
		t.Fatalf("seq mismatch")
	}

	// Break the link: simulate by closing both sides and swapping pipes.
	nextA, nextB = newPipePair()
	_ = ca.Close()
	_ = cb.Close()

	// b sees read error; auto-reconnect dials nextB. Wait for the event.
	ev := waitEvent(t, b, func(e Event) bool { return e.Type == EventReconnected })
	t.Logf("judgement: reader observed failure and reconnected: %v detail=%v", ev.Type, ev.Detail)

	// Repoint sender a onto nextA manually (a has no dialer).
	a.mu.Lock()
	a.transport = nextA
	a.cond.Broadcast()
	a.mu.Unlock()

	// Peer replays the old message seq=0 after reconnect (duplicate), then
	// sends a genuinely new message. Send assigns seq=1 monotonically.
	seq1, err := a.Send([]byte("second"))
	if err != nil {
		t.Fatalf("send after reconnect: %v", err)
	}
	t.Logf("input: sender seq counter continues across reconnect -> seq=%d", seq1)

	// Inject a replay of seq=0 directly from the "old peer" side into nextB.
	nextB.peer.writeRaw(replayFrameFor(testSessionID, 0, []byte("first")))
	m, err = b.Recv()
	if err != nil || string(m.Data) != "second" || m.Sequence != 1 {
		t.Fatalf("judgement: expected new seq=1, got %q seq=%d err=%v", m.Data, m.Sequence, err)
	}
	t.Logf("judgement: replayed seq=0 not re-delivered; new seq=%d %q delivered", m.Sequence, m.Data)
}

// TestReconnectAbortsPartial verifies incomplete messages are reported when
// the transport is replaced.
func TestReconnectAbortsPartial(t *testing.T) {
	cb := newInjectTransport()
	var replacement *injectTransport
	cfg := testCfg(func() (Transport, error) { return replacement, nil })
	b, _ := New(cb, cfg)
	defer b.Close()

	// One fragment of a 2-fragment message.
	sp := reassembly.NewSplitter(cfg.Assembly)
	frags, _ := sp.Split(testSessionID, 0, []byte("abcdefgh"))
	r, _ := frame.Encode(&frame.Frame{
		Version: frame.Version1, Session: testSessionID, Sequence: 0,
		Payload: reassembly.EncodeFragment(&frags[0]),
	})
	cb.inject(r)
	time.Sleep(50 * time.Millisecond) // let it get assembled into pending

	replacement = newInjectTransport()
	if err := b.Reconnect(); err != nil {
		t.Fatal(err)
	}
	ev := waitEvent(t, b, func(e Event) bool {
		return e.Type == EventMessageTimeout && e.Seq == 0
	})
	t.Logf("judgement: partial seq=0 abandoned on reconnect: %v", ev.Detail)

	// Fresh transport still delivers new messages.
	replacement.inject(mustSingleFrame(t, 1, []byte("new")))
	m, err := b.Recv()
	if err != nil || string(m.Data) != "new" {
		t.Fatalf("post-reconnect delivery failed: %v %v", m, err)
	}
}

// TestConcurrentSends hammers Send from many goroutines; assigned sequence
// numbers must be unique and contiguous, with no race reported.
func TestConcurrentSends(t *testing.T) {
	ca, cb := sessionPipes()
	a, _ := New(ca, testCfg(nil))
	b, _ := New(cb, testCfg(nil))
	defer a.Close()
	defer b.Close()

	const n = 40
	var wg sync.WaitGroup
	seqs := make(chan uint32, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data := []byte{byte(i)}
			seq, err := a.Send(data)
			if err != nil {
				t.Errorf("send: %v", err)
				return
			}
			seqs <- seq
		}(i)
	}
	wg.Wait()
	close(seqs)

	got := map[uint32]bool{}
	for seq := range seqs {
		if got[seq] {
			t.Fatalf("duplicate send seq %d", seq)
		}
		got[seq] = true
	}
	if len(got) != n {
		t.Fatalf("expected %d unique seqs, got %d", n, len(got))
	}
	// Drain all messages; count must equal n.
	for i := 0; i < n; i++ {
		m, err := b.Recv()
		if err != nil {
			t.Fatalf("recv %d: %v", i, err)
		}
		if !got[m.Sequence] {
			t.Fatalf("unexpected seq %d", m.Sequence)
		}
		delete(got, m.Sequence)
	}
	t.Logf("judgement: %d concurrent sends produced unique seqs, all delivered once", n)
}

// TestPartialMessageTimeout waits for the sweep loop to evict a stale partial.
func TestPartialMessageTimeout(t *testing.T) {
	cb := newInjectTransport()
	cfg := testCfg(nil)
	b, _ := New(cb, cfg)
	defer b.Close()

	sp := reassembly.NewSplitter(cfg.Assembly)
	frags, _ := sp.Split(testSessionID, 3, []byte("abcdefgh"))
	r, _ := frame.Encode(&frame.Frame{
		Version: frame.Version1, Session: testSessionID, Sequence: 3,
		Payload: reassembly.EncodeFragment(&frags[0]),
	})
	cb.inject(r)
	ev := waitEvent(t, b, func(e Event) bool {
		return e.Type == EventMessageTimeout && e.Seq == 3
	})
	t.Logf("judgement: incomplete seq=3 evicted after TTL: %v; pending bounded", ev.Detail)
}

// injectTransport is a read-only transport fed manually.
type injectTransport struct {
	mu     sync.Mutex
	cond   *sync.Cond
	buf    [][]byte
	closed bool
}

func newInjectTransport() *injectTransport {
	it := &injectTransport{}
	it.cond = sync.NewCond(&it.mu)
	return it
}

func (t2 *injectTransport) inject(b []byte) {
	t2.mu.Lock()
	t2.buf = append(t2.buf, append([]byte(nil), b...))
	t2.cond.Broadcast()
	t2.mu.Unlock()
}

func (t2 *injectTransport) Read(p []byte) (int, error) {
	t2.mu.Lock()
	for len(t2.buf) == 0 && !t2.closed {
		t2.cond.Wait()
	}
	if len(t2.buf) == 0 {
		t2.mu.Unlock()
		return 0, errClosedPipe
	}
	n := copy(p, t2.buf[0])
	if n == len(t2.buf[0]) {
		t2.buf = t2.buf[1:]
	} else {
		t2.buf[0] = t2.buf[0][n:]
	}
	t2.mu.Unlock()
	return n, nil
}

func (t2 *injectTransport) Write([]byte) (int, error) { return 0, errClosedPipe }
func (t2 *injectTransport) Close() error {
	t2.mu.Lock()
	t2.closed = true
	t2.cond.Broadcast()
	t2.mu.Unlock()
	return nil
}

func mustSingleFrame(t *testing.T, seq uint32, data []byte) []byte {
	t.Helper()
	raw, err := frame.Encode(&frame.Frame{
		Version: frame.Version1, Session: testSessionID, Sequence: seq,
		Payload: reassembly.EncodeFragment(&reassembly.Fragment{
			Sequence: seq, Total: 1, Index: 0,
			Flags: reassembly.FlagFirst | reassembly.FlagLast, Data: data,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func replayFrameFor(session uint64, seq uint32, data []byte) []byte {
	raw, _ := frame.Encode(&frame.Frame{
		Version: frame.Version1, Session: session, Sequence: seq,
		Payload: reassembly.EncodeFragment(&reassembly.Fragment{
			Sequence: seq, Total: 1, Index: 0,
			Flags: reassembly.FlagFirst | reassembly.FlagLast, Data: data,
		}),
	})
	return raw
}
