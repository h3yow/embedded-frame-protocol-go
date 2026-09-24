package reassembly

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func testConfig() Config {
	return Config{
		FragmentSize: 4,
		MaxTotal:     3,
		MaxPending:   2,
		MaxMessage:   12,
		FragmentTTL:  50 * time.Millisecond,
	}
}

// fragmentsFor splits payload via Splitter, guaranteeing identical metadata.
func fragmentsFor(t *testing.T, seq uint32, payload string) []Fragment {
	t.Helper()
	s := NewSplitter(testConfig())
	fs, err := s.Split(7, seq, []byte(payload))
	if err != nil {
		t.Fatalf("split(%q): %v", payload, err)
	}
	return fs
}

// TestOutOfOrderReassembly feeds fragments in a scrambled order and checks
// that the message is delivered exactly once, correctly assembled.
func TestOutOfOrderReassembly(t *testing.T) {
	a := NewAssembler(testConfig())
	fs := fragmentsFor(t, 100, "abcdefgh") // 2 fragments of 4 bytes
	t.Logf("input: seq=%d payload=%q split into %d fragments (sizes %d,%d)",
		100, "abcdefgh", len(fs), len(fs[0].Data), len(fs[1].Data))

	order := []int{1, 0} // last fragment first
	var completed *Event
	for _, i := range order {
		ev := a.Offer(&fs[i])
		if i == 1 && ev.Type != EventProgress {
			t.Fatalf("fragment 1 first: expected progress, got %s", ev.Type)
		}
		if ev.Type == EventCompleted {
			ev := ev
			completed = &ev
		}
	}
	if completed == nil || !bytes.Equal(completed.Data, []byte("abcdefgh")) {
		t.Fatalf("judgement: expected completed message, got %+v", completed)
	}
	t.Logf("judgement: scrambled order %v reassembled to %q", order, completed.Data)
}

// TestDuplicates feeds identical and conflicting duplicate fragments.
func TestDuplicates(t *testing.T) {
	a := NewAssembler(testConfig())
	fs := fragmentsFor(t, 5, "abcdefgh")

	if ev := a.Offer(&fs[0]); ev.Type != EventProgress {
		t.Fatalf("first offer: expected progress, got %s", ev.Type)
	}
	ev := a.Offer(&fs[0]) // identical replay
	if ev.Type != EventDuplicate {
		t.Fatalf("identical replay: expected duplicate, got %s err=%v", ev.Type, ev.Err)
	}
	t.Logf("judgement: identical fragment replayed -> EventDuplicate (no state change)")

	conflict := fs[0]
	conflict.Data = []byte("zzzz")
	ev = a.Offer(&conflict)
	if ev.Type != EventRejected || !errors.Is(ev.Err, ErrPartialDuplicate) {
		t.Fatalf("conflicting replay: expected rejection ErrPartialDuplicate, got %s %v", ev.Type, ev.Err)
	}
	t.Logf("judgement: same index different data -> EventRejected/ErrPartialDuplicate: %v", ev.Err)

	// Genuine completion still works.
	ev = a.Offer(&fs[1])
	if ev.Type != EventCompleted || !bytes.Equal(ev.Data, []byte("abcdefgh")) {
		t.Fatalf("completion after conflict rejected? got %s %v", ev.Type, ev.Err)
	}
	// Completing a completed message again is impossible (entry deleted);
	// its fragments now start a new assembly whose total stays consistent,
	// but indices replay as duplicates only while the entry exists.
	t.Logf("judgement: message seq=5 delivered exactly once as %q", ev.Data)
}

// TestTimeout verifies incomplete messages are deterministically abandoned.
func TestTimeout(t *testing.T) {
	a := NewAssembler(testConfig())
	fs := fragmentsFor(t, 9, "abcdefgh")
	if ev := a.Offer(&fs[0]); ev.Type != EventProgress {
		t.Fatalf("got %s", ev.Type)
	}
	t.Logf("input: only fragment 0 of seq=9; waiting past TTL=%v", testConfig().FragmentTTL)
	if evs := a.SweepExpired(timeNow().Add(10 * time.Millisecond)); len(evs) != 0 {
		t.Fatalf("swept too early: %+v", evs)
	}
	evs := a.SweepExpired(timeNow().Add(100 * time.Millisecond))
	if len(evs) != 1 || evs[0].Type != EventTimeout || evs[0].Sequence != 9 {
		t.Fatalf("expected one timeout for seq 9, got %+v", evs)
	}
	t.Logf("judgement: %v, pending freed (count=%d)", evs[0].Err, a.PendingCount())
	if a.PendingCount() != 0 {
		t.Fatalf("timed-out message still occupies memory")
	}
}

// TestPendingCap bounds concurrent incomplete messages.
func TestPendingCap(t *testing.T) {
	a := NewAssembler(testConfig()) // MaxPending = 2
	fs := fragmentsFor(t, 1, "abcdefgh")
	a.Offer(&fs[0]) // opens seq 1
	fs2 := fragmentsFor(t, 2, "ijklmnop")
	a.Offer(&fs2[0]) // opens seq 2
	fs3 := fragmentsFor(t, 3, "qrstuvwx")
	ev := a.Offer(&fs3[0]) // would open seq 3
	if ev.Type != EventRejected || !errors.Is(ev.Err, ErrTooManyPending) {
		t.Fatalf("expected ErrTooManyPending, got %s %v", ev.Type, ev.Err)
	}
	t.Logf("judgement: opening 3rd pending message rejected: %v", ev.Err)
}

// TestTotalMismatch rejects fragments that disagree on fragment count.
func TestTotalMismatch(t *testing.T) {
	a := NewAssembler(testConfig())
	fs := fragmentsFor(t, 1, "abcdefgh") // total=2
	a.Offer(&fs[0])
	other := fragmentsFor(t, 1, "abc") // total=1, same seq
	ev := a.Offer(&other[0])
	if ev.Type != EventRejected || !errors.Is(ev.Err, ErrTotalMismatch) {
		t.Fatalf("expected ErrTotalMismatch, got %s %v", ev.Type, ev.Err)
	}
	t.Logf("judgement: total 2 vs 1 for same seq rejected: %v", ev.Err)
}

// TestResetAborts reports every discarded message, never silently.
func TestResetAborts(t *testing.T) {
	a := NewAssembler(testConfig())
	fs1 := fragmentsFor(t, 1, "abcdefgh")
	fs2 := fragmentsFor(t, 2, "ijklmnop")
	a.Offer(&fs1[0])
	a.Offer(&fs2[0])
	evs := a.Reset()
	if len(evs) != 2 {
		t.Fatalf("expected 2 abort events, got %d", len(evs))
	}
	seen := map[uint32]bool{}
	for _, ev := range evs {
		if ev.Type != EventAborted || !errors.Is(ev.Err, ErrMessageIncomplete) {
			t.Fatalf("unexpected event: %+v", ev)
		}
		seen[ev.Sequence] = true
	}
	if !seen[1] || !seen[2] {
		t.Fatalf("missing abort for one of the messages: %v", seen)
	}
	t.Logf("judgement: reset produced explicit abort events for seqs %v", seen)
}

// TestEncodeDecodeRoundTrip covers fragment payload wire format.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	fs := fragmentsFor(t, 21, "hello")
	for _, f := range fs {
		raw := EncodeFragment(&f)
		got, err := DecodeFragment(f.Session, f.Sequence, raw)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.Index != f.Index || got.Total != f.Total || got.Flags != f.Flags ||
			!bytes.Equal(got.Data, f.Data) {
			t.Fatalf("fragment mismatch: %+v vs %+v", got, f)
		}
	}
	t.Logf("judgement: %d fragments survive encode/decode with flags/index/total intact", len(fs))
}

// TestSingleAndEmpty covers 1-byte messages and empty payloads.
func TestSingleAndEmpty(t *testing.T) {
	a := NewAssembler(testConfig())
	for i, in := range []string{"", "a", "abcd"} {
		seq := uint32(50 + i)
		fs := fragmentsFor(t, seq, in)
		for i := range fs {
			ev := a.Offer(&fs[i])
			if ev.Type != EventCompleted {
				t.Fatalf("payload %q: expected single completed event, got %s", in, ev.Type)
			}
			if !bytes.Equal(ev.Data, []byte(in)) {
				t.Fatalf("payload %q reassembled as %q", in, ev.Data)
			}
		}
	}
	t.Logf("judgement: empty, single-byte and exactly-fitting payloads complete as single fragments")
}
