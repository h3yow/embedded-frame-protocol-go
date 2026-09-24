package efp

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestFragmentReassembleRoundTrip(t *testing.T) {
	payload := bytes.Repeat([]byte("ABCDEFGH"), 400) // 3200 bytes -> 4 fragments
	frames, nextSeq, err := Fragment(11, payload, 100)
	if err != nil {
		t.Fatalf("Fragment: %v", err)
	}
	t.Logf("payload=%d bytes -> %d fragments, seqs [%d..%d)",
		len(payload), len(frames), 100, nextSeq)

	r := NewReassembler(DefaultReassemblyConfig())
	var got *ReassembledMessage
	for _, f := range frames {
		msg, err := r.Push(f)
		if err != nil {
			t.Fatalf("Push: %v", err)
		}
		if msg != nil {
			got = msg
		}
	}
	if got == nil {
		t.Fatal("message not delivered after all fragments")
	}
	if got.MsgID != 11 || !bytes.Equal(got.Data, payload) {
		t.Fatalf("reassembled mismatch: msgID=%d len=%d", got.MsgID, len(got.Data))
	}
	t.Logf("reassembled msgID=%d, %d bytes, in-flight after completion=%d",
		got.MsgID, len(got.Data), r.InFlight())
}

func TestReassemblyOutOfOrder(t *testing.T) {
	payload := bytes.Repeat([]byte("xyz"), 1000) // 3000 bytes -> 3 fragments
	frames, _, err := Fragment(21, payload, 0)
	if err != nil {
		t.Fatalf("Fragment: %v", err)
	}
	// Deliver in reverse order.
	r := NewReassembler(DefaultReassemblyConfig())
	var got *ReassembledMessage
	for i := len(frames) - 1; i >= 0; i-- {
		msg, err := r.Push(frames[i])
		if err != nil {
			t.Fatalf("Push frag %d: %v", i, err)
		}
		if i > 0 && msg != nil {
			t.Fatalf("delivered early at frag %d: incomplete message must not be delivered", i)
		}
		got = msg
	}
	if got == nil || !bytes.Equal(got.Data, payload) {
		t.Fatalf("out-of-order reassembly failed: got nil=%v", got == nil)
	}
	t.Logf("out-of-order: %d fragments delivered in reverse, reassembled %d bytes correctly",
		len(frames), len(got.Data))
}

func TestReassemblyDuplicateFragment(t *testing.T) {
	payload := bytes.Repeat([]byte("dup!"), 600) // 2400 bytes -> 3 fragments
	frames, _, err := Fragment(31, payload, 0)
	if err != nil {
		t.Fatalf("Fragment: %v", err)
	}
	r := NewReassembler(DefaultReassemblyConfig())
	// Push every fragment twice; duplicates must be ignored and the message
	// delivered exactly once.
	var delivered []*ReassembledMessage
	for _, f := range frames {
		for rep := 0; rep < 2; rep++ {
			msg, err := r.Push(f)
			if err != nil {
				t.Fatalf("Push: %v", err)
			}
			if msg != nil {
				delivered = append(delivered, msg)
			}
		}
	}
	if len(delivered) != 1 {
		t.Fatalf("delivered %d times, want exactly 1", len(delivered))
	}
	if !bytes.Equal(delivered[0].Data, payload) {
		t.Fatal("payload mismatch after duplicate fragments")
	}
	t.Logf("duplicate fragments: each of %d fragments pushed twice, delivered exactly once (%d bytes)",
		len(frames), len(delivered[0].Data))
}

func TestReassemblyTimeout(t *testing.T) {
	cfg := DefaultReassemblyConfig()
	cfg.FragmentTimeout = time.Second
	r := NewReassembler(cfg)
	now := time.Unix(1000, 0)
	r.SetClock(func() time.Time { return now })

	frames, _, _ := Fragment(41, bytes.Repeat([]byte("t"), 2000), 0)
	if _, err := r.Push(frames[0]); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if r.InFlight() != 1 {
		t.Fatalf("InFlight=%d, want 1", r.InFlight())
	}
	// Advance the clock past the timeout and expire.
	now = now.Add(2 * time.Second)
	errs := r.Expire()
	if len(errs) != 1 {
		t.Fatalf("Expire returned %d errors, want 1", len(errs))
	}
	var re *ReassemblyError
	if !errors.As(errs[0], &re) || !errors.Is(errs[0], ErrReassemblyTimeout) {
		t.Fatalf("error not classified as timeout: %v", errs[0])
	}
	if re.MsgID != 41 {
		t.Fatalf("expired MsgID=%d, want 41", re.MsgID)
	}
	if r.InFlight() != 0 {
		t.Fatalf("InFlight=%d after expiry, want 0", r.InFlight())
	}
	t.Logf("timeout: message 41 expired with 1/%d fragments, reason=%q", frames[0].FragCnt, errs[0])
}

func TestReassemblyInFlightLimit(t *testing.T) {
	cfg := DefaultReassemblyConfig()
	cfg.MaxInFlight = 2
	r := NewReassembler(cfg)
	mkFrag := func(msgID uint32) *Frame {
		fs, _, _ := Fragment(msgID, bytes.Repeat([]byte("q"), 2000), 0)
		return fs[0]
	}
	for id := uint32(1); id <= 2; id++ {
		if _, err := r.Push(mkFrag(id)); err != nil {
			t.Fatalf("Push msg %d: %v", id, err)
		}
	}
	_, err := r.Push(mkFrag(3))
	if !errors.Is(err, ErrReassemblyOverflow) {
		t.Fatalf("expected ErrReassemblyOverflow, got %v", err)
	}
	t.Logf("in-flight limit: 3rd concurrent message rejected with %q (InFlight=%d)",
		err, r.InFlight())
}

func TestReassemblyInconsistentFragCnt(t *testing.T) {
	r := NewReassembler(DefaultReassemblyConfig())
	f1 := &Frame{Flags: FlagFragment, MsgID: 51, FragIdx: 0, FragCnt: 3, Payload: []byte("a")}
	f2 := &Frame{Flags: FlagFragment, MsgID: 51, FragIdx: 1, FragCnt: 4, Payload: []byte("b")}
	if _, err := r.Push(f1); err != nil {
		t.Fatalf("Push f1: %v", err)
	}
	_, err := r.Push(f2)
	if err == nil {
		t.Fatal("expected error for inconsistent FragCnt")
	}
	if r.InFlight() != 0 {
		t.Fatalf("inconsistent message must be dropped, InFlight=%d", r.InFlight())
	}
	t.Logf("inconsistent FragCnt (3 then 4) rejected deterministically: %v", err)
}

func TestFragmentRejectsHugeMessage(t *testing.T) {
	_, _, err := Fragment(1, make([]byte, int(^uint16(0))*MaxPayload+1), 0)
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("expected ErrMessageTooLarge, got %v", err)
	}
	t.Logf("oversize message rejected by Fragment: %v", err)
}
