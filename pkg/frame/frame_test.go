package frame

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func mustEncode(t *testing.T, f *Frame) []byte {
	t.Helper()
	b, err := Encode(f)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	return b
}

// TestRoundTrip checks basic encode -> parse and field preservation.
func TestRoundTrip(t *testing.T) {
	in := &Frame{Session: 42, Sequence: 7, Payload: []byte("hello frame")}
	raw := mustEncode(t, in)
	t.Logf("input: session=%d seq=%d payload=%q -> wire=%x", in.Session, in.Sequence, in.Payload, raw)
	if len(raw) != TotalSize(len(in.Payload)) {
		t.Fatalf("wire size = %d, want %d", len(raw), TotalSize(len(in.Payload)))
	}
	out, err := ParseFrame(raw)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if out.Session != 42 || out.Sequence != 7 || !bytes.Equal(out.Payload, in.Payload) {
		t.Fatalf("decoded %+v does not match input", out)
	}
	t.Logf("judgement: fields preserved, checksum verified over %d header/payload bytes",
		HeaderLength+len(in.Payload))
}

// TestFeedHalfFrames feeds one byte at a time; the frame may only appear once
// the final checksum byte arrives.
func TestFeedHalfFrames(t *testing.T) {
	raw := mustEncode(t, &Frame{Session: 1, Sequence: 100, Payload: []byte("XY")})
	dec := NewDecoder(Config{})
	var got *Frame
	for i, b := range raw {
		res := dec.Feed([]byte{b})
		for _, r := range res {
			if r.Err != nil {
				t.Fatalf("unexpected error at byte %d: %v", i, r.Err)
			}
			got = r.Frame
		}
		if i < len(raw)-1 && got != nil {
			t.Fatalf("frame decoded prematurely after %d bytes", i+1)
		}
	}
	if got == nil {
		t.Fatalf("frame never decoded after %d one-byte feeds", len(raw))
	}
	t.Logf("judgement: byte-at-a-time feeds yielded exactly one frame seq=%d", got.Sequence)
}

// TestFeedNoiseAndMultipleFrames verifies resynchronization: garbage prefix,
// a corrupted frame (bad checksum), and trailing good frames.
func TestFeedNoiseAndMultipleFrames(t *testing.T) {
	f1 := mustEncode(t, &Frame{Session: 9, Sequence: 1, Payload: []byte("one")})
	f2 := mustEncode(t, &Frame{Session: 9, Sequence: 2, Payload: []byte("two")})

	// Corrupt f1's payload byte.
	bad := append([]byte(nil), f1...)
	bad[HeaderLength] ^= 0xFF

	stream := concat(
		[]byte{0x00, 0x11, Magic0}, // noise ending with lone magic byte
		[]byte{0x22},               // breaks the magic
		f1,                         // good
		[]byte{0xDE, 0xAD},         // noise between frames
		bad,                        // checksum failure
		f2,                         // good: decoder must resync
	)

	dec := NewDecoder(Config{})
	results := dec.Feed(stream)

	var syncs, checksums, good int
	var seqs []uint32
	for _, r := range results {
		switch {
		case r.Frame != nil:
			good++
			seqs = append(seqs, r.Frame.Sequence)
		case errors.Is(r.Err, ErrSyncLost):
			syncs++
			t.Logf("resync event: %v", r.Err)
		case errors.Is(r.Err, ErrChecksum):
			checksums++
			t.Logf("checksum rejection: %v", r.Err)
		default:
			t.Fatalf("unexpected result: %v", r.Err)
		}
	}
	t.Logf("input contains: noise prefix + 1 good + noise + 1 corrupt + 1 good; "+
		"judgement: good=%d seqs=%v syncEvents=%d checksumRejects=%d",
		good, seqs, syncs, checksums)
	if good != 2 || seqs[0] != 1 || seqs[1] != 2 {
		t.Fatalf("expected exactly frames 1 and 2, got %v", seqs)
	}
	if checksums != 1 {
		t.Fatalf("expected 1 checksum rejection, got %d", checksums)
	}
	if syncs < 2 {
		t.Fatalf("expected resync events for noise prefix and mid-stream noise, got %d", syncs)
	}
}

// TestIllegalLengthAndVersion verify distinct error classification.
func TestIllegalLengthAndVersion(t *testing.T) {
	// Length beyond configured limit.
	long := make([]byte, TotalSize(10))
	long[0], long[1] = Magic0, Magic1
	long[2] = Version1
	binary.LittleEndian.PutUint16(long[4:6], 10)
	binary.LittleEndian.PutUint32(long[HeaderLength+10:], Checksum(long[:HeaderLength+10]))
	dec := NewDecoder(Config{MaxPayload: 4})
	res := dec.Feed(long)
	if len(res) == 0 || !errors.Is(res[0].Err, ErrIllegalLength) {
		t.Fatalf("expected first result ErrIllegalLength, got %+v", res)
	}
	t.Logf("judgement: length 10 > limit 4 rejected as ErrIllegalLength: %v", res[0].Err)
	for _, r := range res[1:] {
		// Remaining bytes are frame-less noise; resync events are expected.
		if r.Frame != nil || !errors.Is(r.Err, ErrSyncLost) {
			t.Fatalf("only resync events may follow, got %+v", r)
		}
	}

	// Unsupported version, length valid.
	raw := mustEncode(t, &Frame{Session: 1, Sequence: 1, Payload: []byte("v")})
	raw[2] = 99
	dec2 := NewDecoder(Config{})
	res2 := dec2.Feed(raw)
	if len(res2) != 1 || !errors.Is(res2[0].Err, ErrUnsupportedVersion) {
		t.Fatalf("expected ErrUnsupportedVersion, got %+v", res2)
	}
	t.Logf("judgement: version 99 rejected as ErrUnsupportedVersion: %v", res2[0].Err)
}

// TestCorruptionRecovery ensures decoding continues after a corrupted length
// field (unknown boundary) by resynchronizing at the next magic.
func TestCorruptionRecovery(t *testing.T) {
	good1 := mustEncode(t, &Frame{Session: 5, Sequence: 1, Payload: []byte{1}})
	good2 := mustEncode(t, &Frame{Session: 5, Sequence: 2, Payload: []byte{2}})

	bogus := make([]byte, HeaderLength+1+ChecksumLength)
	bogus[0], bogus[1] = Magic0, Magic1
	bogus[2] = Version1
	binary.LittleEndian.PutUint16(bogus[4:6], 60000) // huge length: boundary unknowable

	dec := NewDecoder(Config{MaxPayload: 4096})
	out := dec.Feed(concat(good1, bogus, good2))
	var seqs []uint32
	var illegal, sync int
	for _, r := range out {
		switch {
		case r.Frame != nil:
			seqs = append(seqs, r.Frame.Sequence)
		case errors.Is(r.Err, ErrIllegalLength):
			illegal++
		case errors.Is(r.Err, ErrSyncLost):
			sync++
		}
	}
	t.Logf("judgement: good1 + bogus-length + good2 -> seqs=%v illegalReject=%d resync=%d",
		seqs, illegal, sync)
	if len(seqs) != 2 || seqs[0] != 1 || seqs[1] != 2 {
		t.Fatalf("expected recovery to frames [1 2], got %v", seqs)
	}
	if illegal != 1 {
		t.Fatalf("expected one illegal-length reject, got %d", illegal)
	}
}

// TestStreamReader checks the io.Reader wrapper drains queued frames after
// the reader returns EOF.
func TestStreamReader(t *testing.T) {
	raw := concat(
		[]byte("garbage"),
		mustEncode(t, &Frame{Session: 3, Sequence: 55, Payload: []byte("a")}),
		mustEncode(t, &Frame{Session: 3, Sequence: 56, Payload: []byte("bb")}),
	)
	sr := NewStreamReader(bytes.NewReader(raw), Config{})
	f1, err := sr.ReadFrame()
	if err != nil || f1.Sequence != 55 {
		t.Fatalf("frame1 = %v err=%v", f1, err)
	}
	f2, err := sr.ReadFrame()
	if err != nil || f2.Sequence != 56 {
		t.Fatalf("frame2 = %v err=%v", f2, err)
	}
	if e := sr.NextError(); !errors.Is(e, ErrSyncLost) {
		t.Fatalf("expected queued sync event, got %v", e)
	}
	_, err = sr.ReadFrame()
	if err == nil {
		t.Fatal("expected EOF after draining")
	}
	t.Logf("judgement: stream reader delivered 55,56 despite garbage prefix; queued sync event consumed")
}

func concat(parts ...[]byte) []byte {
	var n int
	for _, p := range parts {
		n += len(p)
	}
	out := make([]byte, 0, n)
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}
