package efp

import (
	"bytes"
	"errors"
	"testing"
)

func mustEncode(t *testing.T, f *Frame) []byte {
	t.Helper()
	b, err := f.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return b
}

func TestDecoderHalfPacket(t *testing.T) {
	d := NewDecoder()
	f := &Frame{Seq: 1, MsgID: 1, FragCnt: 1, Payload: []byte("half-packet-payload")}
	buf := mustEncode(t, f)

	// Feed the frame one byte at a time: nothing may be decoded until the
	// final byte arrives.
	for i := 0; i < len(buf)-1; i++ {
		frames, errs := d.Feed(buf[i : i+1])
		if len(frames) != 0 || len(errs) != 0 {
			t.Fatalf("byte %d: unexpected frames=%d errs=%v", i, len(frames), errs)
		}
	}
	frames, errs := d.Feed(buf[len(buf)-1:])
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if len(frames) != 1 || string(frames[0].Payload) != string(f.Payload) {
		t.Fatalf("got %d frames, want 1 with payload %q", len(frames), f.Payload)
	}
	t.Logf("half-packet: fed %d bytes one at a time, decoded exactly after last byte", len(buf))
}

func TestDecoderNoiseThenFrame(t *testing.T) {
	d := NewDecoder()
	noise := []byte{0x00, 0x11, 0x22, 0x33}
	f := &Frame{Seq: 2, MsgID: 5, FragCnt: 1, Payload: []byte("after noise")}
	stream := append(noise, mustEncode(t, f)...)

	frames, errs := d.Feed(stream)
	if len(frames) != 1 {
		t.Fatalf("got %d frames, want 1", len(frames))
	}
	if !errors.Is(errs[0], ErrBadMagic) {
		t.Fatalf("expected ErrBadMagic for noise, got %v", errs)
	}
	if d.BytesDropped != len(noise) {
		t.Fatalf("BytesDropped=%d, want %d", d.BytesDropped, len(noise))
	}
	t.Logf("noise=%v dropped=%d, then decoded frame payload=%q", noise, d.BytesDropped, frames[0].Payload)
}

func TestDecoderCorruptFrameResync(t *testing.T) {
	d := NewDecoder()
	good1 := mustEncode(t, &Frame{Seq: 1, MsgID: 1, FragCnt: 1, Payload: []byte("first")})
	corrupt := mustEncode(t, &Frame{Seq: 2, MsgID: 2, FragCnt: 1, Payload: []byte("broken")})
	corrupt[len(corrupt)-1] ^= 0xFF // break CRC
	good2 := mustEncode(t, &Frame{Seq: 3, MsgID: 3, FragCnt: 1, Payload: []byte("third")})

	stream := bytes.Join([][]byte{good1, corrupt, good2}, nil)
	frames, errs := d.Feed(stream)

	if len(frames) != 2 {
		t.Fatalf("decoded %d frames, want 2 (good1, good2)", len(frames))
	}
	if string(frames[0].Payload) != "first" || string(frames[1].Payload) != "third" {
		t.Fatalf("payloads = %q, %q; want first, third", frames[0].Payload, frames[1].Payload)
	}
	var sawCRC bool
	for _, e := range errs {
		if errors.Is(e, ErrCRCMismatch) {
			sawCRC = true
		}
	}
	if !sawCRC {
		t.Fatalf("expected a CRC error, got %v", errs)
	}
	t.Logf("corrupt frame rejected (%v), decoder resynced and decoded %q and %q",
		errs[0], frames[0].Payload, frames[1].Payload)
}

func TestDecoderUnsupportedVersionResync(t *testing.T) {
	d := NewDecoder()
	bad := mustEncode(t, &Frame{Seq: 1, MsgID: 1, FragCnt: 1, Payload: []byte("v99")})
	bad[2] = 99 // unsupported version (CRC now invalid too, version checked first)
	good := mustEncode(t, &Frame{Seq: 2, MsgID: 2, FragCnt: 1, Payload: []byte("ok")})

	frames, errs := d.Feed(append(bad, good...))
	if len(frames) != 1 || string(frames[0].Payload) != "ok" {
		t.Fatalf("frames=%v, want one frame with payload ok", frames)
	}
	if !errors.Is(errs[0], ErrUnsupportedVersion) {
		t.Fatalf("expected ErrUnsupportedVersion, got %v", errs)
	}
	t.Logf("unsupported version rejected: %v; subsequent frame decoded", errs[0])
}

func TestDecoderGarbageOnlyStream(t *testing.T) {
	d := NewDecoder()
	garbage := bytes.Repeat([]byte{0x01, 0x02, 0x03, 0x04}, 6) // 24 bytes > HeaderLen
	frames, errs := d.Feed(garbage)
	if len(frames) != 0 {
		t.Fatalf("decoded %d frames from garbage", len(frames))
	}
	if len(errs) == 0 {
		t.Fatal("expected an error for garbage input")
	}
	// Decoder must still work afterwards.
	good := mustEncode(t, &Frame{Seq: 9, MsgID: 9, FragCnt: 1, Payload: []byte("recovered")})
	frames, _ = d.Feed(good)
	if len(frames) != 1 {
		t.Fatalf("decoder did not recover after garbage, frames=%d", len(frames))
	}
	t.Logf("garbage-only input: %v; decoder recovered with payload=%q", errs[0], frames[0].Payload)
}

// TestDecoderCapsVersionControlFrame: a control frame stamped with
// capsVersion is accepted (so capability negotiation works), while a
// non-control frame at capsVersion is rejected like any unsupported
// version — and a legacy receiver's view (Version only) never sees the
// hello as data.
func TestDecoderCapsVersionControlFrame(t *testing.T) {
	d := NewDecoder()
	hello := mustEncode(t, &Frame{
		Version: capsVersion, Flags: FlagControl, Seq: 0, FragCnt: 1,
		Payload: []byte{1, 0, 0, 1, 0, 0, 8},
	})
	data := mustEncode(t, &Frame{Seq: 1, MsgID: 42, FragCnt: 1, Payload: []byte("v1 data")})

	frames, errs := d.Feed(append(hello, data...))
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if len(frames) != 2 {
		t.Fatalf("decoded %d frames, want 2 (hello + data)", len(frames))
	}
	if frames[0].Version != capsVersion || frames[0].Flags&FlagControl == 0 {
		t.Fatalf("frame 0 = version %d flags %#x, want capsVersion control", frames[0].Version, frames[0].Flags)
	}
	if frames[1].Version != Version || string(frames[1].Payload) != "v1 data" {
		t.Fatalf("frame 1 = version %d payload %q, want v1 data", frames[1].Version, frames[1].Payload)
	}
	t.Logf("capsVersion control frame accepted alongside v1 data: %+v / %q",
		frames[0].Payload, frames[1].Payload)

	// A capsVersion frame WITHOUT the control flag is not decodable.
	d2 := NewDecoder()
	bogus := mustEncode(t, &Frame{Seq: 2, MsgID: 43, FragCnt: 1, Payload: []byte("v2 data?")})
	bogus[2] = capsVersion // re-stamp a plain data frame; CRC stays valid
	frames, errs = d2.Feed(bogus)
	if len(frames) != 0 {
		t.Fatalf("capsVersion data frame decoded: %+v", frames)
	}
	if len(errs) == 0 || !errors.Is(errs[0], ErrUnsupportedVersion) {
		t.Fatalf("capsVersion data frame: got errs=%v, want ErrUnsupportedVersion", errs)
	}
	t.Logf("capsVersion non-control frame rejected as unsupported version: %v", errs[0])
}
