package efp

import (
	"errors"
	"testing"
)

func TestFrameEncodeDecodeRoundTrip(t *testing.T) {
	in := &Frame{
		Flags:   FlagFragment,
		Seq:     42,
		MsgID:   7,
		FragIdx: 1,
		FragCnt: 3,
		Payload: []byte("hello embedded world"),
	}
	buf, err := in.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	t.Logf("input frame: %+v, encoded %d bytes", in, len(buf))
	out, err := DecodeFrame(buf)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	if out.Seq != in.Seq || out.MsgID != in.MsgID || out.FragIdx != in.FragIdx ||
		out.FragCnt != in.FragCnt || out.Flags != in.Flags ||
		string(out.Payload) != string(in.Payload) {
		t.Fatalf("round trip mismatch: got %+v want %+v", out, in)
	}
	if !out.Fragmented() {
		t.Fatal("expected Fragmented() == true")
	}
	t.Logf("round trip ok: seq=%d msgID=%d frag=%d/%d payload=%q",
		out.Seq, out.MsgID, out.FragIdx, out.FragCnt, out.Payload)
}

func TestDecodeFrameErrorKinds(t *testing.T) {
	good := &Frame{Seq: 1, MsgID: 1, FragCnt: 1, Payload: []byte("abc")}
	buf, err := good.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	cases := []struct {
		name   string
		mutate func([]byte)
		want   error
	}{
		{"bad magic", func(b []byte) { b[0] = 0x00 }, ErrBadMagic},
		{"unsupported version", func(b []byte) { b[2] = 99 }, ErrUnsupportedVersion},
		{"crc mismatch", func(b []byte) { b[len(b)-1] ^= 0xFF }, ErrCRCMismatch},
		{"bad length field", func(b []byte) { b[16] = 0xFF; b[17] = 0xFF }, ErrBadLength},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := make([]byte, len(buf))
			copy(c, buf)
			tc.mutate(c)
			_, err := DecodeFrame(c)
			if err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("error %v is not classified as %v", err, tc.want)
			}
			var fe *FrameError
			if !errors.As(err, &fe) {
				t.Fatalf("error is not a *FrameError: %T", err)
			}
			t.Logf("case %q: classified error kind=%q detail=%q", tc.name, fe.Kind, fe.Detail)
		})
	}
}

func TestEncodeRejectsOversizePayload(t *testing.T) {
	f := &Frame{FragCnt: 1, Payload: make([]byte, MaxPayload+1)}
	_, err := f.Encode()
	if !errors.Is(err, ErrBadLength) {
		t.Fatalf("expected ErrBadLength, got %v", err)
	}
	t.Logf("oversize payload (%d bytes) rejected with: %v", MaxPayload+1, err)
}
