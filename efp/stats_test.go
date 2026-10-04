package efp

import (
	"bytes"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

// TestSessionDuplicateFragmentEventAndStats: a duplicate fragment is
// counted once, reported once as a distinguishable event, and the message
// is still delivered exactly once.
func TestSessionDuplicateFragmentEventAndStats(t *testing.T) {
	s := NewSession(SessionConfig{})
	t.Cleanup(func() { s.Close() })

	payload := bytes.Repeat([]byte("dup"), 800) // 2400 bytes -> 3 fragments
	frames, _, err := Fragment(701, payload, 0)
	if err != nil {
		t.Fatalf("Fragment: %v", err)
	}
	c1a, c1b := net.Pipe()
	drainPeer(t, c1b)
	if err := s.Attach(c1a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	// Fragment 0 is sent twice; the rest once.
	order := [][]byte{mustEncode(t, frames[0]), mustEncode(t, frames[0])}
	for _, f := range frames[1:] {
		order = append(order, mustEncode(t, f))
	}
	t.Logf("input: message 701 in %d fragments, fragment 0 sent twice (%d frames total)",
		len(frames), len(order))
	for _, b := range order {
		if _, err := c1b.Write(b); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	// Expect exactly one duplicate event identifying message 701, frag 0.
	select {
	case ev := <-s.Events():
		t.Logf("event received: %v", &ev)
		if ev.MsgID != 701 || ev.FragIdx != 0 {
			t.Fatalf("event = %+v, want {MsgID:701 FragIdx:0}", ev)
		}
		if !errors.Is(&ev, ErrDuplicateFragment) {
			t.Fatalf("event not distinguishable via errors.Is(ErrDuplicateFragment)")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no duplicate-fragment event reported")
	}

	got := recvN(t, s, 1, 2*time.Second)
	if got[0].MsgID != 701 || !bytes.Equal(got[0].Data, payload) {
		t.Fatalf("delivered mismatch: id=%d len=%d", got[0].MsgID, len(got[0].Data))
	}

	st := s.Stats()
	t.Logf("stats after duplicate: %+v", st)
	if st.DuplicateFragments != 1 {
		t.Fatalf("DuplicateFragments=%d, want 1 (duplicate counts as one)", st.DuplicateFragments)
	}
	select {
	case m := <-s.Delivered():
		t.Fatalf("message delivered twice: %+v", m)
	case <-time.After(150 * time.Millisecond):
		t.Log("verdict: duplicate counted once, event reported once, message delivered exactly once")
	}
}

// TestSessionStatsTimeoutAndInFlight: fragment timeouts and in-flight
// rejections are counted separately and surface as errors.
func TestSessionStatsTimeoutAndInFlight(t *testing.T) {
	cfg := SessionConfig{Reassembly: ReassemblyConfig{
		MaxMessageSize:  64 * 1024,
		MaxInFlight:     1,
		FragmentTimeout: 100 * time.Millisecond,
	}}
	s := NewSession(cfg)
	t.Cleanup(func() { s.Close() })
	c1a, c1b := net.Pipe()
	drainPeer(t, c1b)
	if err := s.Attach(c1a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	mkFrag := func(msgID uint32) []byte {
		fs, _, _ := Fragment(msgID, bytes.Repeat([]byte("z"), 2000), 0)
		return mustEncode(t, fs[0])
	}
	t.Log("input: msg 801 occupies the single in-flight slot; msg 802 must be rejected; msg 801 then times out")
	if _, err := c1b.Write(mkFrag(801)); err != nil {
		t.Fatalf("write 801: %v", err)
	}
	time.Sleep(50 * time.Millisecond) // ensure 801 is in flight before 802 arrives
	if _, err := c1b.Write(mkFrag(802)); err != nil {
		t.Fatalf("write 802: %v", err)
	}

	waitFor(t, "InFlightRejected==1", 2*time.Second, func() bool {
		return s.Stats().InFlightRejected == 1
	})
	waitFor(t, "FragmentTimeouts==1", 2*time.Second, func() bool {
		return s.Stats().FragmentTimeouts == 1
	})

	var sawOverflow, sawTimeout bool
	deadline := time.After(2 * time.Second)
	for !sawOverflow || !sawTimeout {
		select {
		case err := <-s.Errors():
			if errors.Is(err, ErrReassemblyOverflow) {
				sawOverflow = true
			}
			if errors.Is(err, ErrReassemblyTimeout) {
				sawTimeout = true
			}
			t.Logf("error surfaced: %v", err)
		case <-deadline:
			t.Fatalf("missing errors: overflow=%v timeout=%v", sawOverflow, sawTimeout)
		}
	}
	st := s.Stats()
	t.Logf("stats: %+v", st)
	if st.InFlightRejected != 1 || st.FragmentTimeouts != 1 {
		t.Fatalf("stats = %+v, want InFlightRejected=1 FragmentTimeouts=1", st)
	}
	t.Log("verdict: in-flight rejection and fragment timeout counted separately, both surfaced")
}

// TestSessionStatsNoiseBytes: skipped noise bytes are visible in stats.
func TestSessionStatsNoiseBytes(t *testing.T) {
	s := NewSession(SessionConfig{})
	t.Cleanup(func() { s.Close() })
	c1a, c1b := net.Pipe()
	drainPeer(t, c1b)
	if err := s.Attach(c1a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	noise := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01}
	frame := mustEncode(t, &Frame{Seq: 0, MsgID: 901, FragCnt: 1, Payload: []byte("clean")})
	t.Logf("input: %d noise bytes followed by one valid frame", len(noise))
	if _, err := c1b.Write(append(noise, frame...)); err != nil {
		t.Fatalf("write: %v", err)
	}

	// The noise produces an expected ErrBadMagic on the error channel;
	// tolerate exactly that while waiting for the frame.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case m := <-s.Delivered():
			if string(m.Data) != "clean" {
				t.Fatalf("payload=%q, want clean", m.Data)
			}
			goto delivered
		case err := <-s.Errors():
			if !errors.Is(err, ErrBadMagic) {
				t.Fatalf("unexpected session error: %v", err)
			}
			t.Logf("expected noise error surfaced: %v", err)
		case <-deadline:
			t.Fatal("timeout waiting for frame after noise")
		}
	}
delivered:
	waitFor(t, "NoiseBytesSkipped==5", 2*time.Second, func() bool {
		return s.Stats().NoiseBytesSkipped == uint64(len(noise))
	})
	st := s.Stats()
	t.Logf("stats: %+v", st)
	if st.FramesDecoded != 1 {
		t.Fatalf("FramesDecoded=%d, want 1", st.FramesDecoded)
	}
	t.Logf("verdict: %d noise bytes counted, valid frame decoded and delivered", st.NoiseBytesSkipped)
}

// TestSessionStatsConcurrentRead: Stats and PeerCaps may be polled from
// many goroutines while traffic flows. Run under -race to prove there is
// no data race; counters must stay consistent.
func TestSessionStatsConcurrentRead(t *testing.T) {
	s1, s2 := pipePair(t)
	const messages = 40

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = s2.Stats()
					_ = s2.PeerCaps()
				}
			}
		}(g)
	}
	t.Log("input: 4 goroutines polling Stats/PeerCaps while 40 messages flow")

	go func() {
		for i := 0; i < messages; i++ {
			if _, err := s1.SendMessage(bytes.Repeat([]byte{byte(i)}, 100)); err != nil {
				t.Errorf("SendMessage: %v", err)
				return
			}
		}
	}()
	got := recvN(t, s2, messages, 5*time.Second)
	close(stop)
	wg.Wait()

	st := s2.Stats()
	t.Logf("final stats: %+v", st)
	if st.FramesDecoded < messages {
		t.Fatalf("FramesDecoded=%d, want >= %d", st.FramesDecoded, messages)
	}
	t.Logf("verdict: %d messages delivered under concurrent stats polling, no race", len(got))
}
