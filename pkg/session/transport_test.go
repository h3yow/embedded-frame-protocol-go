package session

import (
	"bytes"
	"sync"
)

// memPipe is one end of an in-process connected pair.
//
// Data written to a memPipe appears in its PEER's read buffer, exactly like a
// connected socket: p.Write -> p.peer.Read. Therefore when session A owns
// end "x" and session B owns end "y", frames sent by A (x.Write) are read by
// B (y.Read) and vice versa.
type memPipe struct {
	mu     sync.Mutex
	cond   *sync.Cond
	buf    bytes.Buffer
	closed bool
	peer   *memPipe
}

// newPipePair returns (x, y): x.Write is readable by the holder of y and
// y.Write is readable by the holder of x.
func newPipePair() (x *memPipe, y *memPipe) {
	x = &memPipe{}
	y = &memPipe{}
	x.cond = sync.NewCond(&x.mu)
	y.cond = sync.NewCond(&y.mu)
	x.peer = y
	y.peer = x
	return x, y
}

// sessionPipe returns transports for two sessions such that traffic flows:
// a (holding clientEnd) -> b (holding serverEnd) -> a.
func sessionPipes() (clientEnd, serverEnd *memPipe) {
	x, y := newPipePair()
	// x.Write lands in y.buf; give x to client A and y to server B so that
	// A.Send (x.Write) is read by B (y.Read). B's replies (y.Write) land in
	// x.buf and are read by A.
	return x, y
}

func (p *memPipe) Read(b []byte) (int, error) {
	p.mu.Lock()
	for p.buf.Len() == 0 && !p.closed {
		p.cond.Wait()
	}
	if p.buf.Len() == 0 {
		p.mu.Unlock()
		return 0, errClosedPipe
	}
	n, _ := p.buf.Read(b)
	p.mu.Unlock()
	return n, nil
}

func (p *memPipe) Write(b []byte) (int, error) {
	p.peer.mu.Lock()
	if p.peer.closed {
		p.peer.mu.Unlock()
		return 0, errClosedPipe
	}
	n, err := p.peer.buf.Write(b)
	p.peer.cond.Broadcast()
	p.peer.mu.Unlock()
	return n, err
}

// writeRaw injects arbitrary bytes into this end's receive buffer (bypassing
// the peer). Used to inject noise and corrupt frames towards this end's
// holder: to feed bytes read by session S holding end p, call p.writeRaw.
func (p *memPipe) writeRaw(b []byte) {
	p.mu.Lock()
	p.buf.Write(b)
	p.cond.Broadcast()
	p.mu.Unlock()
}

func (p *memPipe) Close() error {
	p.mu.Lock()
	p.closed = true
	p.cond.Broadcast()
	p.mu.Unlock()
	return nil
}

type closedPipeError struct{}

func (closedPipeError) Error() string { return "test pipe closed" }

var errClosedPipe = closedPipeError{}
