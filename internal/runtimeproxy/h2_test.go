package runtimeproxy

// A Worker that misbehaves at the HTTP/2 level, for the limit tests: it can withhold the last
// byte of a request, ignore the server's stream limit, never read a response or send
// oversized headers, none of which a gRPC client would do.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

type rawConn struct {
	t    *testing.T
	conn net.Conn
	fr   *http2.Framer

	wmu  sync.Mutex // writes, and the hpack encoder
	henc *hpack.Encoder
	hbuf bytes.Buffer

	mu         sync.Mutex
	cond       *sync.Cond
	connWin    int64
	initWin    int64 // the server's SETTINGS_INITIAL_WINDOW_SIZE
	maxFrame   int
	maxStreams uint32
	settings   []http2.Setting // every setting the server sent, in order
	streams    map[uint32]*rawStream
	nextID     uint32
	closed     error
	gotSet     chan struct{}

	read bool // send WINDOW_UPDATE for response data, as a reading client does
}

type rawStream struct {
	id      uint32
	held    bool // the client reads none of this stream's response beyond its first window
	win     int64
	reset   http2.ErrCode
	isReset bool
	ended   bool
	status  string
	message string
	sent    atomic.Int64
	got     atomic.Int64
}

// dialRaw connects to addr and waits for the server's SETTINGS, unless wait is false.
func dialRaw(t *testing.T, addr string, read, wait bool) *rawConn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c := &rawConn{
		t: t, conn: conn, fr: http2.NewFramer(conn, conn), connWin: 65535, initWin: 65535, maxFrame: 16384,
		maxStreams: 1<<31 - 1, streams: map[uint32]*rawStream{}, nextID: 1, gotSet: make(chan struct{}), read: read,
	}
	c.cond = sync.NewCond(&c.mu)
	c.henc = hpack.NewEncoder(&c.hbuf)
	c.fr.ReadMetaHeaders = hpack.NewDecoder(1<<20, nil)
	c.fr.MaxHeaderListSize = 1 << 20
	t.Cleanup(func() { conn.Close() })
	if _, err := conn.Write([]byte(http2.ClientPreface)); err != nil {
		t.Fatal(err)
	}
	if err := c.fr.WriteSettings(); err != nil {
		t.Fatal(err)
	}
	go c.readLoop()
	if wait && !c.waitSettings(5*time.Second) {
		t.Fatal("no SETTINGS from the server")
	}
	return c
}

func (c *rawConn) waitSettings(d time.Duration) bool {
	select {
	case <-c.gotSet:
		return true
	case <-time.After(d):
		return false
	}
}

func (c *rawConn) readLoop() {
	first := true
	for {
		f, err := c.fr.ReadFrame()
		if err != nil {
			c.mu.Lock()
			c.closed = err
			c.cond.Broadcast()
			c.mu.Unlock()
			return
		}
		c.mu.Lock()
		switch f := f.(type) {
		case *http2.SettingsFrame:
			if f.IsAck() {
				break
			}
			f.ForeachSetting(func(s http2.Setting) error {
				c.settings = append(c.settings, s)
				switch s.ID {
				case http2.SettingInitialWindowSize:
					d := int64(s.Val) - c.initWin
					c.initWin = int64(s.Val)
					for _, st := range c.streams {
						st.win += d
					}
				case http2.SettingMaxFrameSize:
					c.maxFrame = int(s.Val)
				case http2.SettingMaxConcurrentStreams:
					c.maxStreams = s.Val
				}
				return nil
			})
			c.write(func() error { return c.fr.WriteSettingsAck() })
			if first {
				first = false
				close(c.gotSet)
			}
		case *http2.WindowUpdateFrame:
			if f.StreamID == 0 {
				c.connWin += int64(f.Increment)
			} else if st := c.streams[f.StreamID]; st != nil {
				st.win += int64(f.Increment)
			}
		case *http2.PingFrame:
			if !f.IsAck() {
				c.write(func() error { return c.fr.WritePing(true, f.Data) })
			}
		case *http2.RSTStreamFrame:
			if st := c.streams[f.StreamID]; st != nil {
				st.reset, st.isReset = f.ErrCode, true
			}
		case *http2.MetaHeadersFrame:
			if st := c.streams[f.StreamID]; st != nil {
				for _, hf := range f.Fields {
					switch hf.Name {
					case "grpc-status":
						st.status = hf.Value
					case "grpc-message":
						st.message = hf.Value
					}
				}
				st.ended = st.ended || f.StreamEnded()
			}
		case *http2.DataFrame:
			n := len(f.Data())
			if st := c.streams[f.StreamID]; st != nil {
				st.got.Add(int64(n))
				st.ended = st.ended || f.StreamEnded()
			}
			if c.read && n > 0 {
				st := c.streams[f.StreamID]
				c.write(func() error {
					if err := c.fr.WriteWindowUpdate(0, uint32(n)); err != nil || (st != nil && st.held) {
						return err
					}
					return c.fr.WriteWindowUpdate(f.StreamID, uint32(n))
				})
			}
		}
		c.cond.Broadcast()
		c.mu.Unlock()
	}
}

func (c *rawConn) write(fn func() error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	fn() // a failed write shows up as a read error
}

// open starts a call of method with the extra headers. It ignores the server's stream limit.
func (c *rawConn) open(method string, extra ...hpack.HeaderField) *rawStream {
	c.t.Helper()
	c.mu.Lock()
	st := &rawStream{id: c.nextID, win: c.initWin}
	c.nextID += 2
	c.streams[st.id] = st
	maxFrame := c.maxFrame
	c.mu.Unlock()

	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.hbuf.Reset()
	for _, hf := range append([]hpack.HeaderField{
		{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: "http"}, {Name: ":path", Value: service + method},
		{Name: ":authority", Value: "localhost"}, {Name: "content-type", Value: "application/grpc"},
		{Name: "te", Value: "trailers"},
	}, extra...) {
		c.henc.WriteField(hf)
	}
	block := c.hbuf.Bytes()
	for first := true; ; first = false {
		n := min(len(block), maxFrame)
		end := n == len(block)
		var err error
		if first {
			err = c.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: st.id, BlockFragment: block[:n], EndHeaders: end})
		} else {
			err = c.fr.WriteContinuation(st.id, end, block[:n])
		}
		if err != nil {
			c.t.Fatal(err)
		}
		if block = block[n:]; end {
			return st
		}
	}
}

// hold stops the client reading the stream's response beyond what its window lets through.
func (c *rawConn) hold(st *rawStream) {
	c.mu.Lock()
	st.held = true
	c.mu.Unlock()
}

// reset cancels the stream, as a call's deadline does.
func (c *rawConn) reset(st *rawStream) {
	c.write(func() error { return c.fr.WriteRSTStream(st.id, http2.ErrCodeCancel) })
}

// send writes data on the stream as flow control allows, ending the stream if end. It returns
// when everything is sent, or with an error when the stream or connection ends or nothing can
// be sent for the given time.
func (c *rawConn) send(st *rawStream, data []byte, end bool, stall time.Duration) error {
	for {
		c.mu.Lock()
		deadline := time.AfterFunc(stall, func() { c.mu.Lock(); c.cond.Broadcast(); c.mu.Unlock() })
		start := time.Now()
		for len(data) > 0 && c.closed == nil && !st.isReset && !st.ended && (c.connWin <= 0 || st.win <= 0) {
			if time.Since(start) >= stall {
				c.mu.Unlock()
				deadline.Stop()
				return errStalled
			}
			c.cond.Wait()
		}
		deadline.Stop()
		switch {
		case c.closed != nil:
			c.mu.Unlock()
			return c.closed
		case st.isReset:
			c.mu.Unlock()
			return fmt.Errorf("stream reset: %v", st.reset)
		case st.ended:
			c.mu.Unlock()
			return fmt.Errorf("stream ended: status %s %q", st.status, st.message)
		}
		n := min(len(data), c.maxFrame, int(c.connWin), int(st.win))
		c.connWin -= int64(n)
		st.win -= int64(n)
		c.mu.Unlock()
		last := end && n == len(data)
		c.wmu.Lock()
		err := c.fr.WriteData(st.id, last, data[:n])
		c.wmu.Unlock()
		if err != nil {
			return err
		}
		st.sent.Add(int64(n))
		if data = data[n:]; len(data) == 0 {
			return nil
		}
	}
}

var errStalled = errors.New("flow control did not let anything through")

// wait waits until done says yes, or fails the test after d.
func (c *rawConn) wait(d time.Duration, what string, done func() bool) {
	c.t.Helper()
	stop := time.AfterFunc(d, func() { c.mu.Lock(); c.cond.Broadcast(); c.mu.Unlock() })
	defer stop.Stop()
	start := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	for !done() {
		if time.Since(start) >= d {
			c.t.Fatalf("timed out after %v waiting for %s", d, what)
		}
		c.cond.Wait()
	}
}

func (c *rawConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed != nil
}

// state is a stream's state as the client last saw it.
func (c *rawConn) state(st *rawStream) rawStream {
	c.mu.Lock()
	defer c.mu.Unlock()
	return rawStream{id: st.id, reset: st.reset, isReset: st.isReset, ended: st.ended, status: st.status, message: st.message}
}

// frame is a gRPC message on the wire: a 5-byte prefix declaring its length, then the message.
func frame(msg []byte, declared int) []byte {
	out := make([]byte, 5, 5+len(msg))
	binary.BigEndian.PutUint32(out[1:], uint32(declared))
	return append(out, msg...)
}
