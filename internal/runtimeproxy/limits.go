package runtimeproxy

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Limits bound what one Worker can make agentd hold in memory, which the OOM killer charges
// to agentd. Without them a Worker could make agentd hold every request it began (up to 128
// MiB each, on as many streams and connections as it liked), multiply a request up to 175
// times by having it decoded, and leave any number of responses unread (docs/m1-notes.md).
type Limits struct {
	// MaxRequest is the largest request agentd reads, decompressed: Temporal's own limit. Its
	// frontend keeps grpc-go's default of 4 MiB and refuses anything larger with
	// ResourceExhausted, so a larger request could never succeed. agentd refuses it the same
	// way, and the SDK fails the workflow task as GRPC_MESSAGE_TOO_LARGE, as it did when
	// Temporal refused.
	MaxRequest int
	// MaxShortRequest is the largest request of the methods that carry no task results
	// (all but largeRequests): polls, heartbeats, history pages, a few hundred bytes each from
	// the SDK. Some are long polls, which hold their share of ForwardBytes until Temporal
	// answers.
	MaxShortRequest int
	// MaxRequestValues bounds the field values in a request, counted on the wire before it
	// is decoded: decoding allocates for every value (an empty heartbeat is 2 bytes on the
	// wire and about 350 once decoded).
	MaxRequestValues int
	// MaxResponse is what Temporal's clients accept: histories arrive in responses.
	MaxResponse int
	// Connections is how many Runtime connections may be open at once; more wait in the
	// kernel's accept queue. A Worker uses one.
	Connections int
	// Streams is how many calls one connection may have open at once (HTTP/2's
	// SETTINGS_MAX_CONCURRENT_STREAMS); HTTP/2 clients queue calls beyond it.
	Streams uint32
	// HeaderBytes bounds a call's headers (SETTINGS_MAX_HEADER_LIST_SIZE).
	HeaderBytes uint32
	// Requests is how many requests are read, checked and forwarded at once, across all
	// connections. A call waiting for its turn has at most its stream window of request
	// data in agentd.
	Requests int
	// ForwardBytes bounds the requests agentd has handed to its connection to Temporal and
	// Temporal has not answered yet, across all calls. A request waits for its share in its
	// turn, so when Temporal falls behind, requests wait in the Workers, not in agentd.
	ForwardBytes int64
	// UnreadResponses is how many response bytes a connection may leave unread before agentd
	// takes no further response for it from Temporal.
	UnreadResponses int64
	// Responses is how many responses are taken from Temporal at once, across all
	// connections: agentd holds about three copies of a response while it takes it. Each
	// connection takes one at a time, so a slow one holds up no other connection, and a
	// response waits for its turn here only after the Worker has read the earlier ones.
	Responses int
	// SlowWorker is how long a Worker may take to send a request or to end its half of a
	// call, or keep a response waiting by leaving more than UnreadResponses unread, before
	// agentd closes its connection. Its calls end with it; what agentd held for it is freed.
	SlowWorker time.Duration
	// IdleConnection is how long a connection may have no call open before agentd closes it,
	// so connections nobody uses do not keep their places. A live Worker always has a poll
	// open.
	IdleConnection time.Duration
}

// DefaultLimits leave a wide margin over what the Python SDK did through the whole test
// suite: requests up to 12 KB (polls and heartbeats under 0.5 KB), responses up to 88 KB, at
// most two calls and two connections at a time, 179 bytes of metadata.
var DefaultLimits = Limits{
	MaxRequest:       4 << 20,
	MaxShortRequest:  64 << 10,
	MaxRequestValues: 50_000,
	MaxResponse:      128 << 20,
	Connections:      4,
	Streams:          32,
	HeaderBytes:      16 << 10,
	Requests:         4,
	ForwardBytes:     16 << 20,
	UnreadResponses:  8 << 20,
	Responses:        2,
	SlowWorker:       30 * time.Second,
	IdleConnection:   5 * time.Minute,
}

// largeRequests are the methods whose requests carry a workflow task's results, and may be as
// large as Temporal takes.
var largeRequests = map[string]bool{
	service + "RespondWorkflowTaskCompleted": true,
	service + "RespondWorkflowTaskFailed":    true,
	service + "RespondQueryTaskCompleted":    true,
}

// streamWindow is every stream's receive window, in both directions, fixed: grpc-go's
// dynamic windows grew to 1.4 MB per stream while a Worker's requests sat unread. A message
// being read gets a window of its own size, so this costs nothing.
const streamWindow = 64 << 10

// backendConnWindow is the connection window towards Temporal. grpc-go replenishes it as data
// arrives, read or not, so it costs no memory; 64 KiB made a 45 MiB history take 8 seconds
// from a Temporal 10 ms away.
const backendConnWindow = 16 << 20

// tooLarge refuses a request in the words of grpc's own size refusal, which the SDK
// recognises: it fails the workflow task as GRPC_MESSAGE_TOO_LARGE rather than retrying it
// for ever.
func tooLarge(detail string) error {
	return status.Errorf(codes.ResourceExhausted, "grpc: received message larger than max (%s)", detail)
}

// logName is a method's name as logged: a Worker chooses it, and long ones would fill the log.
func logName(method string) string {
	if len(method) > 128 {
		return method[:128] + "..."
	}
	return method
}

// --- connections -------------------------------------------------------------------------

// listener accepts at most Limits.Connections connections at once and wraps each in a conn.
type listener struct {
	net.Listener
	s      *Server
	done   chan struct{}
	once   sync.Once
	nextID atomic.Uint64
	warned time.Time // when the limit was last logged; only Serve's goroutine calls Accept
}

func (l *listener) Accept() (net.Conn, error) {
	select {
	case l.s.conns <- struct{}{}:
	default:
		if time.Since(l.warned) > time.Minute {
			l.warned = time.Now()
			l.s.log.Warn("runtime connections at the limit; new ones wait", "limit", cap(l.s.conns))
		}
		select {
		case l.s.conns <- struct{}{}:
		case <-l.done:
			return nil, net.ErrClosed
		}
	}
	c, err := l.Listener.Accept()
	if err != nil {
		<-l.s.conns
		return nil, err
	}
	return &conn{
		Conn: c, id: l.nextID.Add(1), log: l.s.log, free: func() { <-l.s.conns },
		closed: make(chan struct{}), changed: make(chan struct{}), turn: make(chan struct{}, 1),
	}, nil
}

func (l *listener) Close() error {
	l.once.Do(func() { close(l.done) })
	return l.Listener.Close()
}

// conn is one Runtime connection. It counts the response bytes written to it, so the proxy
// knows how much of its responses the Worker has left unread.
type conn struct {
	net.Conn
	id     uint64
	log    *slog.Logger
	free   func() // gives the connection's place back
	once   sync.Once
	closed chan struct{}
	turn   chan struct{} // the turn to take a response from Temporal for this connection

	mu      sync.Mutex
	flow    flow          // what of its responses the Worker has left unread
	changed chan struct{} // closed, and replaced, when that may have shrunk
}

// Read follows what the Worker sends, for the calls it opens and resets.
func (c *conn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.mu.Lock()
	if c.flow.read(p[:n]) {
		c.signal()
	}
	c.mu.Unlock()
	return n, err
}

// Write follows what agentd writes: the responses, as they reach the socket. They are
// counted before they are written: once written, the Worker can read them and reset the call
// before this goroutine counts them, and a reset stream's later bytes are not counted. A
// write that fails, even in part, ends the connection.
func (c *conn) Write(p []byte) (int, error) {
	c.mu.Lock()
	if c.flow.wrote(p) {
		c.signal()
	}
	c.mu.Unlock()
	return c.Conn.Write(p)
}

// signal wakes those waiting for the Worker to read; c.mu is held.
func (c *conn) signal() {
	close(c.changed)
	c.changed = make(chan struct{})
}

func (c *conn) Close() error { return c.close("") }

// drop closes the connection because of the Worker: every call on it ends.
func (c *conn) drop(reason string, attrs ...any) { c.close(reason, attrs...) }

// close closes the connection once; the first caller's reason is the one logged.
func (c *conn) close(reason string, attrs ...any) error {
	err := net.ErrClosed
	c.once.Do(func() {
		if reason != "" {
			c.log.Warn("runtime connection closed", append([]any{"reason", reason, "conn", c.name()}, attrs...)...)
		}
		err = c.Conn.Close()
		close(c.closed)
		c.free()
	})
	return err
}

func (c *conn) name() string { return fmt.Sprintf("%d %s", c.id, c.RemoteAddr()) }

// refusalLog logs refused calls. Each one means a Worker tried what no Worker needs, so the
// first of each kind (method, command and rule) in an hour is logged in full, whatever came
// before it: a flood of one kind hides no other. Beyond those, about one a second is logged
// across all connections, and the rest are counted in a line at most `every` later: a Worker
// could otherwise fill agentd's log as fast as it can make calls.
type refusalLog struct {
	log   *slog.Logger
	every time.Duration
	lines *rate.Limiter

	mu    sync.Mutex
	seen  map[string]time.Time // when each kind was last logged in full
	held  int64                // refusals not logged since the last count
	timer *time.Timer
}

func (r *refusalLog) refused(kind string, attrs ...any) {
	now := time.Now()
	r.mu.Lock()
	first := now.Sub(r.seen[kind]) > time.Hour && r.room(now)
	if first {
		r.seen[kind] = now
	}
	if !first && !r.lines.Allow() {
		r.held++
		if r.timer == nil {
			r.timer = time.AfterFunc(r.every, r.count)
		}
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()
	r.log.Warn("runtime call refused", attrs...)
}

// room makes room for one more kind, dropping those not logged for an hour. Kinds are few
// (allowed methods, command types and rules); this only bounds a mistake.
func (r *refusalLog) room(now time.Time) bool {
	if len(r.seen) < maxKinds {
		return true
	}
	for k, t := range r.seen {
		if now.Sub(t) > time.Hour {
			delete(r.seen, k)
		}
	}
	return len(r.seen) < maxKinds
}

func (r *refusalLog) count() {
	r.mu.Lock()
	n := r.held
	r.held, r.timer = 0, nil
	r.mu.Unlock()
	r.log.Warn("runtime call refusals not logged", "count", n, "within", r.every.String())
}

const maxKinds = 256

// logReason is a refusal's reason as logged: it can quote what the Worker sent, up to 4 MiB.
func logReason(reason string) string {
	if len(reason) > 300 {
		return reason[:300] + "..."
	}
	return reason
}

// within runs fn, which reads from the Worker, and closes the connection if fn has not
// returned after d: meanwhile the Worker holds a turn other calls wait for.
func (c *conn) within(d time.Duration, what string, fn func() error) error {
	var returned atomic.Bool
	t := time.AfterFunc(d, func() {
		if !returned.Load() {
			c.drop(fmt.Sprintf("%s took longer than %v", what, d))
		}
	})
	defer t.Stop()
	err := fn()
	returned.Store(true)
	return err
}

// queue counts a response taken from Temporal for this connection, or one grpc-go would not
// take (n < 0).
func (c *conn) queue(n int64) {
	c.mu.Lock()
	c.flow.taken(n)
	if n < 0 {
		c.signal()
	}
	c.mu.Unlock()
}

// admit takes the connection's turn to take a response from Temporal, once the Worker has
// read all but limit bytes of the earlier ones, and then one of takes, the responses taken
// at once across connections. Only the turn's holder adds to what is unread, so waiting
// with it holds nothing else back. A Worker that leaves more unread for longer than d while
// a response waits loses its connection: agentd would otherwise hold its responses for as
// long as it liked.
func (c *conn) admit(ctx context.Context, limit int64, d time.Duration, takes chan struct{}) (release func(), err error) {
	select {
	case c.turn <- struct{}{}:
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	case <-c.closed:
		return nil, errConnClosed
	}
	if err := c.waitUnread(ctx, limit, d); err != nil {
		<-c.turn
		return nil, err
	}
	select {
	case takes <- struct{}{}:
		return func() { <-takes; <-c.turn }, nil
	case <-ctx.Done():
		err = status.FromContextError(ctx.Err()).Err()
	case <-c.closed:
		err = errConnClosed
	}
	<-c.turn
	return nil, err
}

var errConnClosed = status.Error(codes.Unavailable, "agentd: runtime connection closed")

func (c *conn) waitUnread(ctx context.Context, limit int64, d time.Duration) error {
	var timeout <-chan time.Time
	for {
		c.mu.Lock()
		unread, changed := c.flow.unread(), c.changed
		c.mu.Unlock()
		if unread <= limit {
			return nil
		}
		if timeout == nil {
			t := time.NewTimer(d)
			defer t.Stop()
			timeout = t.C
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		case <-c.closed:
			return errConnClosed
		case <-timeout:
			c.drop(fmt.Sprintf("responses left unread for %v", d), "unread_bytes", unread)
			return status.Error(codes.Unavailable, "agentd: the Worker is not reading its responses")
		}
	}
}

// flow follows the HTTP/2 frames of a Runtime connection both ways and keeps how much of the
// responses taken for it is still to be written: what the Worker has left unread. A
// response counts from when it is taken from Temporal until its stream ends: its trailers
// are written, or either side resets the stream, after which grpc-go writes no more of it.
// Its size on the wire is the gRPC length prefix at the start of its first DATA frame; until
// that is written it counts in pending. grpc-go pads no frame.
type flow struct {
	in, out    frames
	preface    int                // bytes of the Worker's connection preface seen
	pending    int64              // responses taken whose size is not yet on the wire
	streams    map[uint32]*stream // the calls open, from the Worker's HEADERS frame on
	lastOpened uint32
}

type stream struct {
	prefix  [5]byte
	n       int   // prefix bytes written
	written int64 // bytes written while the prefix was incomplete
	left    int64 // bytes of the response still to be written; -1 until its size is known
}

// taken counts a response of n bytes taken from Temporal, or one grpc-go would not take
// (n < 0).
func (fl *flow) taken(n int64) { fl.pending += n }

func (fl *flow) unread() int64 {
	n := fl.pending
	for _, st := range fl.streams {
		n += max(st.left, 0)
	}
	return n
}

// read follows what the Worker sends: its preface, then frames. A HEADERS frame opens a call;
// RST_STREAM ends one. It reports whether a response ended.
func (fl *flow) read(p []byte) (ended bool) {
	if k := min(len(http2.ClientPreface)-fl.preface, len(p)); k > 0 {
		fl.preface += k
		p = p[k:]
	}
	fl.in.feed(p, func(f *frames) {
		switch f.typ {
		case http2.FrameHeaders:
			if f.id > fl.lastOpened { // stream IDs only grow; an old one stays ended
				fl.lastOpened = f.id
				if fl.streams == nil {
					fl.streams = map[uint32]*stream{}
				}
				fl.streams[f.id] = &stream{left: -1}
			}
		case http2.FrameRSTStream:
			ended = fl.end(f.id) || ended
		}
	}, nil)
	return ended
}

// wrote follows what agentd writes: the responses' DATA frames, and the end of each call. It
// reports whether anything of a response was written or ended.
func (fl *flow) wrote(p []byte) (progress bool) {
	fl.out.feed(p, func(f *frames) {
		if f.typ == http2.FrameRSTStream || f.typ == http2.FrameHeaders && f.flags.Has(http2.FlagHeadersEndStream) {
			progress = fl.end(f.id) || progress
		}
	}, func(f *frames, b []byte) {
		st := fl.streams[f.id]
		if f.typ != http2.FrameData || st == nil {
			return // a stream that ended: what grpc-go still writes of it was already not counted
		}
		progress = true
		if st.left >= 0 {
			st.left = max(st.left-int64(len(b)), 0)
			return
		}
		st.written += int64(len(b))
		st.n += copy(st.prefix[st.n:], b)
		if st.n == len(st.prefix) {
			size := 5 + int64(binary.BigEndian.Uint32(st.prefix[1:]))
			fl.pending -= size
			st.left = max(size-st.written, 0)
		}
	})
	return progress
}

// end ends a call: what was left of its response is dropped, not unread. A response whose
// stream ends before its first 5 bytes are written stays in pending: only a Worker that
// gives a call no window at all and resets it does that, and it costs that Worker its
// connection once more than Limits.UnreadResponses has gathered.
func (fl *flow) end(id uint32) bool {
	if _, ok := fl.streams[id]; !ok {
		return false
	}
	delete(fl.streams, id)
	return true
}

// frames follows a sequence of HTTP/2 frames, given in pieces of any size.
type frames struct {
	head  [9]byte
	n     int // bytes of head seen
	left  int // payload bytes left in the current frame
	typ   http2.FrameType
	flags http2.Flags
	id    uint32
}

// feed calls frame on each frame's header, and data on each piece of its payload.
func (f *frames) feed(p []byte, frame func(*frames), data func(*frames, []byte)) {
	for len(p) > 0 {
		if f.left > 0 {
			k := min(f.left, len(p))
			if data != nil {
				data(f, p[:k])
			}
			f.left -= k
			p = p[k:]
			continue
		}
		k := copy(f.head[f.n:], p)
		f.n += k
		p = p[k:]
		if f.n == len(f.head) {
			f.n = 0
			f.left = int(f.head[0])<<16 | int(f.head[1])<<8 | int(f.head[2])
			f.typ, f.flags = http2.FrameType(f.head[3]), http2.Flags(f.head[4])
			f.id = binary.BigEndian.Uint32(f.head[5:]) & (1<<31 - 1)
			frame(f)
		}
	}
}

// connCreds hands each call its connection, through peer.AuthInfo. There is no handshake:
// the Runtime entry is a 0600 Unix socket in a 0700 directory, or a loopback port.
type connCreds struct{}

type connInfo struct {
	credentials.CommonAuthInfo
	conn *conn
}

func (connInfo) AuthType() string { return "agentd-runtime" }

func (connCreds) ServerHandshake(c net.Conn) (net.Conn, credentials.AuthInfo, error) {
	rc, ok := c.(*conn)
	if !ok {
		return nil, nil, errors.New("runtime connection not accepted by the proxy's listener")
	}
	return c, connInfo{CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.NoSecurity}, conn: rc}, nil
}

func (connCreds) ClientHandshake(context.Context, string, net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("the runtime proxy's credentials are for its server only")
}

func (connCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "insecure"}
}

func (c connCreds) Clone() credentials.TransportCredentials { return c }

func (connCreds) OverrideServerName(string) error { return nil }

func connFrom(ctx context.Context) *conn {
	if p, ok := peer.FromContext(ctx); ok {
		if i, ok := p.AuthInfo.(connInfo); ok {
			return i.conn
		}
	}
	return nil
}

// --- request values ----------------------------------------------------------------------

// maxDepth bounds how deep messages nest in a request. Temporal's are a handful of levels
// deep; a failure's chain of causes adds one level per cause.
const maxDepth = 100

var errTooDeep = fmt.Errorf("messages nested more than %d deep", maxDepth)

// countValues counts the field values in raw, a message of type md on the wire, nested
// messages and each element of packed lists included, without decoding anything. It stops
// with an error past limit.
func countValues(raw []byte, md protoreflect.MessageDescriptor, limit int) error {
	n := 0
	return walkValues(raw, md, 0, &n, limit)
}

func walkValues(b []byte, md protoreflect.MessageDescriptor, depth int, n *int, limit int) error {
	if depth > maxDepth {
		return errTooDeep
	}
	for len(b) > 0 {
		num, typ, k := protowire.ConsumeTag(b)
		if k < 0 {
			return protowire.ParseError(k)
		}
		b = b[k:]
		fd := md.Fields().ByNumber(num)
		values := 1
		if typ == protowire.BytesType && fd != nil {
			v, k := protowire.ConsumeBytes(b)
			if k < 0 {
				return protowire.ParseError(k)
			}
			switch {
			case fd.Kind() == protoreflect.MessageKind:
				if *n++; *n > limit {
					return tooManyValues(limit)
				}
				if err := walkValues(v, fd.Message(), depth+1, n, limit); err != nil {
					return err
				}
				b = b[k:]
				continue
			case fd.IsList():
				values = packedValues(v, fd.Kind())
			}
		}
		if *n += values; *n > limit {
			return tooManyValues(limit)
		}
		k = protowire.ConsumeFieldValue(num, typ, b)
		if k < 0 {
			return protowire.ParseError(k)
		}
		b = b[k:]
	}
	return nil
}

// packedValues counts the elements of a packed list of kind, at least one.
func packedValues(v []byte, kind protoreflect.Kind) int {
	n := 0
	switch kind {
	case protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind, protoreflect.FloatKind:
		n = len(v) / 4
	case protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind, protoreflect.DoubleKind:
		n = len(v) / 8
	case protoreflect.StringKind, protoreflect.BytesKind:
		n = 1 // one string, not a packed list
	default: // varints: one ends at each byte without the continuation bit
		for _, c := range v {
			if c < 0x80 {
				n++
			}
		}
	}
	return max(n, 1)
}

type valueLimitError int

func (e valueLimitError) Error() string { return fmt.Sprintf("more than %d values", int(e)) }

func tooManyValues(limit int) error { return valueLimitError(limit) }
