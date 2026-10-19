package runtimeproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// temporal stands in for Temporal's frontend: it takes requests of any size, records them and
// answers with what respond returns, which may block.
type temporal struct {
	cc      *grpc.ClientConn // agentd's connection to it
	respond func(method string, req []byte) []byte
	// beforeAnswer, if set, runs before the answer is sent: it may send headers early.
	beforeAnswer func(method string, ss grpc.ServerStream)

	mu    sync.Mutex
	calls []backendCall
}

type backendCall struct {
	method   string
	requests int    // messages received on the call
	first    []byte // the first one
}

func startTemporal(t *testing.T, respond func(method string, req []byte) []byte) *temporal {
	t.Helper()
	if respond == nil {
		respond = func(string, []byte) []byte { return nil }
	}
	tp := &temporal{respond: respond}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.MaxRecvMsgSize(1<<30), grpc.MaxSendMsgSize(1<<30),
		grpc.UnknownServiceHandler(func(_ any, ss grpc.ServerStream) error {
			method, _ := grpc.MethodFromServerStream(ss)
			var c backendCall
			c.method = strings.TrimPrefix(method, service)
			for {
				req := &emptypb.Empty{}
				if err := ss.RecvMsg(req); err == io.EOF {
					break
				} else if err != nil {
					tp.record(c)
					return err
				}
				if c.requests++; c.requests == 1 {
					c.first = req.ProtoReflect().GetUnknown()
				}
			}
			tp.record(c)
			resp := &emptypb.Empty{}
			resp.ProtoReflect().SetUnknown(tp.respond(c.method, c.first))
			if tp.beforeAnswer != nil {
				tp.beforeAnswer(c.method, ss)
			}
			return ss.SendMsg(resp)
		}))
	go srv.Serve(ln)
	t.Cleanup(srv.Stop)
	tp.cc, err = grpc.NewClient(ln.Addr().String(),
		append(DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tp.cc.Close() })
	return tp
}

func (tp *temporal) record(c backendCall) {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	tp.calls = append(tp.calls, c)
}

func (tp *temporal) received() []backendCall {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	return append([]backendCall(nil), tp.calls...)
}

// logBuffer collects agentd's JSON log lines.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

// waitLines waits up to 5s for at least n lines with msg: grpc-go sends a call's status
// before the handler has returned, and logged.
func (l *logBuffer) waitLines(t *testing.T, msg string, n int) []map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if lines := l.lines(msg); len(lines) >= n || time.Now().After(deadline) {
			return lines
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (l *logBuffer) lines(msg string) []map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []map[string]any
	for line := range strings.SplitSeq(l.buf.String(), "\n") {
		var e map[string]any
		if json.Unmarshal([]byte(line), &e) == nil && e["msg"] == msg {
			out = append(out, e)
		}
	}
	return out
}

// startProxy runs the Runtime proxy in front of tp and returns its address and its log.
func startProxy(t *testing.T, tp *temporal) (string, *logBuffer) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	logs := &logBuffer{}
	srv := NewServer(tp.cc, rules, slog.New(slog.NewJSONHandler(logs, nil)))
	go srv.Serve(ln)
	t.Cleanup(srv.Stop)
	return ln.Addr().String(), logs
}

// dialWorker connects a well-behaved gRPC client, as the Worker's SDK is.
func dialWorker(t *testing.T, addr string) workflowservice.WorkflowServiceClient {
	t.Helper()
	cc, err := grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(1<<30), grpc.MaxCallRecvMsgSize(1<<30)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return workflowservice.NewWorkflowServiceClient(cc)
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return c
}

// completion is a RespondWorkflowTaskCompleted request of exactly size bytes.
func completion(t *testing.T, size int) *workflowservice.RespondWorkflowTaskCompletedRequest {
	t.Helper()
	req := &workflowservice.RespondWorkflowTaskCompletedRequest{Namespace: "default", TaskToken: []byte("token")}
	pad := size - proto.Size(req)
	for range 3 {
		req.Identity = strings.Repeat("x", max(pad, 0))
		pad += size - proto.Size(req)
	}
	if proto.Size(req) != size {
		t.Fatalf("cannot make a %d-byte request (got %d)", size, proto.Size(req))
	}
	return req
}

func TestARequestOverTemporalsLimitIsRefused(t *testing.T) {
	tp := startTemporal(t, nil)
	addr, logs := startProxy(t, tp)
	w := dialWorker(t, addr)

	if _, err := w.RespondWorkflowTaskCompleted(ctx(t), completion(t, 4<<20)); err != nil {
		t.Fatalf("a 4 MiB request was refused: %v", err)
	}
	_, err := w.RespondWorkflowTaskCompleted(ctx(t), completion(t, 4<<20+1))
	// The same refusal Temporal gives: the SDK fails the workflow task as GRPC_MESSAGE_TOO_LARGE.
	if status.Code(err) != codes.ResourceExhausted || !strings.Contains(err.Error(), "grpc: received message larger than max") {
		t.Fatalf("a request over 4 MiB: got %v, want ResourceExhausted, received message larger than max", err)
	}
	if got := tp.received(); len(got) != 1 || len(got[0].first) != 4<<20 {
		t.Errorf("Temporal received %d calls, want only the 4 MiB one", len(got))
	}
	refused := logs.waitLines(t, "runtime call refused", 1)
	if len(refused) != 1 || !strings.Contains(refused[0]["reason"].(string), "larger than max") {
		t.Errorf("want one refusal in agentd's log, got %v", refused)
	}
}

func TestARequestWithTooManyValuesIsRefusedBeforeDecoding(t *testing.T) {
	tp := startTemporal(t, nil)
	addr, logs := startProxy(t, tp)
	w := dialWorker(t, addr)
	// The namespace and the task token are two values; each START_TIMER command is two more
	// (the command and its type), 4 bytes on the wire and about 120 once decoded.
	timers := func(n int) *workflowservice.RespondWorkflowTaskCompletedRequest {
		req := &workflowservice.RespondWorkflowTaskCompletedRequest{Namespace: "default", TaskToken: []byte("token")}
		for range n {
			req.Commands = append(req.Commands, &commandpb.Command{CommandType: enumspb.COMMAND_TYPE_START_TIMER})
		}
		return req
	}
	atLimit := (DefaultLimits.MaxRequestValues - 2) / 2
	if _, err := w.RespondWorkflowTaskCompleted(ctx(t), timers(atLimit)); err != nil {
		t.Fatalf("a request at the value limit was refused: %v", err)
	}
	_, err := w.RespondWorkflowTaskCompleted(ctx(t), timers(atLimit+1))
	// Worded as grpc's own size refusal: the SDK fails a workflow task on it rather than
	// retrying it for ever.
	if status.Code(err) != codes.ResourceExhausted || !strings.HasPrefix(status.Convert(err).Message(), "grpc: received message larger than max") {
		t.Fatalf("a request over the value limit: got %v, want ResourceExhausted, received message larger than max", err)
	}
	if got := tp.received(); len(got) != 1 {
		t.Errorf("Temporal received %d calls, want only the one at the limit", len(got))
	}
	if refused := logs.waitLines(t, "runtime call refused", 1); len(refused) != 1 {
		t.Errorf("want one refusal in agentd's log, got %v", refused)
	}
}

func TestOneConnectionHasAtMostSoManyCalls(t *testing.T) {
	hold := make(chan struct{})
	tp := startTemporal(t, func(string, []byte) []byte { <-hold; return nil })
	t.Cleanup(func() { close(hold) })
	addr, _ := startProxy(t, tp)
	c := dialRaw(t, addr, true, true)
	if c.maxStreams != DefaultLimits.Streams {
		t.Errorf("SETTINGS_MAX_CONCURRENT_STREAMS = %d, want %d", c.maxStreams, DefaultLimits.Streams)
	}
	req, _ := proto.Marshal(&workflowservice.DescribeNamespaceRequest{Namespace: "default"})
	var all []*rawStream
	for range DefaultLimits.Streams {
		st := c.open("DescribeNamespace")
		if err := c.send(st, frame(req, len(req)), true, 5*time.Second); err != nil {
			t.Fatal(err)
		}
		all = append(all, st)
	}
	extra := c.open("DescribeNamespace") // one more than the server allows
	c.send(extra, frame(req, len(req)), true, 5*time.Second)
	c.wait(5*time.Second, "the call over the limit to be refused", func() bool { return extra.isReset || extra.ended })
	if s := c.state(extra); s.reset != http2.ErrCodeRefusedStream {
		t.Errorf("the call over the limit: reset %v (ended %v, status %q), want REFUSED_STREAM", s.reset, s.ended, s.status)
	}
	time.Sleep(200 * time.Millisecond)
	if n := len(tp.received()); n > int(DefaultLimits.Streams) {
		t.Errorf("Temporal got %d calls from one connection, want at most %d", n, DefaultLimits.Streams)
	}
}

func TestAtMostSoManyConnections(t *testing.T) {
	tp := startTemporal(t, nil)
	addr, logs := startProxy(t, tp)
	var open []*rawConn
	for range DefaultLimits.Connections {
		open = append(open, dialRaw(t, addr, true, true))
	}
	extra := dialRaw(t, addr, true, false)
	if extra.waitSettings(500 * time.Millisecond) {
		t.Fatalf("connection %d was served", DefaultLimits.Connections+1)
	}
	if len(logs.lines("runtime connections at the limit; new ones wait")) == 0 {
		t.Error("reaching the connection limit was not logged")
	}
	open[0].conn.Close()
	if !extra.waitSettings(5 * time.Second) {
		t.Fatal("a closed connection did not make room for a waiting one")
	}
}

func TestOnlySoManyRequestsAreReadAtOnce(t *testing.T) {
	tp := startTemporal(t, nil)
	addr, _ := startProxy(t, tp)
	c := dialRaw(t, addr, true, true)
	const size = 1 << 20
	body := make([]byte, size)
	var held []*rawStream
	sent := make(chan error, DefaultLimits.Requests+1)
	for range DefaultLimits.Requests + 1 {
		st := c.open("DescribeNamespace")
		held = append(held, st)
		go func() {
			// All but the last byte: agentd keeps waiting for the rest of the request.
			sent <- c.send(st, frame(body[:size-1], size), false, 3*time.Second)
		}()
	}
	var stalled int
	for range DefaultLimits.Requests + 1 {
		if err := <-sent; err == errStalled {
			stalled++
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if stalled != 1 {
		t.Fatalf("%d requests could not be sent, want exactly 1 (the one over the limit)", stalled)
	}
	for _, st := range held {
		if s := st.sent.Load(); s < size && s > int64(streamWindow) {
			t.Errorf("a request waiting its turn got %d bytes in, more than its %d-byte window", s, streamWindow)
		}
	}
	// One request ends: its turn goes to the waiting one.
	var done, waiting *rawStream
	for _, st := range held {
		if st.sent.Load() >= size {
			done = st
		} else {
			waiting = st
		}
	}
	if err := c.send(done, []byte{0}, true, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	rest := frame(body[:size-1], size)[waiting.sent.Load():]
	if err := c.send(waiting, rest, false, 5*time.Second); err != nil {
		t.Fatalf("the waiting request did not get its turn: %v", err)
	}
}

func TestACallsHeadersAreLimited(t *testing.T) {
	tp := startTemporal(t, nil)
	addr, _ := startProxy(t, tp)
	c := dialRaw(t, addr, true, true)
	var big []hpack.HeaderField
	for i := range 4 {
		big = append(big, hpack.HeaderField{Name: "x-pad-" + string(rune('a'+i)), Value: strings.Repeat("a", 16<<10)})
	}
	req, _ := proto.Marshal(&workflowservice.DescribeNamespaceRequest{Namespace: "default"})
	st := c.open("DescribeNamespace", big...)
	c.send(st, frame(req, len(req)), true, 5*time.Second)
	c.wait(5*time.Second, "the call to end", func() bool { return st.isReset || st.ended || c.closed != nil })
	if s := c.state(st); s.ended && s.status == "0" {
		t.Errorf("a call with 64 KiB of headers succeeded")
	}
	if n := len(tp.received()); n != 0 {
		t.Errorf("Temporal got %d calls with 64 KiB of headers", n)
	}
}

func TestACallCarriesOneRequest(t *testing.T) {
	tp := startTemporal(t, nil)
	addr, _ := startProxy(t, tp)
	c := dialRaw(t, addr, true, true)
	req, _ := proto.Marshal(&workflowservice.DescribeNamespaceRequest{Namespace: "default"})
	st := c.open("DescribeNamespace")
	// The second request is never checked: it must not reach Temporal.
	two := append(frame(req, len(req)), frame(req, len(req))...)
	if err := c.send(st, two, true, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	c.wait(5*time.Second, "the call to end", func() bool { return st.isReset || st.ended })
	if s := c.state(st); s.status == "0" {
		t.Errorf("a call with two requests succeeded")
	}
	time.Sleep(200 * time.Millisecond)
	for _, call := range tp.received() {
		if call.requests > 1 {
			t.Errorf("Temporal got %d requests on one call", call.requests)
		}
	}
}

// history answers GetWorkflowExecutionHistory with size bytes.
func history(size int) func(string, []byte) []byte {
	return func(method string, _ []byte) []byte {
		if method != "GetWorkflowExecutionHistory" {
			return nil
		}
		// A valid response: field 1 (history), length-delimited, padded with unknown bytes.
		resp, _ := proto.Marshal(&workflowservice.GetWorkflowExecutionHistoryResponse{NextPageToken: make([]byte, size)})
		return resp
	}
}

func historyRequest() []byte {
	req, _ := proto.Marshal(&workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: "default", Execution: &commonpb.WorkflowExecution{WorkflowId: "w"}})
	return frame(req, len(req))
}

// startProxyWith runs the Runtime proxy within limits l.
func startProxyWith(t *testing.T, tp *temporal, l Limits) (string, *logBuffer) {
	t.Helper()
	_, addr, logs := startServer(t, tp, l)
	return addr, logs
}

func startServer(t *testing.T, tp *temporal, l Limits) (*Server, string, *logBuffer) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	logs := &logBuffer{}
	srv := newServer(tp.cc, rules, l, slog.New(slog.NewJSONHandler(logs, nil)))
	go srv.Serve(ln)
	t.Cleanup(srv.Stop)
	return srv, ln.Addr().String(), logs
}

func TestUnreadResponsesStopFurtherOnesThenTheConnection(t *testing.T) {
	tp := startTemporal(t, history(2<<20))
	l := DefaultLimits
	l.UnreadResponses, l.SlowWorker = 1<<20, time.Second
	addr, logs := startProxyWith(t, tp, l)
	c := dialRaw(t, addr, false, true) // reads nothing
	start := time.Now()
	for range 3 {
		st := c.open("GetWorkflowExecutionHistory")
		if err := c.send(st, historyRequest(), true, 5*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	c.wait(10*time.Second, "agentd to close the connection", func() bool { return c.closed != nil })
	if d := time.Since(start); d < l.SlowWorker {
		t.Errorf("closed after %v, before the Worker had %v to read", d, l.SlowWorker)
	}
	closed := logs.waitLines(t, "runtime connection closed", 1)
	if len(closed) != 1 {
		t.Fatalf("want one closed connection in agentd's log, got %v", closed)
	}
	// One 2 MiB response was taken for the connection, not three; the client's 64 KiB window
	// let the first bytes of it through.
	if unread := closed[0]["unread_bytes"].(float64); unread < 2<<20-64<<10 || unread > 2<<20+64<<10 {
		t.Errorf("unread when closed: %v bytes, want one response's worth", unread)
	}
}

func TestAWorkerThatReadsGetsEveryResponse(t *testing.T) {
	tp := startTemporal(t, history(2<<20))
	l := DefaultLimits
	l.UnreadResponses, l.SlowWorker = 1<<20, 10*time.Second
	addr, logs := startProxyWith(t, tp, l)
	c := dialRaw(t, addr, true, true)
	var all []*rawStream
	for range 8 {
		st := c.open("GetWorkflowExecutionHistory")
		if err := c.send(st, historyRequest(), true, 5*time.Second); err != nil {
			t.Fatal(err)
		}
		all = append(all, st)
	}
	c.wait(30*time.Second, "every response", func() bool {
		for _, st := range all {
			if !st.ended {
				return false
			}
		}
		return true
	})
	for _, st := range all {
		if s := c.state(st); s.status != "0" || st.got.Load() < 2<<20 {
			t.Errorf("stream %d: status %q %q, %d bytes", st.id, s.status, s.message, st.got.Load())
		}
	}
	if closed := logs.lines("runtime connection closed"); len(closed) != 0 {
		t.Errorf("a reading Worker lost its connection: %v", closed)
	}
}

func TestAWorkerThatStallsARequestLosesItsConnection(t *testing.T) {
	tp := startTemporal(t, nil)
	l := DefaultLimits
	l.SlowWorker = time.Second
	addr, logs := startProxyWith(t, tp, l)
	c := dialRaw(t, addr, true, true)
	st := c.open("DescribeNamespace")
	if err := c.send(st, frame([]byte{0x0a}, 9), false, 5*time.Second); err != nil { // 1 of 9 bytes
		t.Fatal(err)
	}
	c.wait(10*time.Second, "agentd to close the connection", func() bool { return c.closed != nil })
	closed := logs.waitLines(t, "runtime connection closed", 1)
	if len(closed) != 1 || !strings.Contains(closed[0]["reason"].(string), "sending a request took longer than 1s") {
		t.Errorf("want the stalled request in agentd's log, got %v", closed)
	}
	if n := len(tp.received()); n != 0 {
		t.Errorf("Temporal got %d calls", n)
	}
}

func TestAWorkerThatNeverEndsItsCallLosesItsConnection(t *testing.T) {
	tp := startTemporal(t, nil)
	l := DefaultLimits
	l.SlowWorker = time.Second
	addr, logs := startProxyWith(t, tp, l)
	c := dialRaw(t, addr, true, true)
	req, _ := proto.Marshal(&workflowservice.DescribeNamespaceRequest{Namespace: "default"})
	st := c.open("DescribeNamespace")
	if err := c.send(st, frame(req, len(req)), false, 5*time.Second); err != nil { // no END_STREAM
		t.Fatal(err)
	}
	c.wait(10*time.Second, "agentd to close the connection", func() bool { return c.closed != nil })
	closed := logs.waitLines(t, "runtime connection closed", 1)
	if len(closed) != 1 || !strings.Contains(closed[0]["reason"].(string), "ending a call took longer than 1s") {
		t.Errorf("want the call left open in agentd's log, got %v", closed)
	}
}

func TestTheStreamWindowStaysFixed(t *testing.T) {
	tp := startTemporal(t, nil)
	addr, _ := startProxy(t, tp)
	c := dialRaw(t, addr, true, true)
	// Requests big enough for grpc-go's dynamic windows to grow, were they on.
	for range 4 {
		st := c.open("DescribeNamespace")
		req := completion(t, 4<<20).ProtoReflect().Interface()
		raw, _ := proto.Marshal(req)
		c.send(st, frame(raw, len(raw)), true, 5*time.Second)
		c.wait(10*time.Second, "the call to end", func() bool { return st.ended || st.isReset })
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.settings {
		if s.ID == http2.SettingInitialWindowSize && s.Val != streamWindow {
			t.Errorf("SETTINGS_INITIAL_WINDOW_SIZE %d, want %d throughout", s.Val, streamWindow)
		}
	}
}

func TestCountValues(t *testing.T) {
	md := (&workflowservice.RespondWorkflowTaskCompletedRequest{}).ProtoReflect().Descriptor()
	marshal := func(m proto.Message) []byte {
		b, err := proto.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	req := completion(t, 100)
	// namespace, task token, identity: three values.
	if err := countValues(marshal(req), md, 3); err != nil {
		t.Errorf("3 values within a limit of 3: %v", err)
	}
	if err := countValues(marshal(req), md, 2); !errors.As(err, new(valueLimitError)) {
		t.Errorf("3 values over a limit of 2: got %v", err)
	}
	// Each element of a packed list is a value: 1000 flags are 1000 values.
	req = &workflowservice.RespondWorkflowTaskCompletedRequest{SdkMetadata: &sdkpb.WorkflowTaskCompletedMetadata{
		CoreUsedFlags: make([]uint32, 1000)}}
	if err := countValues(marshal(req), md, 1000); !errors.As(err, new(valueLimitError)) {
		t.Errorf("1000 packed flags and their message over a limit of 1000: got %v", err)
	}
	if err := countValues(marshal(req), md, 1002); err != nil {
		t.Errorf("1000 packed flags, their list and their message within 1002: %v", err)
	}
	// A failure's causes nest; past maxDepth the request is refused.
	f := &failurepb.Failure{Message: "x"}
	for range maxDepth + 1 {
		f = &failurepb.Failure{Cause: f}
	}
	failed := (&workflowservice.RespondWorkflowTaskFailedRequest{}).ProtoReflect().Descriptor()
	if err := countValues(marshal(&workflowservice.RespondWorkflowTaskFailedRequest{Failure: f}), failed, 1<<20); !errors.Is(err, errTooDeep) {
		t.Errorf("causes nested %d deep: got %v", maxDepth+2, err)
	}
	if err := countValues([]byte{0x0a, 0x05, 'a'}, md, 10); err == nil {
		t.Error("a truncated request was counted")
	}
}

func TestACompressingWorkerIsCountedRight(t *testing.T) {
	tp := startTemporal(t, history(2<<20))
	l := DefaultLimits
	l.UnreadResponses, l.SlowWorker = 1<<20, 10*time.Second
	addr, logs := startProxyWith(t, tp, l)
	cc, err := grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name), grpc.MaxCallRecvMsgSize(1<<30)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	w := workflowservice.NewWorkflowServiceClient(cc)
	// Were responses compressed like the requests, far fewer bytes would reach the socket than
	// agentd took from Temporal, and agentd would see them as unread.
	var wg sync.WaitGroup
	errs := make(chan error, 6)
	for range 6 {
		wg.Go(func() {
			_, err := w.GetWorkflowExecutionHistory(ctx(t), &workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace: "default", Execution: &commonpb.WorkflowExecution{WorkflowId: "w"}})
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
	if closed := logs.lines("runtime connection closed"); len(closed) != 0 {
		t.Errorf("a reading Worker lost its connection: %v", closed)
	}
}

func TestAConnectionThatNeverStartsIsClosed(t *testing.T) {
	tp := startTemporal(t, nil)
	l := DefaultLimits
	l.SlowWorker = time.Second
	addr, _ := startProxyWith(t, tp, l)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	start := time.Now()
	if _, err := io.Copy(io.Discard, conn); err != nil { // returns when agentd closes it
		t.Fatalf("agentd kept a silent connection open: %v", err)
	}
	if d := time.Since(start); d < l.SlowWorker/2 {
		t.Errorf("closed after %v", d)
	}
}

func TestAShortCallsRequestIsLimited(t *testing.T) {
	tp := startTemporal(t, nil)
	addr, logs := startProxy(t, tp)
	w := dialWorker(t, addr)
	poll := func(size int) error {
		req := &workflowservice.PollWorkflowTaskQueueRequest{Namespace: "default",
			TaskQueue: &taskqueuepb.TaskQueue{Name: "agentos-workflows"}}
		pad := size - proto.Size(req)
		for range 3 {
			req.Identity = strings.Repeat("x", max(pad, 0))
			pad += size - proto.Size(req)
		}
		if proto.Size(req) != size {
			t.Fatalf("cannot make a %d-byte poll (got %d)", size, proto.Size(req))
		}
		_, err := w.PollWorkflowTaskQueue(ctx(t), req)
		return err
	}
	if err := poll(DefaultLimits.MaxShortRequest); err != nil {
		t.Fatalf("a poll at the limit was refused: %v", err)
	}
	err := poll(DefaultLimits.MaxShortRequest + 1)
	if status.Code(err) != codes.ResourceExhausted || !strings.HasPrefix(status.Convert(err).Message(), "grpc: received message larger than max") {
		t.Fatalf("a poll over the limit: got %v, want ResourceExhausted, received message larger than max", err)
	}
	if n := len(tp.received()); n != 1 {
		t.Errorf("Temporal got %d polls, want only the one at the limit", n)
	}
	if refused := logs.waitLines(t, "runtime call refused", 1); len(refused) != 1 {
		t.Errorf("want one refusal in agentd's log, got %v", refused)
	}
}

func TestRequestsWaitWhileTemporalHoldsTheEarlierOnes(t *testing.T) {
	hold := make(chan struct{})
	tp := startTemporal(t, func(string, []byte) []byte { <-hold; return nil })
	l := DefaultLimits
	l.ForwardBytes = 2 << 20
	addr, _ := startProxyWith(t, tp, l)
	w := dialWorker(t, addr)
	done := make(chan error, 7)
	for range 7 {
		go func() {
			_, err := w.RespondWorkflowTaskCompleted(ctx(t), completion(t, 1<<20))
			done <- err
		}()
	}
	// Two 1 MiB requests fit in the 2 MiB Temporal has not answered; the rest wait, four of
	// them read in their turns and the last one in the Worker.
	for deadline := time.Now().Add(10 * time.Second); len(tp.received()) < 2 && time.Now().Before(deadline); {
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	if n := len(tp.received()); n != 2 {
		t.Errorf("Temporal got %d requests while it held the earlier ones, want 2", n)
	}
	close(hold)
	for range 7 {
		if err := <-done; err != nil {
			t.Error(err)
		}
	}
	if n := len(tp.received()); n != 7 {
		t.Errorf("Temporal got %d requests in the end, want 7", n)
	}
}

func TestASlowResponseHoldsUpNoOtherConnection(t *testing.T) {
	tp := startTemporal(t, nil)
	// A history whose headers come at once and whose body takes 2s: the body's wait must
	// hold up no other Worker.
	tp.beforeAnswer = func(method string, ss grpc.ServerStream) {
		if method == "GetWorkflowExecutionHistory" {
			ss.SendHeader(metadata.Pairs("early", "yes"))
			time.Sleep(4 * time.Second)
		}
	}
	addr, _ := startProxy(t, tp)
	slow, quick := dialWorker(t, addr), dialWorker(t, addr)
	if _, err := quick.GetSystemInfo(ctx(t), &workflowservice.GetSystemInfoRequest{}); err != nil {
		t.Fatal(err) // connected before the clock starts
	}
	go slow.GetWorkflowExecutionHistory(ctx(t), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: "default", Execution: &commonpb.WorkflowExecution{WorkflowId: "w"}})
	time.Sleep(300 * time.Millisecond) // the slow call has its headers
	start := time.Now()
	if _, err := quick.DescribeNamespace(ctx(t), &workflowservice.DescribeNamespaceRequest{Namespace: "default"}); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("another connection's call took %v while a slow response was being taken", d)
	}
}

func TestAnIdleConnectionIsClosed(t *testing.T) {
	tp := startTemporal(t, nil)
	l := DefaultLimits
	l.IdleConnection = time.Second
	addr, _ := startProxyWith(t, tp, l)
	c := dialRaw(t, addr, true, true)
	c.wait(10*time.Second, "agentd to close an idle connection", func() bool { return c.closed != nil })
}

func TestAFloodOfRefusalsHidesNoOtherKindNorFillsTheLog(t *testing.T) {
	tp := startTemporal(t, nil)
	srv, addr, logs := startServer(t, tp, DefaultLimits)
	srv.refusals.every = 200 * time.Millisecond
	w := dialWorker(t, addr)
	floodStart := time.Now()
	for range 200 {
		if _, err := w.SignalWorkflowExecution(ctx(t), &workflowservice.SignalWorkflowExecutionRequest{}); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("a signal: got %v, want PermissionDenied", err)
		}
	}
	// After the flood, the first try of something else is logged in full.
	_, err := w.PollWorkflowTaskQueue(ctx(t), &workflowservice.PollWorkflowTaskQueueRequest{Namespace: "default",
		TaskQueue: &taskqueuepb.TaskQueue{Name: "someone-elses"}})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("a poll of another task queue: got %v, want PermissionDenied", err)
	}
	floodTook := time.Since(floodStart)
	var logged, held int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) { // the count of those held back follows within srv.refusals.every
		logged, held = len(logs.lines("runtime call refused")), 0
		for _, e := range logs.lines("runtime call refusals not logged") {
			held += int(e["count"].(float64))
		}
		if logged+held == 201 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if logged+held != 201 {
		t.Fatalf("%d refusals logged and %d counted, want 201 in all", logged, held)
	}
	// The first of each kind, a burst of 20, then one a second.
	if limit := 2 + 20 + int(floodTook.Seconds()) + 2; logged > limit {
		t.Errorf("%d of 201 refusals logged in %v, want at most %d", logged, floodTook, limit)
	}
	var polls []map[string]any
	for _, e := range logs.lines("runtime call refused") {
		if strings.HasSuffix(e["method"].(string), "/PollWorkflowTaskQueue") {
			polls = append(polls, e)
		}
	}
	if len(polls) != 1 || !strings.Contains(polls[0]["reason"].(string), "someone-elses") {
		t.Errorf("the poll of another task queue after the flood: logged %v", polls)
	}
}

func TestOnlySoManyResponsesAreTakenAtOnce(t *testing.T) {
	tp := startTemporal(t, nil)
	tp.beforeAnswer = func(method string, ss grpc.ServerStream) {
		if method == "GetWorkflowExecutionHistory" {
			ss.SendHeader(metadata.Pairs("early", "yes"))
			time.Sleep(2 * time.Second)
		}
	}
	addr, _ := startProxy(t, tp)
	// As many slow responses as are taken at once, each on its own connection...
	for range DefaultLimits.Responses {
		go dialWorker(t, addr).GetWorkflowExecutionHistory(ctx(t), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: "default", Execution: &commonpb.WorkflowExecution{WorkflowId: "w"}})
	}
	time.Sleep(300 * time.Millisecond)
	// ...and another connection's response waits for one of them: each is held in agentd
	// while it is taken, so they are few.
	start := time.Now()
	if _, err := dialWorker(t, addr).DescribeNamespace(ctx(t), &workflowservice.DescribeNamespaceRequest{Namespace: "default"}); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < time.Second {
		t.Errorf("a response was taken in %v while %d others were being taken", d, DefaultLimits.Responses)
	}
}

func TestTheBackendConnectionUsesFixedWindows(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	cc, err := grpc.NewClient(ln.Addr().String(), append(DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	cc.Connect()
	conn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(conn, make([]byte, len(http2.ClientPreface))); err != nil {
		t.Fatal(err)
	}
	fr := http2.NewFramer(nil, conn)
	var streamWin, connWin int64 = -1, 65535
	for {
		f, err := fr.ReadFrame()
		if err != nil {
			break // the deadline: the client has said what it says unprompted
		}
		switch f := f.(type) {
		case *http2.SettingsFrame:
			if v, ok := f.Value(http2.SettingInitialWindowSize); ok {
				streamWin = int64(v)
			}
		case *http2.WindowUpdateFrame:
			if f.StreamID == 0 {
				connWin += int64(f.Increment)
			}
		}
	}
	// A stream holds at most its window of a response agentd has not taken; the connection's
	// window is replenished as data arrives, so it costs nothing and keeps a remote Temporal fast.
	if streamWin != streamWindow {
		t.Errorf("stream window %d, want %d", streamWin, streamWindow)
	}
	if connWin != backendConnWindow {
		t.Errorf("connection window %d, want %d", connWin, backendConnWindow)
	}
}

func TestAWorkerThatDoesNotReadHoldsUpNoOtherRequests(t *testing.T) {
	tp := startTemporal(t, history(2<<20))
	l := DefaultLimits
	// Room for two of the history requests Temporal has not answered, no more.
	l.ForwardBytes = int64(2 * (len(historyRequest()) - 5))
	l.UnreadResponses, l.SlowWorker = 1<<20, 20*time.Second
	addr, _ := startProxyWith(t, tp, l)
	c := dialRaw(t, addr, false, true) // reads nothing
	for range 3 {
		st := c.open("GetWorkflowExecutionHistory")
		if err := c.send(st, historyRequest(), true, 5*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(500 * time.Millisecond) // two answers wait for the Worker to read the first
	// Temporal has answered them all: their requests no longer count, though their responses
	// wait.
	start := time.Now()
	if _, err := dialWorker(t, addr).DescribeNamespace(ctx(t), &workflowservice.DescribeNamespaceRequest{Namespace: "default"}); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("another Worker's request took %v while responses waited to be read", d)
	}
}

func TestWorkersThatDoNotReadHoldUpNoOtherResponses(t *testing.T) {
	tp := startTemporal(t, history(2<<20))
	l := DefaultLimits
	l.UnreadResponses, l.SlowWorker = 1<<20, 20*time.Second
	addr, _ := startProxyWith(t, tp, l)
	// As many Workers as responses are taken at once, each with a response it does not read
	// and another waiting behind it.
	for range l.Responses {
		c := dialRaw(t, addr, false, true)
		for range 2 {
			st := c.open("GetWorkflowExecutionHistory")
			if err := c.send(st, historyRequest(), true, 5*time.Second); err != nil {
				t.Fatal(err)
			}
		}
	}
	time.Sleep(500 * time.Millisecond)
	start := time.Now()
	if _, err := dialWorker(t, addr).DescribeNamespace(ctx(t), &workflowservice.DescribeNamespaceRequest{Namespace: "default"}); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("another Worker's response took %v while responses waited to be read", d)
	}
}

func TestACancelledResponseIsNotCountedAsUnread(t *testing.T) {
	tp := startTemporal(t, history(20<<20))
	l := DefaultLimits
	l.UnreadResponses, l.SlowWorker = 1<<20, 3*time.Second
	addr, logs := startProxyWith(t, tp, l)
	c := dialRaw(t, addr, true, true) // a Worker that reads
	big := c.open("GetWorkflowExecutionHistory")
	c.hold(big)
	if err := c.send(big, historyRequest(), true, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	c.wait(10*time.Second, "the start of the history", func() bool { return big.got.Load() > 0 })
	c.reset(big) // its deadline passed while it read: grpc-go drops the rest
	// The rest was never written, so it is not unread: the next response comes at once.
	req, _ := proto.Marshal(&workflowservice.DescribeNamespaceRequest{Namespace: "default"})
	next := c.open("DescribeNamespace")
	if err := c.send(next, frame(req, len(req)), true, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	c.wait(10*time.Second, "the next response", func() bool { return next.ended || c.closed != nil })
	if s := c.state(next); s.status != "0" || time.Since(start) > l.SlowWorker/2 {
		t.Errorf("the next response: status %q %q after %v", s.status, s.message, time.Since(start))
	}
	if closed := logs.lines("runtime connection closed"); len(closed) != 0 {
		t.Errorf("a Worker that cancelled one call lost its connection: %v", closed)
	}
}

func TestStreamAccountingFollowsBothSides(t *testing.T) {
	// What the Worker sends: the preface, two calls, then a reset of the second.
	var in bytes.Buffer
	in.WriteString(http2.ClientPreface)
	cfr := http2.NewFramer(&in, nil)
	cfr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: []byte("call"), EndHeaders: true})
	cfr.WriteHeaders(http2.HeadersFrameParam{StreamID: 3, BlockFragment: []byte("call"), EndHeaders: true})
	opened := in.Len()
	cfr.WriteRSTStream(3, http2.ErrCodeCancel)
	// What agentd writes: stream 1's response whole, with its gRPC prefix split across frames,
	// then its trailers; the first 1000 bytes of stream 3's.
	msg1, msg3 := frame(make([]byte, 20000), 20000), frame(make([]byte, 50000), 50000)
	var out bytes.Buffer
	sfr := http2.NewFramer(&out, nil)
	sfr.WriteSettings()
	sfr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: []byte("response"), EndHeaders: true})
	sfr.WriteData(1, false, msg1[:3])
	sfr.WriteData(1, false, msg1[3:16384])
	sfr.WriteHeaders(http2.HeadersFrameParam{StreamID: 3, BlockFragment: []byte("response"), EndHeaders: true})
	sfr.WriteData(3, false, msg3[:1000])
	sfr.WriteData(1, false, msg1[16384:])
	sfr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: []byte("trailers"), EndHeaders: true, EndStream: true})
	written := out.Len()
	sfr.WriteData(3, false, msg3[1000:2000]) // already on its way when the reset is read

	for _, chunk := range []int{1, 2, 7, 9, 10, 4096, 1 << 20} {
		var f flow
		feed := func(side func([]byte) bool, b []byte) {
			for len(b) > 0 {
				n := min(chunk, len(b))
				side(b[:n])
				b = b[n:]
			}
		}
		f.taken(int64(len(msg1)))
		f.taken(int64(len(msg3)))
		feed(f.read, in.Bytes()[:opened])
		feed(f.wrote, out.Bytes()[:written])
		if got, want := f.unread(), int64(len(msg3)-1000); got != want {
			t.Errorf("%d-byte pieces: unread %d after stream 1 ended and 1000 bytes of stream 3, want %d", chunk, got, want)
		}
		feed(f.read, in.Bytes()[opened:])
		feed(f.wrote, out.Bytes()[written:])
		if got := f.unread(); got != 0 {
			t.Errorf("%d-byte pieces: unread %d after stream 3 was reset, want 0", chunk, got)
		}
		if len(f.streams) != 0 { // a long-lived connection would otherwise keep every call
			t.Errorf("%d-byte pieces: %d streams kept after both ended", chunk, len(f.streams))
		}
	}
}

// endedStream is a call that ended before its response was handed over.
type endedStream struct{ grpc.ServerStream }

func (endedStream) SendMsg(any) error { return status.Error(codes.Canceled, "the call ended") }

func TestAResponseGrpcDoesNotTakeIsNotUnread(t *testing.T) {
	c := &conn{changed: make(chan struct{})}
	resp := &emptypb.Empty{}
	resp.ProtoReflect().SetUnknown(make([]byte, 995))
	c.queue(responseSize(resp)) // taken from Temporal; the call ends before it goes out
	if err := (&serverStream{ServerStream: endedStream{}, c: c}).SendMsg(resp); err == nil {
		t.Fatal("handed a response to an ended call")
	}
	if got := c.flow.unread(); got != 0 {
		t.Errorf("unread %d after grpc-go did not take the response, want 0", got)
	}
}

func TestMadeUpCommandTypesAreOneKindOfRefusal(t *testing.T) {
	tp := startTemporal(t, nil)
	addr, logs := startProxy(t, tp)
	w := dialWorker(t, addr)
	start := time.Now()
	// Each command type this API does not know would otherwise be a kind of its own, logged in
	// full the first time.
	for i := range 200 {
		_, err := w.RespondWorkflowTaskCompleted(ctx(t), &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: "default", Commands: []*commandpb.Command{{CommandType: enumspb.CommandType(100000 + i)}}})
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("a made-up command: got %v, want PermissionDenied", err)
		}
	}
	if logged, limit := len(logs.lines("runtime call refused")), 1+20+int(time.Since(start).Seconds())+2; logged > limit {
		t.Errorf("%d refusals logged for 200 made-up command types, want at most %d", logged, limit)
	}
}

func TestTheRefusalLogKeepsFewKinds(t *testing.T) {
	r := refusalLog{log: slog.New(slog.NewJSONHandler(io.Discard, nil)), every: time.Minute,
		lines: rate.NewLimiter(0, 0), seen: map[string]time.Time{}}
	for i := range 10 * maxKinds {
		r.refused(fmt.Sprintf("kind %d", i))
	}
	if len(r.seen) > maxKinds {
		t.Errorf("%d kinds kept, want at most %d", len(r.seen), maxKinds)
	}
	if r.held != int64(10*maxKinds-maxKinds) {
		t.Errorf("%d refusals held back, want the %d beyond the first %d kinds", r.held, 9*maxKinds, maxKinds)
	}
}
