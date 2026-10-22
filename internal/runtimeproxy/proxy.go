// Package runtimeproxy forwards the Worker's Runtime connection to Temporal and lets
// through only what a workflow-only Worker needs. Two checks run on every call:
//   - the method: signals, termination, starting workflows and every activity-task method
//     are refused, so the Worker can neither forge gateway results nor steer runs directly;
//   - the request (rules.go): its namespace and task queue, and every command inside
//     RespondWorkflowTaskCompleted, so the Worker cannot signal or cancel other workflows,
//     start child workflows or schedule activities anywhere but the gateway.
//
// Limits (limits.go) bound what a Worker can make agentd hold: requests, the calls and
// connections they arrive on, and responses the Worker does not read.
package runtimeproxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/mwitkow/grpc-proxy/proxy"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

const service = "/temporal.api.workflowservice.v1.WorkflowService/"

// AllowedMethods are the RPCs the Temporal Python SDK (1.34) makes as a workflow-only
// Worker with sticky execution off, as observed through this proxy. No activity-task method is here: the Worker runs
// no activities, and allowing PollActivityTaskQueue or RespondActivityTaskCompleted(ById)
// would let it answer gateway calls itself.
var AllowedMethods = map[string]bool{
	service + "GetSystemInfo":                true, // client connect
	service + "DescribeNamespace":            true, // worker start-up validation
	service + "PollWorkflowTaskQueue":        true,
	service + "RespondWorkflowTaskCompleted": true,
	service + "RespondWorkflowTaskFailed":    true,
	service + "RespondQueryTaskCompleted":    true, // queries, e.g. the Web UI's stack trace
	service + "GetWorkflowExecutionHistory":  true, // the rest of a long history
	service + "ShutdownWorker":               true,
	service + "RecordWorkerHeartbeat":        true,
}

// Allowed reports whether a Worker may call the gRPC method (its full name).
func Allowed(method string) bool { return AllowedMethods[method] }

// Server is the Runtime proxy.
type Server struct {
	srv     *grpc.Server
	backend grpc.ClientConnInterface
	rules   Rules
	limits  Limits
	log     *slog.Logger

	conns    chan struct{}       // a place per open connection
	requests chan struct{}       // a turn per request being read, checked and forwarded
	forward  *semaphore.Weighted // bytes of requests Temporal has not answered yet
	takes    chan struct{}       // a place per response being taken from Temporal
	refusals refusalLog
}

// NewServer returns a proxy that forwards permitted calls to backend, within DefaultLimits.
func NewServer(backend grpc.ClientConnInterface, rules Rules, log *slog.Logger) *Server {
	return newServer(backend, rules, DefaultLimits, log)
}

func newServer(backend grpc.ClientConnInterface, rules Rules, l Limits, log *slog.Logger) *Server {
	s := &Server{
		backend: backend, rules: rules, limits: l, log: log,
		conns: make(chan struct{}, l.Connections), requests: make(chan struct{}, l.Requests),
		forward: semaphore.NewWeighted(l.ForwardBytes), takes: make(chan struct{}, l.Responses),
		refusals: refusalLog{log: log, every: time.Minute, lines: rate.NewLimiter(rate.Every(time.Second), 20),
			seen: map[string]time.Time{}},
	}
	s.srv = grpc.NewServer(
		grpc.Creds(connCreds{}),
		grpc.StreamInterceptor(s.checkRequest),
		grpc.UnknownServiceHandler(proxy.TransparentHandler(s.direct)),
		grpc.MaxRecvMsgSize(l.MaxRequest),
		grpc.MaxSendMsgSize(l.MaxResponse),
		grpc.MaxConcurrentStreams(l.Streams),
		grpc.MaxHeaderListSize(l.HeaderBytes),
		grpc.StaticStreamWindowSize(streamWindow),
		// A connection that has not started HTTP/2 by then is closed.
		grpc.ConnectionTimeout(l.SlowWorker),
		grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionIdle: l.IdleConnection}),
		// The Python SDK pings every 30s. grpc-go's default policy (one ping per 5 minutes)
		// would answer with GOAWAY "too_many_pings"; this matches Temporal's frontend.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
	)
	return s
}

// Serve serves Workers on ln until Stop.
func (s *Server) Serve(ln net.Listener) error {
	return s.srv.Serve(&listener{Listener: ln, s: s, done: make(chan struct{})})
}

// Stop closes every connection and ends every call.
func (s *Server) Stop() { s.srv.Stop() }

// call is what the proxy knows about a call it lets through.
type call struct {
	c        *conn
	answered func() // gives back the request's share of Limits.ForwardBytes
}

type callKey struct{}

func (s *Server) direct(ctx context.Context, method string) (context.Context, grpc.ClientConnInterface, error) {
	cl, _ := ctx.Value(callKey{}).(*call)
	if cl == nil || !Allowed(method) { // checkRequest let nothing else through
		return nil, nil, status.Errorf(codes.Internal, "agentd: %s was not checked", logName(method))
	}
	s.log.Debug("runtime call", "method", strings.TrimPrefix(method, service))
	md, _ := metadata.FromIncomingContext(ctx)
	md = md.Copy()
	md.Delete("authorization") // the Worker has no credentials to pass on
	return metadata.NewOutgoingContext(ctx, md), &backend{s: s, call: cl}, nil
}

// checkRequest reads the call's request before anything is forwarded and refuses it with
// PermissionDenied when the rules say no. Every method a Worker may call is unary, so the
// request is a single message; it is then handed to the proxy as if read for the first time.
func (s *Server) checkRequest(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	method := info.FullMethod
	ctx := ss.Context()
	c := connFrom(ctx)
	if c == nil {
		return status.Error(codes.Internal, "agentd: runtime call without its connection")
	}
	// kind is what the refusal log tells refusals apart by: never the Worker's own values.
	refused := func(kind string, err error) {
		line := Refused{Method: logName(method), Reason: logReason(err.Error()), Count: 1}
		attrs := []any{"method", line.Method, "reason", line.Reason, "peer", peerAddr(ctx), "conn", c.name()}
		var r *Refusal
		if errors.As(err, &r) {
			kind += " " + r.Command + " " + r.rule
			if r.Command != "" {
				attrs = append(attrs, "command", r.Command)
				line.Command = r.Command
			}
		}
		s.refusals.refused(kind, line, attrs...)
	}
	newRequest, ok := requestTypes[method]
	if !Allowed(method) || !ok {
		refused("unlisted method", errors.New("not available to workers"))
		return status.Errorf(codes.PermissionDenied, "agentd: %s is not available to workers", method)
	}
	// Responses go out uncompressed, so they can be counted as they reach the socket.
	if err := grpc.SetSendCompressor(ctx, encoding.Identity); err != nil {
		return status.Errorf(codes.Internal, "agentd: %v", err)
	}

	// The request is read, checked and forwarded in its turn: at most Limits.Requests of
	// them are in agentd at once, whatever the number of calls.
	select {
	case s.requests <- struct{}{}:
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
	var once sync.Once
	done := func() { once.Do(func() { <-s.requests }) }
	defer done()

	first := &emptypb.Empty{} // the proxy's frame: the raw request rides in the unknown fields
	if err := c.within(s.limits.SlowWorker, "sending a request", func() error { return ss.RecvMsg(first) }); err != nil {
		if status.Code(err) == codes.ResourceExhausted { // over Limits.MaxRequest
			refused(method+" size", errors.New(status.Convert(err).Message()))
		}
		return err
	}
	raw := first.ProtoReflect().GetUnknown()
	if !largeRequests[method] && len(raw) > s.limits.MaxShortRequest {
		err := tooLarge(fmt.Sprintf("%d vs. %d", len(raw), s.limits.MaxShortRequest))
		refused(method+" size", errors.New(status.Convert(err).Message()))
		return err
	}
	if err := countValues(raw, newRequest().ProtoReflect().Descriptor(), s.limits.MaxRequestValues); err != nil {
		if errors.As(err, new(valueLimitError)) || errors.Is(err, errTooDeep) {
			err = tooLarge(err.Error())
			refused(method+" values", errors.New(status.Convert(err).Message()))
			return err
		}
		err = refuse("", "cannot decode the request: %v", err)
		refused(method, err)
		return status.Errorf(codes.PermissionDenied, "agentd: %v", err)
	}
	if err := s.rules.Check(method, raw); err != nil {
		refused(method, err)
		return status.Errorf(codes.PermissionDenied, "agentd: %v", err)
	}

	// Handed to the connection to Temporal, a request stays in agentd until Temporal reads
	// it: it waits for its share of Limits.ForwardBytes, which Temporal's answer gives back.
	share := min(int64(len(raw)), s.limits.ForwardBytes)
	if err := s.forward.Acquire(ctx, share); err != nil {
		return status.FromContextError(err).Err()
	}
	var answeredOnce sync.Once
	answered := func() { answeredOnce.Do(func() { s.forward.Release(share) }) }
	defer answered()
	cl := &call{c: c, answered: answered}
	return handler(srv, &serverStream{ServerStream: ss, ctx: context.WithValue(ctx, callKey{}, cl),
		first: first, c: c, slow: s.limits.SlowWorker, done: done})
}

// serverStream gives the proxy the request checkRequest already read, and ends the request's
// turn once the Worker has ended its half of the call.
type serverStream struct {
	grpc.ServerStream
	ctx   context.Context // carries the call
	first *emptypb.Empty
	c     *conn
	slow  time.Duration
	done  func()
}

func (s *serverStream) Context() context.Context { return s.ctx }

// SendMsg hands a response to grpc-go, which writes it as the Worker reads. One it does not
// take (the call has ended) will not be written: it does not count as unread.
func (s *serverStream) SendMsg(m any) error {
	err := s.ServerStream.SendMsg(m)
	if err != nil {
		s.c.queue(-responseSize(m))
	}
	return err
}

func (s *serverStream) RecvMsg(m any) error {
	if s.first == nil {
		// The request has been forwarded. Every allowed method is unary: all that may follow
		// is the end of the Worker's half of the call.
		err := s.c.within(s.slow, "ending a call", func() error { return s.ServerStream.RecvMsg(m) })
		s.done()
		if err == nil {
			return status.Error(codes.InvalidArgument, "agentd: a call carries one request")
		}
		return err
	}
	dst, ok := m.(*emptypb.Empty)
	if !ok {
		return status.Errorf(codes.Internal, "agentd: unexpected frame type %T", m)
	}
	proto.Reset(dst)
	dst.ProtoReflect().SetUnknown(s.first.ProtoReflect().GetUnknown())
	s.first = nil
	return nil
}

// backend is Temporal, as seen by one call: its response is taken only when the Worker's
// connection may hold it.
type backend struct {
	s    *Server
	call *call
}

func (b *backend) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	return status.Error(codes.Internal, "agentd: the runtime proxy streams every call")
}

func (b *backend) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	cs, err := b.s.backend.NewStream(ctx, desc, method, opts...)
	if err != nil {
		return nil, err
	}
	return &backendStream{ClientStream: cs, s: b.s, call: b.call, ctx: ctx}, nil
}

type backendStream struct {
	grpc.ClientStream
	s     *Server
	call  *call
	ctx   context.Context
	taken bool
}

// RecvMsg takes the call's response from Temporal once the Worker has read all but
// Limits.UnreadResponses of its earlier ones, one response at a time per connection and
// Limits.Responses at a time in all. Until then the response stays in Temporal but for a
// stream window. So agentd holds at most Limits.Responses responses being taken, and per
// connection Limits.UnreadResponses plus one response unread.
func (b *backendStream) RecvMsg(m any) error {
	if !b.taken {
		b.taken = true
		// Temporal sends a response's headers with the response, so a long poll waits here
		// holding no turn. No headers: the call ended with a status only. Either way Temporal
		// has read the request.
		md, _ := b.ClientStream.Header()
		b.call.answered()
		if md != nil {
			release, err := b.call.c.admit(b.ctx, b.s.limits.UnreadResponses, b.s.limits.SlowWorker, b.s.takes)
			if err != nil {
				return err
			}
			defer release()
		}
	}
	if err := b.ClientStream.RecvMsg(m); err != nil {
		return err
	}
	b.call.c.queue(responseSize(m))
	return nil
}

// responseSize is what a response, the proxy's frame, takes on the wire: gRPC's 5-byte
// message prefix, then the message.
func responseSize(m any) int64 {
	if e, ok := m.(*emptypb.Empty); ok {
		return int64(5 + len(e.ProtoReflect().GetUnknown()))
	}
	return 0
}

// DialOptions are the options for the backend connection to Temporal.
func DialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(DefaultLimits.MaxResponse), grpc.MaxCallSendMsgSize(DefaultLimits.MaxRequest)),
		// A response waiting for its turn holds at most a stream window in agentd.
		grpc.WithStaticStreamWindowSize(streamWindow),
		grpc.WithStaticConnWindowSize(backendConnWindow),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 15 * time.Second, PermitWithoutStream: true}),
	}
}

func peerAddr(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		return p.Addr.String()
	}
	return ""
}
