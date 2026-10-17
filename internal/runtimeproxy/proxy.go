// Package runtimeproxy forwards the Worker's Runtime connection to Temporal and lets
// through only what a workflow-only Worker needs. Two checks run on every call:
//   - the method: signals, termination, starting workflows and every activity-task method
//     are refused, so the Worker can neither forge gateway results nor steer runs directly;
//   - the request (rules.go): its namespace and task queue, and every command inside
//     RespondWorkflowTaskCompleted, so the Worker cannot signal or cancel other workflows,
//     start child workflows or schedule activities anywhere but the gateway.
package runtimeproxy

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/mwitkow/grpc-proxy/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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

// MaxMessageSize matches what Temporal's own clients accept (128 MiB). grpc-go's 4 MiB
// default would break large histories.
const MaxMessageSize = 128 << 20

// NewServer returns a gRPC server that forwards permitted calls to backend.
func NewServer(backend grpc.ClientConnInterface, rules Rules, log *slog.Logger) *grpc.Server {
	director := func(ctx context.Context, method string) (context.Context, grpc.ClientConnInterface, error) {
		if !Allowed(method) {
			log.Warn("runtime call refused", "method", method, "peer", peerAddr(ctx))
			return nil, nil, status.Errorf(codes.PermissionDenied, "agentd: %s is not available to workers", method)
		}
		log.Debug("runtime call", "method", strings.TrimPrefix(method, service))
		md, _ := metadata.FromIncomingContext(ctx)
		md = md.Copy()
		md.Delete("authorization") // the Worker has no credentials to pass on
		return metadata.NewOutgoingContext(ctx, md), backend, nil
	}
	return grpc.NewServer(
		grpc.StreamInterceptor(checkRequest(rules, log)),
		grpc.UnknownServiceHandler(proxy.TransparentHandler(director)),
		grpc.MaxRecvMsgSize(MaxMessageSize),
		grpc.MaxSendMsgSize(MaxMessageSize),
		// The Python SDK pings every 30s. grpc-go's default policy (one ping per 5 minutes)
		// would answer with GOAWAY "too_many_pings"; this matches Temporal's frontend.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
	)
}

// checkRequest reads the call's request before anything is forwarded and refuses it with
// PermissionDenied when the rules say no. Every method a Worker may call is unary, so the
// request is a single message; it is then handed to the proxy as if read for the first time.
func checkRequest(rules Rules, log *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		method := info.FullMethod
		refused := func(err error) error {
			attrs := []any{"method", method, "reason", err.Error(), "peer", peerAddr(ss.Context())}
			var r *Refusal
			if errors.As(err, &r) && r.Command != "" {
				attrs = append(attrs, "command", r.Command)
			}
			log.Warn("runtime call refused", attrs...)
			return status.Errorf(codes.PermissionDenied, "agentd: %v", err)
		}
		if !Allowed(method) {
			return refused(refuse("", "%s is not available to workers", method))
		}
		first := &emptypb.Empty{} // the proxy's frame: the raw request rides in the unknown fields
		if err := ss.RecvMsg(first); err != nil {
			return err
		}
		if err := rules.Check(method, first.ProtoReflect().GetUnknown()); err != nil {
			return refused(err)
		}
		return handler(srv, &replayFirst{ServerStream: ss, first: first})
	}
}

// replayFirst gives the proxy the request checkRequest already read.
type replayFirst struct {
	grpc.ServerStream
	first *emptypb.Empty
}

func (s *replayFirst) RecvMsg(m any) error {
	if s.first == nil {
		return s.ServerStream.RecvMsg(m)
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

// DialOptions are the options for the backend connection to Temporal.
func DialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxMessageSize), grpc.MaxCallSendMsgSize(MaxMessageSize)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 15 * time.Second, PermitWithoutStream: true}),
	}
}

func peerAddr(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		return p.Addr.String()
	}
	return ""
}
