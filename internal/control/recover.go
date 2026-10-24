package control

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ServerOptions are the Control server's options: the peer's credentials (PeerCredentials), and the
// recovery of a panic in any of its calls (RecoverUnary, RecoverStream).
func ServerOptions(log *slog.Logger) []grpc.ServerOption {
	return []grpc.ServerOption{grpc.Creds(PeerCredentials()), grpc.ChainUnaryInterceptor(RecoverUnary(log)),
		grpc.ChainStreamInterceptor(RecoverStream(log))}
}

// RecoverUnary keeps agentd up through a panic in a Control call: what the call reads (a history the
// Worker shaped, the ledger) must not take down the process that serves every run's gateway calls.
// The panic and its stack go to the log, and the caller gets Internal.
func RecoverUnary(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if p := recover(); p != nil {
				resp, err = nil, panicked(log, info.FullMethod, p)
			}
		}()
		return handler(ctx, req)
	}
}

// RecoverStream is RecoverUnary for a streaming call (ExportAudit).
func RecoverStream(log *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if p := recover(); p != nil {
				err = panicked(log, info.FullMethod, p)
			}
		}()
		return handler(srv, ss)
	}
}

func panicked(log *slog.Logger, method string, p any) error {
	log.Error("a Control call panicked", "method", method, "panic", fmt.Sprint(p), "stack", string(debug.Stack()))
	return status.Error(codes.Internal, "agentd failed to serve the call; its log says why")
}
