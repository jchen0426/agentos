package control

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/tasks"
)

// A start whose parameters are refused is an invalid argument; one whose parameters could not
// be checked (the git tool server failed) is unavailable, and may work later.
func TestStartRunSaysWhyResolveFailed(t *testing.T) {
	for err, want := range map[error]codes.Code{
		errors.New("commit abc1234 not found in the mirror"):                                codes.InvalidArgument,
		&tasks.Unavailable{Err: errors.New("the git tool server failed on resolve_commit")}: codes.Unavailable,
	} {
		s := &Server{Tasks: tasks.Registry{"t": {Name: "t", Resolve: func(context.Context, map[string]string) (any, map[string]string, error) {
			return nil, nil, err
		}}}}
		_, got := s.StartRun(context.Background(), &agentosv1.StartRunRequest{Task: "t"})
		if status.Code(got) != want || status.Convert(got).Message() != err.Error() {
			t.Errorf("%v: got %v, want %v", err, got, want)
		}
	}
}
