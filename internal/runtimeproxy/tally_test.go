package runtimeproxy

import (
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func poll(queue string) *workflowservice.PollWorkflowTaskQueueRequest {
	return &workflowservice.PollWorkflowTaskQueueRequest{Namespace: "default",
		TaskQueue: &taskqueuepb.TaskQueue{Name: queue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}}
}

// handingOut answers a poll with a task when task is set, and with the empty response of a long poll
// that found nothing otherwise; every other call, with nothing.
func handingOut(task *bool) func(string, []byte) []byte {
	return func(method string, _ []byte) []byte {
		if method != "PollWorkflowTaskQueue" || !*task {
			return nil
		}
		b, _ := proto.Marshal(&workflowservice.PollWorkflowTaskQueueResponse{TaskToken: []byte("token"), Attempt: 1})
		return b
	}
}

// A poll the rules let through counts while it is open; when it ends, the quiet begins. Its empty
// response (a long poll that found nothing) hands nothing out; a task does, until the Worker answers,
// even with an answer the rules refuse.
func TestTheTallyOfPollsHandOutsAndAnswers(t *testing.T) {
	task := false
	tp := startTemporal(t, handingOut(&task))
	srv, addr, _ := startServer(t, tp, DefaultLimits)
	tally := srv.NewTally()
	began := time.Now()
	w := dialWorker(t, addr)

	if _, err := w.PollWorkflowTaskQueue(ctx(t), poll("agentos-workflows")); err != nil {
		t.Fatal(err)
	}
	polls, quiet, held := tally.Liveness()
	if polls != 0 || !quiet.After(began) || !held.IsZero() {
		t.Errorf("after an empty poll: %d polls, quiet since %s (began %s), held since %s", polls, quiet, began, held)
	}
	task = true
	if _, err := w.PollWorkflowTaskQueue(ctx(t), poll("agentos-workflows")); err != nil {
		t.Fatal(err)
	}
	if _, _, held = tally.Liveness(); held.IsZero() {
		t.Fatal("a task handed out holds nothing")
	}
	first := held
	if _, err := w.PollWorkflowTaskQueue(ctx(t), poll("agentos-workflows")); err != nil {
		t.Fatal(err)
	}
	if _, _, held = tally.Liveness(); !held.Equal(first) {
		t.Errorf("held since %s, then %s: the wait is the first task's", first, held)
	}
	// Refused (another namespace), still an answer.
	_, err := w.RespondWorkflowTaskCompleted(ctx(t), &workflowservice.RespondWorkflowTaskCompletedRequest{Namespace: "other",
		TaskToken: []byte("token")})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("the completion: %v", err)
	}
	if _, _, held = tally.Liveness(); !held.IsZero() {
		t.Errorf("held since %s after an answer", held)
	}
}

// A poll the rules refuse counts for nothing; a poll held open by Temporal counts until it ends.
func TestTheTallyCountsOnlyPollsLetThrough(t *testing.T) {
	release := make(chan struct{})
	tp := startTemporal(t, func(method string, _ []byte) []byte {
		if method == "PollWorkflowTaskQueue" {
			<-release
		}
		return nil
	})
	srv, addr, _ := startServer(t, tp, DefaultLimits)
	tally := srv.NewTally()
	_, begun, _ := tally.Liveness()
	w := dialWorker(t, addr)

	if _, err := w.PollWorkflowTaskQueue(ctx(t), poll("someone-elses")); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("a poll of another queue: %v", err)
	}
	if polls, quiet, _ := tally.Liveness(); polls != 0 || !quiet.Equal(begun) {
		t.Errorf("after a refused poll: %d polls, quiet since %s, not %s", polls, quiet, begun)
	}
	done := make(chan error, 1)
	go func() {
		_, err := w.PollWorkflowTaskQueue(ctx(t), poll("agentos-workflows"))
		done <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for polls, _, _ := tally.Liveness(); polls != 1; polls, _, _ = tally.Liveness() {
		if time.Now().After(deadline) {
			t.Fatalf("%d polls open, want 1", polls)
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if polls, quiet, _ := tally.Liveness(); polls != 0 || !quiet.After(begun) {
		t.Errorf("after the poll ended: %d polls, quiet since %s", polls, quiet)
	}
}

// A connection counts into the tally of the generation it came from: the latest tally when the proxy
// accepted it. One accepted before a new tally counts into the old one, whatever it does after.
func TestAConnectionCountsIntoTheTallyBeforeIt(t *testing.T) {
	task := true
	tp := startTemporal(t, handingOut(&task))
	srv, addr, _ := startServer(t, tp, DefaultLimits)
	older := srv.NewTally()
	old := dialWorker(t, addr)
	if _, err := old.PollWorkflowTaskQueue(ctx(t), poll("agentos-workflows")); err != nil { // connects it
		t.Fatal(err)
	}
	newer := srv.NewTally()
	if _, err := old.PollWorkflowTaskQueue(ctx(t), poll("agentos-workflows")); err != nil {
		t.Fatal(err)
	}
	if _, quiet, held := newer.Liveness(); !held.IsZero() {
		t.Errorf("an older connection's task counts in the newer tally (held since %s, quiet since %s)", held, quiet)
	}
	if _, _, held := older.Liveness(); held.IsZero() {
		t.Error("the older tally lost its connection's task")
	}
	if _, err := dialWorker(t, addr).PollWorkflowTaskQueue(ctx(t), poll("agentos-workflows")); err != nil {
		t.Fatal(err)
	}
	if _, _, held := newer.Liveness(); held.IsZero() {
		t.Error("a connection accepted after the newer tally does not count in it")
	}
}

func TestHandsOut(t *testing.T) {
	for _, c := range []struct {
		resp proto.Message
		want bool
	}{
		{&workflowservice.PollWorkflowTaskQueueResponse{}, false},
		{&workflowservice.PollWorkflowTaskQueueResponse{Attempt: 3, BacklogCountHint: 9}, false},
		{&workflowservice.PollWorkflowTaskQueueResponse{TaskToken: []byte("t")}, true},
		{&workflowservice.PollWorkflowTaskQueueResponse{Attempt: 1, TaskToken: []byte("t")}, true},
	} {
		raw, err := proto.Marshal(c.resp)
		if err != nil {
			t.Fatal(err)
		}
		if got := handsOut(raw); got != c.want {
			t.Errorf("%v: %v, want %v", c.resp, got, c.want)
		}
	}
	if handsOut([]byte{0x0a, 0x05, 'x'}) { // a token cut short
		t.Error("a broken response hands out a task")
	}
}
