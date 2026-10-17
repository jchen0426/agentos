package runtimeproxy

import (
	"errors"
	"testing"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"
)

var rules = Rules{
	Namespace: "default", WorkflowQueue: "agentos-workflows", GatewayQueue: "gateway",
	ActivityType: "Invoke", SearchAttributes: []string{"AgentState", "TemporalChangeVersion"},
}

func check(t *testing.T, method string, req proto.Message) error {
	t.Helper()
	raw, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	return rules.Check(service+method, raw)
}

func TestEveryAllowedMethodHasARequestType(t *testing.T) {
	for m := range AllowedMethods {
		if _, ok := requestTypes[m]; !ok {
			t.Errorf("%s is allowed but its request is never checked", m)
		}
	}
	for m := range requestTypes {
		if !AllowedMethods[m] {
			t.Errorf("%s has a request type but is not allowed", m)
		}
	}
}

func TestNamespaceIsPinned(t *testing.T) {
	if err := check(t, "DescribeNamespace", &workflowservice.DescribeNamespaceRequest{Namespace: "default"}); err != nil {
		t.Errorf("default namespace refused: %v", err)
	}
	for _, req := range []proto.Message{
		&workflowservice.DescribeNamespaceRequest{Namespace: "temporal-system"},
		&workflowservice.DescribeNamespaceRequest{Id: "some-namespace-id"},
		&workflowservice.GetWorkflowExecutionHistoryRequest{Namespace: "temporal-system"},
		&workflowservice.RespondWorkflowTaskCompletedRequest{Namespace: "other"},
	} {
		method := map[string]string{
			"temporal.api.workflowservice.v1.DescribeNamespaceRequest":            "DescribeNamespace",
			"temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryRequest":  "GetWorkflowExecutionHistory",
			"temporal.api.workflowservice.v1.RespondWorkflowTaskCompletedRequest": "RespondWorkflowTaskCompleted",
		}[string(req.ProtoReflect().Descriptor().FullName())]
		if err := check(t, method, req); err == nil {
			t.Errorf("%s %v: allowed", method, req)
		}
	}
}

func TestPollOnlyTheWorkflowQueue(t *testing.T) {
	poll := func(tq *taskqueuepb.TaskQueue) error {
		return check(t, "PollWorkflowTaskQueue", &workflowservice.PollWorkflowTaskQueueRequest{Namespace: "default", TaskQueue: tq})
	}
	allowed := []*taskqueuepb.TaskQueue{
		{Name: "agentos-workflows", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		{Name: "agentos-workflows"},
	}
	refused := []*taskqueuepb.TaskQueue{
		{Name: "gateway", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		{Name: "temporal-sys-batcher-taskqueue"},
		// Sticky queues are refused whatever they claim to belong to.
		{Name: "worker-sticky-1", Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: "agentos-workflows"},
		{Name: "worker-sticky-1", Kind: enumspb.TASK_QUEUE_KIND_STICKY},
		{Name: "agentos-workflows", Kind: enumspb.TASK_QUEUE_KIND_STICKY},
	}
	for _, tq := range allowed {
		if err := poll(tq); err != nil {
			t.Errorf("%v refused: %v", tq, err)
		}
	}
	for _, tq := range refused {
		if err := poll(tq); err == nil {
			t.Errorf("%v allowed", tq)
		}
	}
}

func TestNoStickyExecution(t *testing.T) {
	sticky := complete(activity("Invoke", "gateway", false))
	sticky.StickyAttributes = &taskqueuepb.StickyExecutionAttributes{
		WorkerTaskQueue: &taskqueuepb.TaskQueue{Name: "worker-sticky-1", Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: "agentos-workflows"},
	}
	if err := check(t, "RespondWorkflowTaskCompleted", sticky); err == nil {
		t.Error("a workflow task completion asking for a sticky queue was allowed")
	}
	if err := check(t, "ShutdownWorker", &workflowservice.ShutdownWorkerRequest{Namespace: "default"}); err != nil {
		t.Errorf("worker shutdown refused: %v", err)
	}
	if err := check(t, "ShutdownWorker", &workflowservice.ShutdownWorkerRequest{Namespace: "default", StickyTaskQueue: "worker-sticky-1"}); err == nil {
		t.Error("shutting down a sticky queue was allowed")
	}
	if err := check(t, "ResetStickyTaskQueue", &workflowservice.ResetStickyTaskQueueRequest{Namespace: "default"}); err == nil {
		t.Error("ResetStickyTaskQueue was allowed")
	}
}

func complete(cmds ...*commandpb.Command) *workflowservice.RespondWorkflowTaskCompletedRequest {
	return &workflowservice.RespondWorkflowTaskCompletedRequest{Namespace: "default", Commands: cmds}
}

func activity(activityType, queue string, eager bool) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
		Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
			ActivityId: "1:git.show_diff", ActivityType: &commonpb.ActivityType{Name: activityType},
			TaskQueue: &taskqueuepb.TaskQueue{Name: queue}, RequestEagerExecution: eager,
		}},
	}
}

func upsert(keys ...string) *commandpb.Command {
	fields := map[string]*commonpb.Payload{}
	for _, k := range keys {
		fields[k] = &commonpb.Payload{Data: []byte(`"RUNNING"`)}
	}
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
		Attributes: &commandpb.Command_UpsertWorkflowSearchAttributesCommandAttributes{UpsertWorkflowSearchAttributesCommandAttributes: &commandpb.UpsertWorkflowSearchAttributesCommandAttributes{
			SearchAttributes: &commonpb.SearchAttributes{IndexedFields: fields},
		}},
	}
}

func marker(name string) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
		Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
			MarkerName: name,
		}},
	}
}

func bare(t enumspb.CommandType) *commandpb.Command { return &commandpb.Command{CommandType: t} }

func TestWhatAWorkflowTaskMayCommand(t *testing.T) {
	// What the security-audit workflow sends on its way.
	ok := complete(
		activity("Invoke", "gateway", false), upsert("AgentState"), upsert("TemporalChangeVersion"), marker("core_patch"),
		bare(enumspb.COMMAND_TYPE_START_TIMER), bare(enumspb.COMMAND_TYPE_CANCEL_TIMER),
		bare(enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK), bare(enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION),
		bare(enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION), bare(enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION),
	)
	if err := check(t, "RespondWorkflowTaskCompleted", ok); err != nil {
		t.Fatalf("the workflow's own commands were refused: %v", err)
	}

	for name, cmd := range map[string]*commandpb.Command{
		"signal another workflow":   bare(enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION),
		"cancel another workflow":   bare(enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION),
		"start a child workflow":    bare(enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION),
		"continue as new":           bare(enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION),
		"start a Nexus operation":   bare(enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION),
		"change workflow memo":      bare(enumspb.COMMAND_TYPE_MODIFY_WORKFLOW_PROPERTIES),
		"activity on another queue": activity("Invoke", "agentos-workflows", false),
		"another activity type":     activity("Steal", "gateway", false),
		"eager gateway activity":    activity("Invoke", "gateway", true),
		"another search attribute":  upsert("AgentState", "BuildIds"),
		"a marker of its own":       marker("anything"),
	} {
		err := check(t, "RespondWorkflowTaskCompleted", complete(activity("Invoke", "gateway", false), cmd))
		var r *Refusal
		if !errors.As(err, &r) || r.Command != cmd.GetCommandType().String() {
			t.Errorf("%s: got %v, want a refusal naming %s", name, err, cmd.GetCommandType())
		}
	}

	withMessages := complete()
	withMessages.Messages = append(withMessages.Messages, nil)
	if err := check(t, "RespondWorkflowTaskCompleted", withMessages); err == nil {
		t.Error("protocol messages were allowed")
	}
}
