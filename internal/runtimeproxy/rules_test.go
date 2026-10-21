package runtimeproxy

import (
	"errors"
	"testing"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	querypb "go.temporal.io/api/query/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/encoding/protowire"
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

func prioritized(p *commonpb.Priority) *commandpb.Command {
	c := activity("Invoke", "gateway", false)
	c.GetScheduleActivityTaskCommandAttributes().Priority = p
	return c
}

func versioned() *commandpb.Command {
	c := activity("Invoke", "gateway", false)
	c.GetScheduleActivityTaskCommandAttributes().UseWorkflowBuildId = true
	return c
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
		"activity ahead of others":  prioritized(&commonpb.Priority{PriorityKey: 1}),
		"activity in a fair share":  prioritized(&commonpb.Priority{FairnessKey: "mine", FairnessWeight: 100}),
		"versioned activity":        versioned(),
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

// The Worker's requests as the Python SDK (1.34) sends them, field for field, as observed through
// this proxy.
func sdkPoll() *workflowservice.PollWorkflowTaskQueueRequest {
	return &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: "default", TaskQueue: &taskqueuepb.TaskQueue{Name: "agentos-workflows", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity: "1234@host", WorkerInstanceKey: "key", WorkerVersionCapabilities: &commonpb.WorkerVersionCapabilities{BuildId: "b"},
	}
}

func sdkCompletion(cmds ...*commandpb.Command) *workflowservice.RespondWorkflowTaskCompletedRequest {
	return &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: "default", TaskToken: []byte("token"), Commands: cmds, Identity: "1234@host", WorkerInstanceKey: "key",
		WorkerVersionStamp: &commonpb.WorkerVersionStamp{BuildId: "b"},
		SdkMetadata:        &sdkpb.WorkflowTaskCompletedMetadata{CoreUsedFlags: []uint32{1}, SdkName: "temporal-python", SdkVersion: "1.34.0"},
		MeteringMetadata:   &commonpb.MeteringMetadata{},
		Capabilities:       &workflowservice.RespondWorkflowTaskCompletedRequest_Capabilities{DiscardSpeculativeWorkflowTaskWithEvents: true},
	}
}

func sdkFailure() *workflowservice.RespondWorkflowTaskFailedRequest {
	return &workflowservice.RespondWorkflowTaskFailedRequest{
		Namespace: "default", TaskToken: []byte("token"), Cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
		Failure: &failurepb.Failure{Message: "KeyError: 'blob'"}, Identity: "1234@host",
		WorkerVersion: &commonpb.WorkerVersionStamp{BuildId: "b"},
	}
}

// sdkHeartbeat is a heartbeat as the SDK fills it in: its version is a build ID.
func sdkHeartbeat() *workerpb.WorkerHeartbeat {
	return &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: "key", WorkerIdentity: "1234@host", HostInfo: &workerpb.WorkerHostInfo{HostName: "host"},
		TaskQueue: "agentos-workflows", DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{BuildId: "b"},
		SdkName: "temporal-python", SdkVersion: "1.34.0", Status: enumspb.WORKER_STATUS_RUNNING,
		WorkflowTaskSlotsInfo: &workerpb.WorkerSlotsInfo{}, WorkflowPollerInfo: &workerpb.WorkerPollerInfo{},
		Environment: &workerpb.EnvironmentInfo{},
	}
}

func sdkShutdown() *workflowservice.ShutdownWorkerRequest {
	return &workflowservice.ShutdownWorkerRequest{
		Namespace: "default", Identity: "1234@host", Reason: "graceful shutdown", WorkerHeartbeat: sdkHeartbeat(),
		WorkerInstanceKey: "key", TaskQueue: "agentos-workflows", TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
	}
}

func TestTheSDKsOwnRequestsAreAllowed(t *testing.T) {
	for method, req := range map[string]proto.Message{
		"GetSystemInfo":                &workflowservice.GetSystemInfoRequest{},
		"DescribeNamespace":            &workflowservice.DescribeNamespaceRequest{Namespace: "default"},
		"PollWorkflowTaskQueue":        sdkPoll(),
		"RespondWorkflowTaskCompleted": sdkCompletion(prioritized(&commonpb.Priority{})),
		"RespondWorkflowTaskFailed":    sdkFailure(),
		"ShutdownWorker":               sdkShutdown(),
		"RecordWorkerHeartbeat": &workflowservice.RecordWorkerHeartbeatRequest{Namespace: "default", Identity: "1234@host",
			WorkerHeartbeat: []*workerpb.WorkerHeartbeat{sdkHeartbeat()}},
		"GetWorkflowExecutionHistory": &workflowservice.GetWorkflowExecutionHistoryRequest{Namespace: "default",
			Execution: &commonpb.WorkflowExecution{WorkflowId: "run"}, MaximumPageSize: 100, NextPageToken: []byte("t")},
	} {
		if err := check(t, method, req); err != nil {
			t.Errorf("%s as the SDK sends it was refused: %v", method, err)
		}
	}
}

func TestFieldsTheSDKNeverSetsAreRefused(t *testing.T) {
	versioned := &deploymentpb.WorkerDeploymentOptions{DeploymentName: "t9-rogue", BuildId: "b1",
		WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED}
	type req struct {
		method string
		msg    proto.Message
	}
	cases := map[string]req{}
	poll := func(name string, change func(*workflowservice.PollWorkflowTaskQueueRequest)) {
		m := sdkPoll()
		change(m)
		cases["poll "+name] = req{"PollWorkflowTaskQueue", m}
	}
	completion := func(name string, change func(*workflowservice.RespondWorkflowTaskCompletedRequest)) {
		m := sdkCompletion(activity("Invoke", "gateway", false))
		change(m)
		cases["completion "+name] = req{"RespondWorkflowTaskCompleted", m}
	}
	failure := func(name string, change func(*workflowservice.RespondWorkflowTaskFailedRequest)) {
		m := sdkFailure()
		change(m)
		cases["failure "+name] = req{"RespondWorkflowTaskFailed", m}
	}
	// What registers a Worker Deployment, and starts its system workflows, under a name the Worker picks.
	poll("with deployment options", func(m *workflowservice.PollWorkflowTaskQueueRequest) { m.DeploymentOptions = versioned })
	poll("using versioning", func(m *workflowservice.PollWorkflowTaskQueueRequest) {
		m.WorkerVersionCapabilities.UseVersioning = true
	})
	poll("naming a deployment series", func(m *workflowservice.PollWorkflowTaskQueueRequest) {
		m.WorkerVersionCapabilities.DeploymentSeriesName = "t9-rogue"
	})
	poll("naming a control queue", func(m *workflowservice.PollWorkflowTaskQueueRequest) { m.WorkerControlTaskQueue = "gateway" })
	poll("with a binary checksum", func(m *workflowservice.PollWorkflowTaskQueueRequest) { m.BinaryChecksum = "c" })
	completion("with deployment options", func(m *workflowservice.RespondWorkflowTaskCompletedRequest) { m.DeploymentOptions = versioned })
	completion("with a deployment", func(m *workflowservice.RespondWorkflowTaskCompletedRequest) {
		m.Deployment = &deploymentpb.Deployment{SeriesName: "t9-rogue", BuildId: "b1"}
	})
	completion("with a versioning behavior", func(m *workflowservice.RespondWorkflowTaskCompletedRequest) {
		m.VersioningBehavior = enumspb.VERSIONING_BEHAVIOR_PINNED
	})
	completion("using versioning", func(m *workflowservice.RespondWorkflowTaskCompletedRequest) {
		m.WorkerVersionStamp.UseVersioning = true
	})
	completion("naming a control queue", func(m *workflowservice.RespondWorkflowTaskCompletedRequest) {
		m.WorkerControlTaskQueue = "gateway"
	})
	completion("forcing a new workflow task", func(m *workflowservice.RespondWorkflowTaskCompletedRequest) {
		m.ForceCreateNewWorkflowTask = true
	})
	failure("with deployment options", func(m *workflowservice.RespondWorkflowTaskFailedRequest) { m.DeploymentOptions = versioned })
	failure("using versioning", func(m *workflowservice.RespondWorkflowTaskFailedRequest) { m.WorkerVersion.UseVersioning = true })
	other := sdkShutdown()
	other.TaskQueue = "gateway"
	cases["shutdown of another task queue"] = req{"ShutdownWorker", other}
	// A heartbeat that names a Worker Deployment, in either request that carries one.
	claiming := sdkHeartbeat()
	claiming.DeploymentVersion.DeploymentName = "t9-rogue"
	cases["heartbeat naming a deployment"] = req{"RecordWorkerHeartbeat",
		&workflowservice.RecordWorkerHeartbeatRequest{Namespace: "default", WorkerHeartbeat: []*workerpb.WorkerHeartbeat{sdkHeartbeat(), claiming}}}
	lastBeat := sdkShutdown()
	lastBeat.WorkerHeartbeat = claiming
	cases["shutdown heartbeat naming a deployment"] = req{"ShutdownWorker", lastBeat}
	cases["namespace by id"] = req{"DescribeNamespace", &workflowservice.DescribeNamespaceRequest{Namespace: "default", Id: "x"}}

	for name, c := range cases {
		if err := check(t, c.method, c.msg); err == nil {
			t.Errorf("%s: allowed", name)
		}
	}
}

func TestFieldsThisAgentdDoesNotKnowAreRefused(t *testing.T) {
	unknown := func(raw []byte) []byte { // field 9999, a varint: no version of the API names it
		return protowire.AppendVarint(protowire.AppendTag(raw, 9999, protowire.VarintType), 1)
	}
	raw, err := proto.Marshal(sdkPoll())
	if err != nil {
		t.Fatal(err)
	}
	if err := rules.Check(service+"PollWorkflowTaskQueue", unknown(raw)); err == nil {
		t.Error("a poll with a field agentd does not know was allowed")
	}
	// Deep inside: one command's attributes.
	cmdRaw, err := proto.Marshal(activity("Invoke", "gateway", false).GetScheduleActivityTaskCommandAttributes())
	if err != nil {
		t.Fatal(err)
	}
	attrs := &commandpb.ScheduleActivityTaskCommandAttributes{}
	if err := proto.Unmarshal(unknown(cmdRaw), attrs); err != nil {
		t.Fatal(err)
	}
	cmd := &commandpb.Command{CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
		Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: attrs}}
	if err := check(t, "RespondWorkflowTaskCompleted", sdkCompletion(cmd)); err == nil {
		t.Error("a command with a field agentd does not know was allowed")
	}
	// Inside a map's values: a query's result, and a payload in an activity's header.
	resultRaw, err := proto.Marshal(&querypb.WorkflowQueryResult{ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED})
	if err != nil {
		t.Fatal(err)
	}
	result := &querypb.WorkflowQueryResult{}
	if err := proto.Unmarshal(unknown(resultRaw), result); err != nil {
		t.Fatal(err)
	}
	answered := sdkCompletion()
	answered.QueryResults = map[string]*querypb.WorkflowQueryResult{"q1": result}
	if err := check(t, "RespondWorkflowTaskCompleted", answered); err == nil {
		t.Error("a query result with a field agentd does not know was allowed")
	}
	payload := &commonpb.Payload{}
	if err := proto.Unmarshal(unknown(nil), payload); err != nil {
		t.Fatal(err)
	}
	headed := activity("Invoke", "gateway", false)
	headed.GetScheduleActivityTaskCommandAttributes().Header = &commonpb.Header{Fields: map[string]*commonpb.Payload{"h": payload}}
	if err := check(t, "RespondWorkflowTaskCompleted", sdkCompletion(headed)); err == nil {
		t.Error("a header payload with a field agentd does not know was allowed")
	}
}
