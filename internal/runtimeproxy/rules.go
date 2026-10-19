package runtimeproxy

import (
	"fmt"
	"slices"

	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Rules pin what a permitted call may carry. A method allowlist alone is not enough: the
// RespondWorkflowTaskCompleted a Worker must send can carry commands that signal or cancel
// other workflows, start child workflows (which may reuse another run's workflow ID) or
// schedule activities anywhere.
type Rules struct {
	Namespace        string   // the only namespace a Worker may name
	WorkflowQueue    string   // the task queue it polls for workflow tasks
	GatewayQueue     string   // the only task queue it may schedule activities on
	ActivityType     string   // the only activity type it may schedule (Invoke)
	SearchAttributes []string // the only search attributes it may upsert
}

// requestTypes maps each allowed method to its request message.
var requestTypes = map[string]func() proto.Message{
	service + "GetSystemInfo":                func() proto.Message { return &workflowservice.GetSystemInfoRequest{} },
	service + "DescribeNamespace":            func() proto.Message { return &workflowservice.DescribeNamespaceRequest{} },
	service + "PollWorkflowTaskQueue":        func() proto.Message { return &workflowservice.PollWorkflowTaskQueueRequest{} },
	service + "RespondWorkflowTaskCompleted": func() proto.Message { return &workflowservice.RespondWorkflowTaskCompletedRequest{} },
	service + "RespondWorkflowTaskFailed":    func() proto.Message { return &workflowservice.RespondWorkflowTaskFailedRequest{} },
	service + "RespondQueryTaskCompleted":    func() proto.Message { return &workflowservice.RespondQueryTaskCompletedRequest{} },
	service + "GetWorkflowExecutionHistory":  func() proto.Message { return &workflowservice.GetWorkflowExecutionHistoryRequest{} },
	service + "ShutdownWorker":               func() proto.Message { return &workflowservice.ShutdownWorkerRequest{} },
	service + "RecordWorkerHeartbeat":        func() proto.Message { return &workflowservice.RecordWorkerHeartbeatRequest{} },
}

// Refusal says why a request was refused.
type Refusal struct {
	Reason  string
	Command string // the command type, when a command was refused
	rule    string // which check refused it: the reason without its values
}

func (r *Refusal) Error() string { return r.Reason }

func refuse(command, format string, args ...any) *Refusal {
	return &Refusal{Reason: fmt.Sprintf(format, args...), Command: command, rule: format}
}

// Check decodes the raw request of method and returns a *Refusal when it is not allowed.
func (r Rules) Check(method string, raw []byte) error {
	newRequest, ok := requestTypes[method]
	if !ok {
		return refuse("", "%s is not available to workers", method)
	}
	req := newRequest()
	if err := proto.Unmarshal(raw, req); err != nil {
		return refuse("", "cannot decode the request: %v", err)
	}
	if ns, ok := namespaceOf(req); ok && ns != r.Namespace {
		return refuse("", "namespace %q is not available to workers", ns)
	}
	switch m := req.(type) {
	case *workflowservice.PollWorkflowTaskQueueRequest:
		return r.checkPollQueue(m.GetTaskQueue())
	case *workflowservice.RespondWorkflowTaskCompletedRequest:
		if m.GetStickyAttributes() != nil {
			return refuse("", "sticky execution is not available to workers")
		}
		for _, c := range m.GetCommands() {
			if err := r.checkCommand(c); err != nil {
				return err
			}
		}
		if len(m.GetMessages()) > 0 {
			return refuse("", "protocol messages (workflow updates) are not available to workers")
		}
	case *workflowservice.ShutdownWorkerRequest:
		if q := m.GetStickyTaskQueue(); q != "" {
			return refuse("", "sticky queue %q is not available to workers", q)
		}
	}
	return nil
}

// checkPollQueue lets a Worker poll the workflow queue and nothing else. Sticky queues are
// refused: the Worker picks their names and Temporal does not tie them to a task queue (its
// normal_name is not checked), so the proxy cannot tell whose sticky queue is named. The
// Workers run with sticky execution off.
func (r Rules) checkPollQueue(tq *taskqueuepb.TaskQueue) error {
	switch tq.GetKind() {
	case enumspb.TASK_QUEUE_KIND_NORMAL, enumspb.TASK_QUEUE_KIND_UNSPECIFIED:
	default:
		return refuse("", "%s task queues are not available to workers", tq.GetKind())
	}
	if tq.GetName() != r.WorkflowQueue {
		return refuse("", "task queue %q is not available to workers", tq.GetName())
	}
	return nil
}

func (r Rules) checkCommand(c *commandpb.Command) error {
	kind := commandName(c.GetCommandType())
	switch c.GetCommandType() {
	case enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK:
		a := c.GetScheduleActivityTaskCommandAttributes()
		switch {
		case a.GetActivityType().GetName() != r.ActivityType:
			return refuse(kind, "activity type %q is not available to workers", a.GetActivityType().GetName())
		case a.GetTaskQueue().GetName() != r.GatewayQueue:
			return refuse(kind, "activities go to %q only, not %q", r.GatewayQueue, a.GetTaskQueue().GetName())
		case a.GetRequestEagerExecution():
			// Eager execution would hand the gateway's activity task to the Worker itself.
			return refuse(kind, "eager activity execution is not available to workers")
		}
	case enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
		for k := range c.GetUpsertWorkflowSearchAttributesCommandAttributes().GetSearchAttributes().GetIndexedFields() {
			if !slices.Contains(r.SearchAttributes, k) {
				return refuse(kind, "search attribute %q is not available to workers", k)
			}
		}
	case enumspb.COMMAND_TYPE_RECORD_MARKER:
		// workflow.patched() records "core_patch"; nothing else here uses markers.
		if name := c.GetRecordMarkerCommandAttributes().GetMarkerName(); name != "core_patch" {
			return refuse(kind, "marker %q is not available to workers", name)
		}
	case enumspb.COMMAND_TYPE_START_TIMER,
		enumspb.COMMAND_TYPE_CANCEL_TIMER,
		enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
		enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
		enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION:
	default:
		// Signalling or cancelling other workflows, child workflows, continue-as-new,
		// Nexus operations and anything added later.
		if kind == unknownCommand {
			return refuse(kind, "command type %d is not available to workers", c.GetCommandType())
		}
		return refuse(kind, "%s is not available to workers", kind)
	}
	return nil
}

const unknownCommand = "COMMAND_TYPE_UNKNOWN"

// commandName is a command's type by name. Every value this version of the API does not name
// has the same one: the Worker picks the value, and must not make up names in agentd's log.
func commandName(t enumspb.CommandType) string {
	if _, ok := enumspb.CommandType_name[int32(t)]; ok {
		return t.String()
	}
	return unknownCommand
}

// namespaceOf returns the request's namespace field, if it has one.
func namespaceOf(m proto.Message) (string, bool) {
	fd := m.ProtoReflect().Descriptor().Fields().ByName("namespace")
	if fd == nil || fd.Kind() != protoreflect.StringKind {
		return "", false
	}
	return m.ProtoReflect().Get(fd).String(), true
}
