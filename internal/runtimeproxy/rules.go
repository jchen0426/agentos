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

// requestFields are the fields each request may set: those the Temporal Python SDK (1.34) sets as
// a workflow-only Worker, as observed through this proxy, and query_results, its answer to a query
// that rides on a workflow task. nil allows every field this version of the API names, for the two
// methods none of whose fields does more than read or answer: a page of a history, a query's
// result. The other fields mean something to Temporal that a Worker has no business with:
// deployment_options or versioning on a poll registers a Worker Deployment under the name the
// Worker gives and starts its system workflows; worker_control_task_queue has Temporal send
// control tasks to a queue the Worker names; sticky_attributes, a sticky queue (see
// checkPollQueue); messages, workflow updates.
//
// The lists are this SDK's: a newer one that sets another field is refused until the field is
// looked at and listed here, and so is a request whose field a newer API adds to the two
// unrestricted methods only once go.temporal.io/api is upgraded (until then the field is unknown,
// and refused). Look at both when upgrading either.
var requestFields = map[string][]string{
	service + "GetSystemInfo":     {},
	service + "DescribeNamespace": {"namespace"},
	service + "PollWorkflowTaskQueue": {"namespace", "task_queue", "identity", "worker_instance_key",
		"worker_version_capabilities"},
	service + "RespondWorkflowTaskCompleted": {"namespace", "task_token", "commands", "identity", "worker_version_stamp",
		"sdk_metadata", "metering_metadata", "capabilities", "worker_instance_key", "query_results"},
	service + "RespondWorkflowTaskFailed":   {"namespace", "task_token", "cause", "failure", "identity", "worker_version"},
	service + "RespondQueryTaskCompleted":   nil,
	service + "GetWorkflowExecutionHistory": nil,
	service + "ShutdownWorker": {"namespace", "identity", "reason", "worker_heartbeat", "worker_instance_key",
		"task_queue", "task_queue_types"},
	service + "RecordWorkerHeartbeat": {"namespace", "identity", "worker_heartbeat"},
}

// versionFields are the requests' notes of the Worker's version: a build ID is all it may give.
var versionFields = []protoreflect.Name{"worker_version_capabilities", "worker_version_stamp", "worker_version"}

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

// refuseField refuses a request for setting a field. The name comes from the API's descriptors,
// never from the request, so each field is a rule of its own in the refusal log.
func refuseField(name string) *Refusal {
	reason := name + " is not available to workers"
	return &Refusal{Reason: reason, rule: reason}
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
	// A field this version of the API does not name is passed on as it came, and agentd cannot
	// tell what Temporal makes of it.
	if hasUnknownFields(req.ProtoReflect()) {
		return refuse("", "the request carries fields this agentd does not know")
	}
	if err := checkFields(req.ProtoReflect(), requestFields[method]); err != nil {
		return err
	}
	if ns, ok := namespaceOf(req); ok && ns != r.Namespace {
		return refuse("", "namespace %q is not available to workers", ns)
	}
	switch m := req.(type) {
	case *workflowservice.PollWorkflowTaskQueueRequest:
		return r.checkPollQueue(m.GetTaskQueue())
	case *workflowservice.RespondWorkflowTaskCompletedRequest:
		for _, c := range m.GetCommands() {
			if err := r.checkCommand(c); err != nil {
				return err
			}
		}
	case *workflowservice.ShutdownWorkerRequest:
		if q := m.GetTaskQueue(); q != "" && q != r.WorkflowQueue {
			return refuse("", "task queue %q is not available to workers", q)
		}
	}
	return nil
}

// checkFields refuses a request that sets a field allowed does not name (nil allows all), or that
// says more about the Worker's version than its build ID.
func checkFields(m protoreflect.Message, allowed []string) error {
	var err error
	m.Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		if allowed != nil && !slices.Contains(allowed, string(fd.Name())) {
			err = refuseField(string(fd.Name()))
		}
		return err == nil
	})
	if err != nil {
		return err
	}
	for _, name := range versionFields {
		if fd := m.Descriptor().Fields().ByName(name); fd != nil && m.Has(fd) {
			if err := onlyBuildID(string(name), m.Get(fd).Message()); err != nil {
				return err
			}
		}
	}
	// A heartbeat (RecordWorkerHeartbeat's, ShutdownWorker's last one) is the Worker's report of
	// itself, for show, but for the version it names: the SDK gives its build ID there, and a
	// deployment name would claim a Worker Deployment, as deployment options would.
	fd := m.Descriptor().Fields().ByName("worker_heartbeat")
	if fd == nil || !m.Has(fd) {
		return nil
	}
	var beats []protoreflect.Message
	if fd.IsList() {
		for i := 0; i < m.Get(fd).List().Len(); i++ {
			beats = append(beats, m.Get(fd).List().Get(i).Message())
		}
	} else {
		beats = append(beats, m.Get(fd).Message())
	}
	for _, b := range beats {
		if v := b.Descriptor().Fields().ByName("deployment_version"); v != nil && b.Has(v) {
			if err := onlyBuildID("worker_heartbeat.deployment_version", b.Get(v).Message()); err != nil {
				return err
			}
		}
	}
	return nil
}

// onlyBuildID refuses a note of the Worker's version, named name, that says more than its build ID.
func onlyBuildID(name string, version protoreflect.Message) error {
	var err error
	version.Range(func(sub protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		if sub.Name() != "build_id" {
			err = refuseField(name + "." + string(sub.Name()))
		}
		return err == nil
	})
	return err
}

// hasUnknownFields reports whether m, or any message in it, holds a field its descriptor does not
// name. countValues has bounded the request's size and depth before it is decoded.
func hasUnknownFields(m protoreflect.Message) bool {
	if len(m.GetUnknown()) > 0 {
		return true
	}
	found := false
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsList() && fd.Message() != nil:
			for i := 0; i < v.List().Len() && !found; i++ {
				found = hasUnknownFields(v.List().Get(i).Message())
			}
		case fd.IsMap() && fd.MapValue().Message() != nil:
			v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
				found = hasUnknownFields(mv.Message())
				return !found
			})
		case !fd.IsList() && !fd.IsMap() && fd.Message() != nil:
			found = hasUnknownFields(v.Message())
		}
		return !found
	})
	return found
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
		case proto.Size(a.GetPriority()) > 0:
			// The SDK sends an empty one. A priority or a fairness key would move this run's
			// calls ahead of other runs' on the one gateway queue.
			return refuse(kind, "activity priority is not available to workers")
		case a.GetUseWorkflowBuildId():
			return refuse(kind, "versioned activities are not available to workers")
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
