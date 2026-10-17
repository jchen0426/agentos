package runtimeproxy

import (
	"testing"

	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
)

// The complete list of what a Worker may call. Adding a method here is a security
// decision: activity-task methods would let the Worker answer gateway calls itself, and
// signal, update or start methods would let it steer runs.
var want = map[string]bool{
	"GetSystemInfo": true, "DescribeNamespace": true,
	"PollWorkflowTaskQueue": true, "RespondWorkflowTaskCompleted": true, "RespondWorkflowTaskFailed": true,
	"RespondQueryTaskCompleted": true, "GetWorkflowExecutionHistory": true, "ResetStickyTaskQueue": true,
	"ShutdownWorker": true, "RecordWorkerHeartbeat": true,
}

func methods(d grpc.ServiceDesc) []string {
	var out []string
	for _, m := range d.Methods {
		out = append(out, "/"+d.ServiceName+"/"+m.MethodName)
	}
	for _, s := range d.Streams {
		out = append(out, "/"+d.ServiceName+"/"+s.StreamName)
	}
	return out
}

func TestOnlyWorkflowTaskMethodsAreAllowed(t *testing.T) {
	all := methods(workflowservice.WorkflowService_ServiceDesc)
	if len(all) < 50 {
		t.Fatalf("only %d WorkflowService methods found; is the descriptor right?", len(all))
	}
	seen := map[string]bool{}
	for _, m := range all {
		name := m[len("/"+workflowservice.WorkflowService_ServiceDesc.ServiceName+"/"):]
		seen[name] = true
		if Allowed(m) != want[name] {
			t.Errorf("%s: allowed=%v, want %v", m, Allowed(m), want[name])
		}
	}
	for name := range want {
		if !seen[name] {
			t.Errorf("%s is allowed but is not a WorkflowService method", name)
		}
	}
	if len(AllowedMethods) != len(want) {
		t.Errorf("AllowedMethods has %d entries, want %d", len(AllowedMethods), len(want))
	}
	for _, m := range methods(operatorservice.OperatorService_ServiceDesc) {
		if Allowed(m) {
			t.Errorf("%s: operator methods must never be allowed", m)
		}
	}
}
