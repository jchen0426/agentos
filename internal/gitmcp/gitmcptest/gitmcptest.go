// Package gitmcptest builds the git tool server for tests that call it the way agentd does:
// through gitmcp.Client, a process per call, here without a sandbox.
package gitmcptest

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// Server returns the agentos-mcp-git binary: built into the test's temporary directory, or the
// prebuilt one AGENTOS_TEST_MCP_GIT names, where there is no Go toolchain. In -short mode the
// test is skipped.
func Server(t testing.TB) string {
	t.Helper()
	if testing.Short() {
		t.Skip("builds the git tool server")
	}
	if bin := os.Getenv("AGENTOS_TEST_MCP_GIT"); bin != "" {
		return bin
	}
	bin := filepath.Join(t.TempDir(), "agentos-mcp-git")
	if out, err := exec.Command("go", "build", "-o", bin, "github.com/jchen0426/agentos/cmd/agentos-mcp-git").CombinedOutput(); err != nil {
		t.Fatalf("build the git tool server: %v\n%s", err, out)
	}
	return bin
}
