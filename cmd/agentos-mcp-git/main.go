// agentos-mcp-git is the first task's read-only git MCP server (internal/gitmcp), on stdio.
// agentd starts it once per call, inside sandbox/<os>/run-tool.sh, with the one mirror the
// call needs.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/jchen0426/agentos/internal/gitmcp"
)

func main() {
	mirror := flag.String("mirror", "", "the read-only git mirror to serve")
	flag.Parse()
	if *mirror == "" {
		fmt.Fprintln(os.Stderr, "agentos-mcp-git: --mirror is required")
		os.Exit(2)
	}
	// The launcher names the one directory the server may write: sandbox-runtime sets TMPDIR
	// to a directory of its own. git's warnings go to stderr, which agentd logs when a call
	// fails.
	s := &gitmcp.Server{Mirror: *mirror, TempDir: os.Getenv("AGENTOS_TMPDIR"), Stderr: os.Stderr}
	if err := s.MCP().Run(context.Background(), &mcp.StdioTransport{MaxLineLength: gitmcp.MaxRequest}); err != nil {
		fmt.Fprintln(os.Stderr, "agentos-mcp-git:", err)
		os.Exit(1)
	}
}
