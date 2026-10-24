package gateway

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/jchen0426/agentos/internal/explain"
	"github.com/jchen0426/agentos/internal/githubtool"
	"github.com/jchen0426/agentos/internal/tasks"
)

// cutter forwards connections to target; while it is cut, it closes them, those open and those that
// come: Temporal out of reach, as agentd's client sees it.
type cutter struct {
	ln     net.Listener
	target string
	cut    atomic.Bool
	mu     sync.Mutex
	open   []net.Conn
}

func newCutter(t *testing.T, target string) *cutter {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c := &cutter{ln: ln, target: target}
	go func() {
		for {
			in, err := ln.Accept()
			if err != nil {
				return
			}
			if c.cut.Load() {
				in.Close()
				continue
			}
			out, err := net.Dial("tcp", target)
			if err != nil {
				in.Close()
				continue
			}
			c.mu.Lock()
			c.open = append(c.open, in, out)
			c.mu.Unlock()
			go func() { io.Copy(out, in); out.Close() }()
			go func() { io.Copy(in, out); in.Close() }()
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return c
}

func (c *cutter) set(cut bool) {
	c.cut.Store(cut)
	if cut {
		c.mu.Lock()
		for _, conn := range c.open {
			conn.Close()
		}
		c.open = nil
		c.mu.Unlock()
	}
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	return port
}

// The report against Temporal's own client and a dev server, behind a connection agentd loses for a
// while (the final adversarial review's shape: their outages of 25 s left the report unknown for
// good). While Temporal cannot be reached, the report's call fails, once the SDK's own retries within
// the call are over, and the attempts after it, as Temporal makes them, fail too, until one gets the
// run's verdict: never "unknown". Needs the temporal CLI: AGENTOS_TEMPORAL=1.
func TestTheReportWaitsForARealTemporal(t *testing.T) {
	if os.Getenv("AGENTOS_TEMPORAL") == "" {
		t.Skip("set AGENTOS_TEMPORAL=1 to run a Temporal dev server")
	}
	port := freePort(t)
	addr := "127.0.0.1:" + port
	srv := exec.Command("temporal", "server", "start-dev", "--ip", "127.0.0.1", "--port", port, "--ui-port", freePort(t),
		"--headless", "--log-level", "error")
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Process.Kill(); srv.Wait() })
	var direct client.Client
	var err error
	for deadline := time.Now().Add(60 * time.Second); ; {
		if direct, err = client.Dial(client.Options{HostPort: addr}); err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer direct.Close()
	ctx := context.Background()
	wr, err := direct.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: testWorkflowID, TaskQueue: "nobody"}, "security-audit")
	if err != nil {
		t.Fatal(err)
	}
	_, l := startingRun(t, time.Now())
	if err := recordRunID(l, wr.GetRunID())(ctx); err != nil {
		t.Fatal(err)
	}
	c := newCutter(t, addr)
	behind, err := client.NewLazyClient(client.Options{HostPort: c.ln.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	defer behind.Close()
	var logged bytes.Buffer
	verdicts := ReportVerdicts{Ledger: l, Temporal: behind, Tasks: tasks.Registry{"security-audit": tasks.SecurityAudit(nil, nil, nil)},
		Log: slog.New(slog.NewTextHandler(&logged, nil))}
	if v, err := verdicts.ReportVerdict(ctx, testWorkflowID, "done", ""); err != nil || v.Verdict != "step_failed" {
		t.Fatalf("Temporal there: %+v %v", v, err)
	}
	for _, outage := range []time.Duration{12 * time.Second, 25 * time.Second} {
		c.set(true)
		back := time.AfterFunc(outage, func() { c.set(false) })
		start := time.Now()
		var got githubtool.Verdict
		var failed []time.Duration
		for {
			// An attempt of the report's call, as the gateway's Prepare runs it: the attempt's 10 minutes.
			actx, cancel := context.WithTimeout(ctx, 10*time.Minute)
			v, err := verdicts.ReportVerdict(actx, testWorkflowID, "done", "")
			cancel()
			if err == nil {
				got = v
				break
			}
			if !explain.Unreachable(err) {
				t.Fatalf("outage of %s: %v", outage, err)
			}
			failed = append(failed, time.Since(start).Round(100*time.Millisecond))
			if time.Since(start) > outage+2*time.Minute {
				t.Fatalf("outage of %s: no verdict after %s", outage, time.Since(start))
			}
			time.Sleep(time.Second) // Temporal's backoff between attempts, shortened
		}
		back.Stop()
		c.set(false)
		t.Logf("outage of %s: attempts failed at %v; the verdict %s/%s at %s", outage, failed, got.Verdict, got.Kind,
			time.Since(start).Round(100*time.Millisecond))
		if got.Verdict != "step_failed" || got.Kind != "step_missing" || len(failed) == 0 {
			t.Fatalf("outage of %s: %+v after %d failed attempts", outage, got, len(failed))
		}
	}
	if strings.Contains(logged.String(), "outcome is unknown") {
		t.Fatalf("log %s", logged.String())
	}
}
