// agentctl is the command line for agentd's Control socket.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
)

type globals struct {
	socket string
	output string
}

func main() {
	g := &globals{}
	root := &cobra.Command{
		Use:           "agentctl",
		Short:         "Command line for agentd",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().StringVar(&g.socket, "socket", defaultSocket(), "agentd control socket (env AGENTOS_CONTROL_SOCKET)")
	root.PersistentFlags().StringVarP(&g.output, "output", "o", "text", "output format: text or json")

	runs := &cobra.Command{Use: "runs", Short: "Start, inspect and cancel runs"}
	runs.AddCommand(startCmd(g), getCmd(g), listCmd(g), cancelCmd(g), explainCmd(g))
	root.AddCommand(runs)

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "agentctl:", err)
		os.Exit(1)
	}
}

func defaultSocket() string {
	if s := os.Getenv("AGENTOS_CONTROL_SOCKET"); s != "" {
		return s
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".agentos", "control", "control.sock")
}

// startCmd parses its own flags: everything after the task name is a task parameter
// (--repo acme/payments --commit 3f2a9c1 --base main), so the platform does not have to
// know any task's parameters (D12).
func startCmd(g *globals) *cobra.Command {
	return &cobra.Command{
		Use:                "start <task> [--<param> <value>]... [--budget-micros N]",
		Short:              "Start a run of a task",
		Example:            "  agentctl runs start security-audit --repo acme/payments --commit 3f2a9c1 --base main",
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			req := &agentosv1.StartRunRequest{Params: map[string]string{}}
			for i := 0; i < len(args); i++ {
				a := args[i]
				if a == "-h" || a == "--help" {
					return cmd.Help()
				}
				if !strings.HasPrefix(a, "-") {
					if req.Task != "" {
						return fmt.Errorf("unexpected argument %q", a)
					}
					req.Task = a
					continue
				}
				name, value, hasValue := strings.Cut(strings.TrimLeft(a, "-"), "=")
				if !hasValue {
					if i+1 >= len(args) {
						return fmt.Errorf("flag %s needs a value", a)
					}
					i++
					value = args[i]
				}
				switch name {
				case "socket":
					g.socket = value
				case "o", "output":
					g.output = value
				case "budget-micros":
					n, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return fmt.Errorf("--budget-micros: %w", err)
					}
					req.BudgetLimitMicros = n
				default:
					req.Params[name] = value
				}
			}
			if req.Task == "" {
				return fmt.Errorf("missing task name\n\n%s", cmd.UsageString())
			}
			return withClient(g, func(ctx context.Context, c agentosv1.ControlClient) error {
				run, err := c.StartRun(ctx, req)
				if err != nil {
					return err
				}
				return printRun(g, run)
			})
		},
	}
}

func getCmd(g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "get <run-id>",
		Short: "Show one run",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withClient(g, func(ctx context.Context, c agentosv1.ControlClient) error {
				run, err := c.GetRun(ctx, &agentosv1.GetRunRequest{RunId: args[0]})
				if err != nil {
					return err
				}
				return printRun(g, run)
			})
		},
	}
}

func listCmd(g *globals) *cobra.Command {
	var limit int32
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List recent runs, newest first",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return withClient(g, func(ctx context.Context, c agentosv1.ControlClient) error {
				resp, err := c.ListRuns(ctx, &agentosv1.ListRunsRequest{Limit: limit})
				if err != nil {
					return err
				}
				if g.output == "json" {
					return printJSON(resp)
				}
				tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
				fmt.Fprintln(tw, "RUN ID\tTASK\tSTATE\tCREATED")
				for _, r := range resp.GetRuns() {
					fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", r.GetRunId(), r.GetTask(), r.GetState(), r.GetCreatedAt())
				}
				return tw.Flush()
			})
		},
	}
	cmd.Flags().Int32Var(&limit, "limit", 20, "how many runs to show")
	return cmd
}

func cancelCmd(g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "cancel <run-id>",
		Short: "Cancel a run; a cancelled run posts no report",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withClient(g, func(ctx context.Context, c agentosv1.ControlClient) error {
				run, err := c.CancelRun(ctx, &agentosv1.CancelRunRequest{RunId: args[0]})
				if err != nil {
					return err
				}
				return printRun(g, run)
			})
		},
	}
}

func explainCmd(g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "explain <run-id>",
		Short: "Point at the step that went wrong and why (M5)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withClient(g, func(ctx context.Context, c agentosv1.ControlClient) error {
				e, err := c.ExplainRun(ctx, &agentosv1.ExplainRunRequest{RunId: args[0]})
				if err != nil {
					return err
				}
				return printJSON(e)
			})
		},
	}
}

func withClient(g *globals, fn func(context.Context, agentosv1.ControlClient) error) error {
	path, err := filepath.Abs(g.socket)
	if err != nil {
		return err
	}
	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("control socket %s: %w (is agentd running?)", path, err)
	}
	conn, err := grpc.NewClient("unix://"+path, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return fn(ctx, agentosv1.NewControlClient(conn))
}

func printRun(g *globals, r *agentosv1.Run) error {
	if g.output == "json" {
		return printJSON(r)
	}
	keys := make([]string, 0, len(r.GetParams()))
	for k := range r.GetParams() {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	params := make([]string, 0, len(keys))
	for _, k := range keys {
		params = append(params, k+"="+r.GetParams()[k])
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "run id\t%s\n", r.GetRunId())
	fmt.Fprintf(tw, "task\t%s\n", r.GetTask())
	fmt.Fprintf(tw, "state\t%s\n", r.GetState())
	fmt.Fprintf(tw, "params\t%s\n", strings.Join(params, " "))
	fmt.Fprintf(tw, "budget\t$%.2f\n", float64(r.GetBudgetLimitMicros())/1e6)
	fmt.Fprintf(tw, "created\t%s\n", r.GetCreatedAt())
	if r.GetResultJson() != "" {
		fmt.Fprintf(tw, "result\t%s\n", r.GetResultJson())
	}
	if r.GetFailure() != "" {
		fmt.Fprintf(tw, "failure\t%s\n", r.GetFailure())
	}
	return tw.Flush()
}

func printJSON(m proto.Message) error {
	b, err := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}.Marshal(m)
	if err != nil {
		return err
	}
	_, err = fmt.Println(string(b))
	return err
}
