// agentctl is the command line for agentd's Control socket.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/safetext"
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
	budget := &cobra.Command{Use: "budget", Short: "Manage run budgets"}
	budget.AddCommand(extendCmd(g))
	reconcile := &cobra.Command{Use: "reconcile", Short: "Settle external writes whose outcome nobody knows"}
	reconcile.AddCommand(reconcileListCmd(g), reconcileShowCmd(g), reconcileResolveCmd(g))
	root.AddCommand(runs, budget, reconcile, approvalsCmd(g))

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "agentctl:", safetext.Line(err.Error()))
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
// know any task's parameters (D12). --dry-run is the one flag without a value.
func startCmd(g *globals) *cobra.Command {
	return &cobra.Command{
		Use:                "start <task> [--<param> <value>]... [--budget-micros N] [--dry-run]",
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
				if name == "dry-run" && !hasValue {
					req.DryRun = true
					continue
				}
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
					fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", safetext.Line(r.GetRunId()), safetext.Line(r.GetTask()),
						safetext.Line(r.GetState()), safetext.Line(r.GetCreatedAt()))
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

func extendCmd(g *globals) *cobra.Command {
	var amount, key string
	cmd := &cobra.Command{
		Use:     "extend <run-id> --usd <amount>",
		Short:   "Raise a run's budget; calls paused for budget ask again",
		Example: "  agentctl budget extend 0f8b1c2e-... --usd 2.50",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			micros, err := parseUSD(amount)
			if err != nil {
				return fmt.Errorf("--usd: %w", err)
			}
			if key == "" {
				key = uuid.NewString()
			}
			return withClient(g, func(ctx context.Context, c agentosv1.ControlClient) error {
				run, err := c.ExtendBudget(ctx, &agentosv1.ExtendBudgetRequest{
					RunId: args[0], AmountMicros: micros, IdempotencyKey: key,
				})
				if err != nil {
					return err
				}
				return printRun(g, run)
			})
		},
	}
	cmd.Flags().StringVar(&amount, "usd", "", "amount to add, in US dollars, e.g. 2.50")
	cmd.Flags().StringVar(&key, "idempotency-key", "", "reuse to make a repeated extension add nothing (default: a new key)")
	_ = cmd.MarkFlagRequired("usd")
	return cmd
}

// parseUSD reads a positive dollar amount, exactly, into micros.
func parseUSD(s string) (int64, error) {
	whole, frac, _ := strings.Cut(strings.TrimPrefix(s, "$"), ".")
	if whole == "" || len(frac) > 6 || strings.Trim(whole+frac, "0123456789") != "" {
		return 0, fmt.Errorf("%q is not an amount like 2.50 (at most 6 decimals)", s)
	}
	n, err := strconv.ParseInt(whole+(frac + "000000")[:6], 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%q is not a positive amount", s)
	}
	return n, nil
}

// usd shows micros as dollars, exactly: $5.00, $0.0045.
func usd(micros int64) string {
	sign := ""
	if micros < 0 {
		sign, micros = "-", -micros
	}
	frac := strings.TrimRight(fmt.Sprintf("%06d", micros%1e6), "0")
	for len(frac) < 2 {
		frac += "0"
	}
	return fmt.Sprintf("%s$%d.%s", sign, micros/1e6, frac)
}

func pendingReconciliations(ctx context.Context, c agentosv1.ControlClient) ([]*agentosv1.PendingItem, error) {
	resp, err := c.ListPending(ctx, &agentosv1.ListPendingRequest{})
	if err != nil {
		return nil, err
	}
	var out []*agentosv1.PendingItem
	for _, it := range resp.GetItems() {
		if it.GetKind() == "reconciliation" {
			out = append(out, it)
		}
	}
	return out, nil
}

func reconcileListCmd(g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List external writes waiting for a person to say whether they happened",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return withClient(g, func(ctx context.Context, c agentosv1.ControlClient) error {
				items, err := pendingReconciliations(ctx, c)
				if err != nil {
					return err
				}
				if g.output == "json" {
					return printJSON(&agentosv1.ListPendingResponse{Items: items})
				}
				tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
				fmt.Fprintln(tw, "EFFECT ID\tRUN ID\tTOOL\tSINCE\tREASON")
				for _, it := range items {
					fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", safetext.Line(it.GetId()), safetext.Line(it.GetRunId()),
						safetext.Line(it.GetTool()), safetext.Line(it.GetSince()), safetext.Line(it.GetReason()))
				}
				return tw.Flush()
			})
		},
	}
}

func reconcileShowCmd(g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "show <effect-id>",
		Short: "Show one write waiting for reconciliation, with exactly what was to be sent",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withClient(g, func(ctx context.Context, c agentosv1.ControlClient) error {
				items, err := pendingReconciliations(ctx, c)
				if err != nil {
					return err
				}
				for _, it := range items {
					if it.GetId() == args[0] {
						return printJSON(it)
					}
				}
				return fmt.Errorf("no effect %s waits for reconciliation", args[0])
			})
		},
	}
}

func reconcileResolveCmd(g *globals) *cobra.Command {
	var executed, notExecuted, lookup bool
	var result, key string
	cmd := &cobra.Command{
		Use:   "resolve <effect-id> (--executed [--result JSON | --lookup] | --not-executed)",
		Short: "Say whether an external write happened; the run then carries on",
		Long: "--executed records the write as done, with what it produced (--result), or with what\n" +
			"agentd finds when it looks (--lookup, for tools that can look). --not-executed has the\n" +
			"run send it again under its original idempotency key; it is dropped instead if the run was\n" +
			"cancelled or has ended, or if the write's approval has run out by then. For a write its\n" +
			"tool can look up, agentd takes --not-executed only once no request of it can still take\n" +
			"effect, and says when that is.",
		Example: "  agentctl reconcile resolve 3c1f... --executed --result '{\"written\": true}'\n" +
			"  agentctl reconcile resolve 3c1f... --not-executed",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if executed == notExecuted {
				return fmt.Errorf("say either --executed or --not-executed")
			}
			if key == "" {
				key = uuid.NewString()
			}
			return withClient(g, func(ctx context.Context, c agentosv1.ControlClient) error {
				resp, err := c.ResolveReconciliation(ctx, &agentosv1.ResolveReconciliationRequest{
					EffectId: args[0], Executed: executed, ResultJson: result, Lookup: lookup, IdempotencyKey: key,
				})
				if err != nil {
					return err
				}
				return printJSON(resp)
			})
		},
	}
	cmd.Flags().BoolVar(&executed, "executed", false, "the write happened")
	cmd.Flags().BoolVar(&notExecuted, "not-executed", false, "the write did not happen")
	cmd.Flags().StringVar(&result, "result", "", "with --executed: what the write produced, as a JSON object")
	cmd.Flags().BoolVar(&lookup, "lookup", false, "with --executed: let agentd look the result up")
	cmd.Flags().StringVar(&key, "idempotency-key", "", "reuse to make a repeated resolution apply once (default: a new key)")
	cmd.MarkFlagsMutuallyExclusive("result", "lookup")
	cmd.MarkFlagsMutuallyExclusive("not-executed", "result")
	cmd.MarkFlagsMutuallyExclusive("not-executed", "lookup")
	return cmd
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
	conn, err := grpc.NewClient("unix://"+path, dialOptions()...)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return fn(ctx, agentosv1.NewControlClient(conn))
}

// maxReceive is what agentctl takes in one answer: an approval card holds the whole payload, a
// pull request's up to 20 files of 1 MiB, larger still once its text is escaped. gRPC's default
// of 4 MiB would leave such an approval impossible to read, and so to decide.
const maxReceive = 512 << 20

func dialOptions() []grpc.DialOption {
	return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxReceive))}
}

func printRun(g *globals, r *agentosv1.Run) error {
	if g.output == "json" {
		return printJSON(r)
	}
	return writeRun(os.Stdout, r)
}

// writeRun prints r as text. Every value goes through safetext.Line: the result and the
// failure come from the Worker, which agentd does not trust.
func writeRun(w io.Writer, r *agentosv1.Run) error {
	keys := make([]string, 0, len(r.GetParams()))
	for k := range r.GetParams() {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	params := make([]string, 0, len(keys))
	for _, k := range keys {
		params = append(params, k+"="+r.GetParams()[k])
	}
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	row := func(name, value string) { fmt.Fprintf(tw, "%s\t%s\n", name, safetext.Line(value)) }
	row("run id", r.GetRunId())
	row("task", r.GetTask())
	row("state", r.GetState())
	row("params", strings.Join(params, " "))
	row("budget", fmt.Sprintf("%s spent, %s reserved, limit %s",
		usd(r.GetBudgetSpentMicros()), usd(r.GetBudgetReservedMicros()), usd(r.GetBudgetLimitMicros())))
	row("created", r.GetCreatedAt())
	if r.GetDryRun() {
		row("dry run", "yes: nothing it writes is sent")
	}
	if r.GetResultJson() != "" {
		row("result", r.GetResultJson())
	}
	if r.GetFailure() != "" {
		row("failure", r.GetFailure())
	}
	return tw.Flush()
}

func printJSON(m proto.Message) error {
	b, err := encodeJSON(m)
	if err != nil {
		return err
	}
	_, err = fmt.Println(string(b))
	return err
}

// encodeJSON is protojson with DEL, C1 controls and format characters escaped too.
func encodeJSON(m proto.Message) ([]byte, error) {
	b, err := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}.Marshal(m)
	if err != nil {
		return nil, err
	}
	return safetext.JSON(b), nil
}
