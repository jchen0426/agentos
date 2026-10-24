package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/safetext"
)

func explainCmd(g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "explain <run-id>",
		Short: "Say why a run is where it is: the step that went wrong and why, and the trouble along the way",
		Long: "Reads the run's history in Temporal with agentd's ledger and audit log. The verdict, the step and\n" +
			"the kind of cause come from what agentd and Temporal recorded; what the workflow says of itself\n" +
			"(its outcome, its failure) is shown apart, as its claim. A crash leaves no record of its own: one is\n" +
			"inferred from what it left, and the cause says so. For a run that waits for a person, next names\n" +
			"the command it waits for.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withClient(g, func(ctx context.Context, c agentosv1.ControlClient) error {
				e, err := c.ExplainRun(ctx, &agentosv1.ExplainRunRequest{RunId: args[0]})
				if err != nil {
					return err
				}
				if g.output == "json" {
					return printJSON(e)
				}
				return writeExplanation(os.Stdout, e)
			})
		},
	}
}

// writeExplanation prints an explanation as text. Its texts quote the Worker, tools, models and
// people: every value goes through safetext.Line, as agentd already did, and what the Worker or a
// model chose (the claim, a call's name, an activity ID, a model's name) is printed quoted (%q), so
// that it cannot read as agentctl's own words.
func writeExplanation(w io.Writer, e *agentosv1.Explanation) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	row := func(name, value string) { fmt.Fprintf(tw, "%s\t%s\n", name, safetext.Line(value)) }
	row("run id", e.GetRunId())
	row("state", e.GetState())
	row("verdict", e.GetVerdict())
	if e.GetStep() != "" {
		where := []string{}
		if e.GetCall() != "" && e.GetCall() != e.GetStep() {
			where = append(where, fmt.Sprintf("call %q", e.GetCall()))
		}
		if e.GetActivityId() != "" {
			where = append(where, fmt.Sprintf("activity %q", e.GetActivityId()))
		}
		if e.GetAttempt() > 0 {
			where = append(where, fmt.Sprintf("attempt %d", e.GetAttempt()))
		}
		step := e.GetStep()
		if len(where) > 0 {
			step += " (" + strings.Join(where, ", ") + ")"
		}
		row("step", step)
	}
	if e.GetKind() != "" {
		row("kind", e.GetKind())
	}
	row("cause", e.GetCause())
	if e.GetClaim() != "" {
		row("workflow says", fmt.Sprintf("%q (its own claim, not checked)", e.GetClaim()))
	}
	if e.GetNext() != "" {
		row("next", e.GetNext())
	}
	if c := e.GetCost(); c != nil {
		row("cost", fmt.Sprintf("%s spent, %s reserved, limit %s", usd(c.GetSpentMicros()), usd(c.GetReservedMicros()),
			usd(c.GetLimitMicros())))
	}
	for i, u := range e.GetUsage() {
		name := ""
		if i == 0 {
			name = "usage"
		}
		row(name, fmt.Sprintf("%q: %s, %d input tokens, %d output, %d read from the cache, %d written to it, %s",
			u.GetModel(), count(u.GetRequests(), "request"), u.GetInputTokens(), u.GetOutputTokens(), u.GetCacheReadTokens(),
			u.GetCacheWriteTokens(), count(u.GetRefusals(), "refusal")))
	}
	for i, ev := range e.GetEvidence() {
		name := ""
		if i == 0 {
			name = "evidence"
		}
		row(name, ev)
	}
	for i, inc := range e.GetIncidents() {
		name := ""
		if i == 0 {
			name = "incidents"
		}
		step := inc.GetStep()
		if inc.GetActivityId() != "" {
			step += fmt.Sprintf(" (activity %q)", inc.GetActivityId())
		}
		if inc.GetAttempt() > 0 {
			step += fmt.Sprintf(" attempt %d", inc.GetAttempt())
		}
		row(name, fmt.Sprintf("%s %s %s: %s", inc.GetAt(), strings.TrimSpace(step), inc.GetKind(), inc.GetCause()))
	}
	if n := e.GetIncidentsOmitted(); n > 0 {
		row("", fmt.Sprintf("and %s more, not listed", count(int64(n), "incident")))
	}
	for i, m := range e.GetMissing() {
		name := ""
		if i == 0 {
			name = "missing"
		}
		row(name, m)
	}
	return tw.Flush()
}

// count is n of a thing, plural when n is not 1: 1 request, 2 requests.
func count(n int64, thing string) string {
	if n == 1 {
		return "1 " + thing
	}
	return fmt.Sprintf("%d %ss", n, thing)
}
