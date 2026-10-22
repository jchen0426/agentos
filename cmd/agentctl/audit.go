package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/safetext"
)

// The answers of agentctl audit verify after its report, besides the report: the log does not verify
// (agentctl exits 1), or some runs could not be checked (exits 2). It exits 3 when it could not
// verify at all (couldNotVerify): agentd did not answer, or not in time, or refused the request; or
// agentd read only part of what Temporal holds, past its bounds, and found nothing wrong in it.
var (
	errUnverified = exitError{1, errors.New("the audit log does not verify")}
	errPartial    = exitError{3, errors.New("verify could not read all of what Temporal holds of the audit log, past its bounds: " +
		"the audit log is not verified")}
	errUnchecked = exitError{2, errors.New("some runs could not be checked: their histories are gone from Temporal " +
		"(--allow-unavailable accepts that)")}
	errBeforeLog = exitError{2, errors.New("some runs were made before the audit log began, and are not checked " +
		"(--allow-before-log accepts them)")}
	errUnrecorded = exitError{2, errors.New("some calls' records were lost to a fault (agentd's log says which), " +
		"and are not checked")}
)

// couldNotVerify is err as agentctl audit verify exits with it: 3, unless it is a verdict.
func couldNotVerify(err error) error {
	var exit exitError
	if err == nil || errors.As(err, &exit) {
		return err
	}
	return exitError{3, err}
}

// exitError is an error agentctl exits with a status of its own for.
type exitError struct {
	code int
	error
}

func (e exitError) Unwrap() error { return e.error }

// verdict is what a report means for agentctl's exit: nil when everything was checked and matches,
// or what was not checked is accepted.
func verdict(r *agentosv1.AuditReport, allowUnavailable, allowBeforeLog bool) error {
	switch {
	case r.GetBrokenAtSeq() != 0 || len(r.GetAnchorsMismatched()) > 0 || len(r.GetRunsMismatched()) > 0 ||
		len(r.GetStateMismatched()) > 0:
		return errUnverified
	case len(r.GetUnread()) > 0: // never 0 on a partial read
		return errPartial
	case len(r.GetAttemptsUnrecorded()) > 0:
		return errUnrecorded
	case len(r.GetRunsUnavailable()) > 0 && !allowUnavailable:
		return errUnchecked
	case len(r.GetRunsBeforeLog()) > 0 && !allowBeforeLog:
		return errBeforeLog
	}
	return nil
}

// auditCmd is `agentctl audit`: agentd's hash-chained audit log (design doc, section 8).
func auditCmd(g *globals) *cobra.Command {
	c := &cobra.Command{Use: "audit", Short: "Check agentd's audit log"}
	c.AddCommand(auditVerifyCmd(g))
	return c
}

func auditVerifyCmd(g *globals) *cobra.Command {
	var run string
	var allowUnavailable, allowBeforeLog bool
	var allowChains, allowReplaced []string
	var timeout time.Duration
	cmd := &cobra.Command{
		Use:   "verify [--run <run-id>]",
		Short: "Verify the audit log's hash chain, and the anchors of it in Temporal's history",
		Long: "Walks the hash chain of agentd's audit log from its first record: a record changed, removed or\n" +
			"put out of order breaks it there. Then reads each run's history in Temporal, where every result\n" +
			"of a gateway call names the record of that call (_audit), and checks each against the ledger: a\n" +
			"chain rewritten consistently from some record on verifies again, but no longer holds the records\n" +
			"the results named. The heads agentd pushes to Temporal anchor the rest of the chain. The ledger's\n" +
			"tables must be the state the records imply. The runs are those on the Worker's task queue in\n" +
			"Temporal and those the audit log records, besides the ledger's: a run the ledger has otherwise, or\n" +
			"not at all, does not match. Exits 0 when everything was checked and matches, 1 when anything does\n" +
			"not match, 2 when a run could not be checked (Temporal no longer has its history, or it was made\n" +
			"before the audit log began) and nothing else is wrong, and 3 when it could not verify at all (agentd\n" +
			"did not answer, or not within --timeout, or refused the request, as for a --run it does not know;\n" +
			"or it read only part of what Temporal holds, past its bounds, and found nothing wrong in that).\n" +
			"Another chain's anchor workflows in the namespace are a mismatch: a chain rewritten from its first\n" +
			"record has a new key, and leaves the old one's behind. --allow-chain accepts another agentd's chain\n" +
			"(the heads name the agentd that pushed them); --allow-replaced-ledger accepts a chain of this\n" +
			"agentd's own, of a ledger you replaced yourself (a reset), unless it went on pushing after this\n" +
			"chain's first push.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return couldNotVerify(withClientTimeout(g, timeout, func(ctx context.Context, c agentosv1.ControlClient) error {
				rep, err := c.VerifyAudit(ctx, &agentosv1.VerifyAuditRequest{RunId: run, AllowChains: allowChains,
					AllowReplacedLedgers: allowReplaced})
				if err != nil {
					return err
				}
				if g.output == "json" {
					err = printJSON(rep)
				} else {
					err = writeAuditReport(os.Stdout, rep)
				}
				if err != nil {
					return err
				}
				return verdict(rep, allowUnavailable, allowBeforeLog)
			}))
		},
	}
	cmd.Flags().StringVar(&run, "run", "", "check only this run's anchors (the chain is walked whole)")
	cmd.Flags().BoolVar(&allowUnavailable, "allow-unavailable", false, "exit 0 though some runs' histories are gone from Temporal")
	cmd.Flags().DurationVar(&timeout, "timeout", 5*time.Minute, "how long to wait for agentd's verification")
	cmd.Flags().StringArrayVar(&allowChains, "allow-chain", nil, "accept the anchor workflows of another agentd's chain in the "+
		"namespace (its key, as the report names it); repeatable")
	cmd.Flags().StringArrayVar(&allowReplaced, "allow-replaced-ledger", nil, "accept the anchor workflows of a chain of this "+
		"agentd's own, of a ledger you replaced yourself (its key, as the report names it); repeatable")
	cmd.Flags().BoolVar(&allowBeforeLog, "allow-before-log", false, "exit 0 though some runs were made before the audit log began "+
		"(an older agentd's), and are not checked")
	return cmd
}

// writeAuditReport prints a report as text. The reasons quote what a Worker named (activity IDs)
// and what Temporal said: every value goes through safetext.Line, and the IDs a Worker or anyone in
// the namespace may choose (activity IDs, workflow IDs, the keys of other chains) are quoted, so that
// none reads as part of the line around it.
func writeAuditReport(w io.Writer, r *agentosv1.AuditReport) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	row := func(name, value string) { fmt.Fprintf(tw, "%s\t%s\n", name, safetext.Line(value)) }
	if r.GetBrokenAtSeq() == 0 {
		row("chain", fmt.Sprintf("whole: %d records", r.GetRecords()))
	} else {
		row("chain", fmt.Sprintf("BROKEN at seq %d: %s", r.GetBrokenAtSeq(), r.GetBrokenReason()))
		row("verified", fmt.Sprintf("%d records before it", r.GetRecords()))
	}
	if r.GetRecords() > 0 {
		row("head", fmt.Sprintf("seq %d, sha256 %s", r.GetHeadSeq(), r.GetHeadSha256()))
	}
	row("anchors", fmt.Sprintf("%d checked in %d runs, %d do not match", r.GetAnchorsChecked(), r.GetRunsChecked(),
		len(r.GetAnchorsMismatched())))
	for _, m := range r.GetAnchorsMismatched() {
		var where []string
		if m.GetRunId() != "" {
			where = append(where, fmt.Sprintf("run %q", m.GetRunId()))
		}
		if m.GetActivityId() != "" {
			where = append(where, fmt.Sprintf("call %q", m.GetActivityId()))
		}
		if m.GetSeq() != 0 {
			where = append(where, fmt.Sprintf("seq %d", m.GetSeq()))
		}
		row("  mismatch", strings.TrimPrefix(strings.Join(where, ", ")+": "+m.GetWhy(), ": "))
	}
	if len(r.GetRunsMismatched()) > 0 {
		row("runs", fmt.Sprintf("%d do not match the ledger", len(r.GetRunsMismatched())))
	}
	for _, m := range r.GetRunsMismatched() {
		row("  mismatch", fmt.Sprintf("run %q: %s", m.GetRunId(), m.GetWhy()))
	}
	if len(r.GetStateMismatched()) > 0 {
		row("state", fmt.Sprintf("%d differences from what the records say", len(r.GetStateMismatched())))
	}
	for _, m := range r.GetStateMismatched() {
		row("  mismatch", fmt.Sprintf("%s %q: %s", m.GetKind(), m.GetId(), m.GetWhy()))
	}
	row("outside", fmt.Sprintf("%d heads pushed to Temporal checked; the latest is seq %d, %d records after it",
		r.GetExternalAnchorsChecked(), r.GetLastExternalAnchorSeq(), r.GetRecordsAfterExternalAnchor()))
	if r.GetChain() != "" {
		row("chain key", r.GetChain())
	}
	if len(r.GetOtherChains()) > 0 {
		keys := make([]string, len(r.GetOtherChains()))
		for i, k := range r.GetOtherChains() {
			keys[i] = strconv.Quote(k)
		}
		row("other chains", fmt.Sprintf("%s; %d anchor workflows of those allowed, not checked", strings.Join(keys, ", "),
			r.GetOtherAnchorWorkflows()))
	}
	for _, u := range r.GetRunsUnavailable() {
		row("not checked", fmt.Sprintf("run %q: %s", u.GetRunId(), u.GetWhy()))
	}
	for _, u := range r.GetRunsBeforeLog() {
		row("before the log", fmt.Sprintf("run %q: %s", u.GetRunId(), u.GetWhy()))
	}
	for _, a := range r.GetAttemptsCutShort() {
		row("cut short", fmt.Sprintf("run %q, call %q, attempt %d: its call.started (seq %d) has no call record; agentd started "+
			"again after it (seq %d)", a.GetRunId(), a.GetActivityId(), a.GetAttempt(), a.GetSeq(), a.GetRestartedSeq()))
	}
	for _, u := range r.GetUncheckedTables() {
		row("unchecked table", fmt.Sprintf("%q: %s", u.GetName(), u.GetWhy()))
	}
	for _, a := range r.GetAttemptsUnrecorded() {
		row("not recorded", fmt.Sprintf("run %q, call %q, attempt %d: its call.started (seq %d) has no call record, which a "+
			"fault lost: %s", a.GetRunId(), a.GetActivityId(), a.GetAttempt(), a.GetSeq(), a.GetWhy()))
	}
	for _, x := range r.GetTerminatedOutsideAgentd() {
		at := "at a time its history does not give"
		if x.GetAt() != "" {
			at = "at " + x.GetAt()
		}
		// Who terminated it and why are its caller's own words: any caller of Temporal's API writes them.
		row("terminated", fmt.Sprintf("run %q, execution %q, %s, not by agentd; its caller says it is %.200q, and why: %.200q",
			x.GetRunId(), x.GetTemporalRunId(), at, x.GetClaimedIdentity(), x.GetClaimedReason()))
	}
	for _, u := range r.GetUnread() {
		row("not read", fmt.Sprintf("%s, %d: %s", u.GetWhat(), u.GetCount(), u.GetWhy()))
	}
	return tw.Flush()
}
