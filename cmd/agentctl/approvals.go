package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/user"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/safetext"
)

// approvalsCmd is `agentctl approvals`: the writes that wait for a person (design doc, section 10).
func approvalsCmd(g *globals) *cobra.Command {
	c := &cobra.Command{Use: "approvals", Short: "Decide on writes that wait for a person's approval"}
	c.AddCommand(approvalsListCmd(g), approvalsShowCmd(g), decideCmd(g, "approve"), decideCmd(g, "edit"), decideCmd(g, "reject"))
	return c
}

func approvalsListCmd(g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List the writes waiting for approval",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return withClient(g, func(ctx context.Context, c agentosv1.ControlClient) error {
				resp, err := c.ListPending(ctx, &agentosv1.ListPendingRequest{})
				if err != nil {
					return err
				}
				var items []*agentosv1.PendingItem
				for _, it := range resp.GetItems() {
					if it.GetKind() == "approval" {
						items = append(items, it)
					}
				}
				if g.output == "json" {
					return printJSON(&agentosv1.ListPendingResponse{Items: items})
				}
				tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
				fmt.Fprintln(tw, "APPROVAL ID\tRUN ID\tWRITE\tSINCE\tEXPIRES")
				for _, it := range items {
					fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", safetext.Line(it.GetId()), safetext.Line(it.GetRunId()),
						safetext.Line(it.GetTool()), safetext.Line(it.GetSince()), safetext.Line(it.GetExpiresAt()))
				}
				return tw.Flush()
			})
		},
	}
}

func approvalsShowCmd(g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "show <approval-id>",
		Short: "Show an approval: exactly what will be sent, and what agentd observed about it",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withClient(g, func(ctx context.Context, c agentosv1.ControlClient) error {
				a, err := c.GetApproval(ctx, &agentosv1.GetApprovalRequest{ApprovalId: args[0]})
				if err != nil {
					return err
				}
				if g.output == "json" {
					return printJSON(a)
				}
				return writeApproval(os.Stdout, a)
			})
		},
	}
}

// decideCmd is approve, edit or reject: a decision names the payload the reviewer saw by its
// sha256, which agentd checks against the one waiting (approvals show prints it).
func decideCmd(g *globals, kind string) *cobra.Command {
	var sha, as, key, reason, patchFile string
	short := map[string]string{
		"approve": "Approve a write: the run sends it",
		"edit":    "Approve a write with your own patch: the run checks it, then asks for approval of the result",
		"reject":  "Reject a write, with the reason: the run reports it and sends nothing",
	}[kind]
	use := map[string]string{
		"approve": "approve <approval-id> --payload-sha256 <sha256>",
		"edit":    "edit <approval-id> --payload-sha256 <sha256> --patch-file <file>",
		"reject":  "reject <approval-id> --payload-sha256 <sha256> --reason <text>",
	}[kind]
	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			req := &agentosv1.ResolveApprovalRequest{ApprovalId: args[0], PayloadSha256: sha, IdempotencyKey: key, DecidedBy: as}
			switch kind {
			case "approve":
				req.Decision = &agentosv1.ResolveApprovalRequest_Approve{Approve: &agentosv1.Approve{}}
			case "reject":
				req.Decision = &agentosv1.ResolveApprovalRequest_Reject{Reject: &agentosv1.Reject{Reason: reason}}
			case "edit":
				b, err := os.ReadFile(patchFile)
				if err != nil {
					return fmt.Errorf("--patch-file: %w", err)
				}
				req.Decision = &agentosv1.ResolveApprovalRequest_ApproveWithEdits{ApproveWithEdits: &agentosv1.ApproveWithEdits{EditedPatch: string(b)}}
			}
			if req.DecidedBy == "" {
				req.DecidedBy = reviewer()
			}
			if req.DecidedBy == "" {
				return fmt.Errorf("say who decides: --as <name>")
			}
			if req.IdempotencyKey == "" {
				req.IdempotencyKey = uuid.NewString()
			}
			return withClient(g, func(ctx context.Context, c agentosv1.ControlClient) error {
				resp, err := c.ResolveApproval(ctx, req)
				if err != nil {
					return err
				}
				if g.output == "json" {
					return printJSON(resp)
				}
				fmt.Printf("approval %s: %s by %s at %s\n", safetext.Line(args[0]), safetext.Line(resp.GetStatus()),
					safetext.Line(resp.GetDecidedBy()), safetext.Line(resp.GetDecidedAt()))
				return nil
			})
		},
	}
	cmd.Flags().StringVar(&sha, "payload-sha256", "", "the sha256 of the payload you saw (approvals show prints it)")
	cmd.Flags().StringVar(&as, "as", "", "who decides, as recorded and reported (default: your user name)")
	cmd.Flags().StringVar(&key, "idempotency-key", "", "reuse to make a repeated decision apply once (default: a new key)")
	_ = cmd.MarkFlagRequired("payload-sha256")
	switch kind {
	case "reject":
		cmd.Flags().StringVar(&reason, "reason", "", "why: it goes into the run's report")
		_ = cmd.MarkFlagRequired("reason")
	case "edit":
		cmd.Flags().StringVar(&patchFile, "patch-file", "", "the patch to send instead")
		_ = cmd.MarkFlagRequired("patch-file")
	}
	return cmd
}

// reviewer is the user agentctl runs as: the default reviewer's name.
func reviewer() string {
	if u, err := user.Current(); err == nil && u.Username != "" {
		return u.Username
	}
	return os.Getenv("USER")
}

// writeApproval prints an approval card. Everything in it goes through safetext: the payload,
// the patch and the findings come from a model, a tool or the outside world.
func writeApproval(w io.Writer, a *agentosv1.Approval) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	row := func(name, value string) { fmt.Fprintf(tw, "%s\t%s\n", name, safetext.Line(value)) }
	row("approval", a.GetApprovalId())
	row("run", a.GetRunId())
	row("write", a.GetTool())
	row("status", a.GetStatus())
	row("created", a.GetCreatedAt())
	row("expires", a.GetExpiresAt())
	row("run budget", fmt.Sprintf("%s spent, %s reserved, limit %s", usd(a.GetBudgetSpentMicros()),
		usd(a.GetBudgetReservedMicros()), usd(a.GetBudgetLimitMicros())))
	if a.GetDecidedAt() != "" {
		row("decided", fmt.Sprintf("by %s at %s", a.GetDecidedBy(), a.GetDecidedAt()))
	}
	if a.GetReason() != "" {
		row("reason", a.GetReason())
	}
	if a.GetSupersedes() != "" {
		row("supersedes", a.GetSupersedes()+" (this approval carries its edit)")
	}
	if a.GetSupersededBy() != "" {
		row("superseded by", a.GetSupersededBy())
	}
	row("payload sha256", a.GetPayloadSha256())
	if err := tw.Flush(); err != nil {
		return err
	}
	var ctxv map[string]any
	if err := json.Unmarshal([]byte(a.GetContextJson()), &ctxv); err == nil && len(ctxv) > 0 {
		fmt.Fprintln(w, "\nWhat agentd observed when it made the approval:")
		writeTree(w, ctxv, "  ")
	}
	if a.GetEditedPatch() != "" {
		fmt.Fprintln(w, "\nThe edited patch:")
		writeBlock(w, a.GetEditedPatch(), "  ")
	}
	fmt.Fprintln(w, "\nExactly what will be sent (the payload):")
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, []byte(a.GetPayloadJson()), "  ", "  "); err != nil {
		pretty.WriteString(a.GetPayloadJson())
	}
	fmt.Fprintln(w, "  "+safetext.Text(pretty.String()))
	if a.GetStatus() == "PENDING" {
		id, sum := safetext.Line(a.GetApprovalId()), safetext.Line(a.GetPayloadSha256())
		fmt.Fprintf(w, "\nTo decide on this payload:\n  agentctl approvals approve %s --payload-sha256 %s\n", id, sum)
		fmt.Fprintf(w, "  agentctl approvals reject %s --payload-sha256 %s --reason '...'\n", id, sum)
		fmt.Fprintf(w, "  agentctl approvals edit %s --payload-sha256 %s --patch-file <file>\n", id, sum)
	}
	return nil
}

// writeTree prints a JSON value as an indented outline, keys in order: what agentd observed
// differs by write and by task, and agentctl knows none of them.
func writeTree(w io.Writer, v any, indent string) {
	switch v := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			writeEntry(w, safetext.Line(k)+":", v[k], indent)
		}
	case []any:
		for _, item := range v {
			writeEntry(w, "-", item, indent)
		}
	default:
		fmt.Fprintln(w, indent+scalar(v))
	}
}

func writeEntry(w io.Writer, label string, v any, indent string) {
	switch v := v.(type) {
	case map[string]any, []any:
		if empty(v) {
			fmt.Fprintf(w, "%s%s (none)\n", indent, label)
			return
		}
		fmt.Fprintln(w, indent+label)
		writeTree(w, v, indent+"  ")
	case string:
		if strings.Contains(v, "\n") {
			fmt.Fprintln(w, indent+label)
			writeBlock(w, v, indent+"  ")
			return
		}
		fmt.Fprintf(w, "%s%s %s\n", indent, label, safetext.Line(v))
	default:
		fmt.Fprintf(w, "%s%s %s\n", indent, label, scalar(v))
	}
}

func empty(v any) bool {
	switch v := v.(type) {
	case map[string]any:
		return len(v) == 0
	case []any:
		return len(v) == 0
	}
	return false
}

func scalar(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "?"
	}
	if s, ok := v.(string); ok {
		return safetext.Line(s)
	}
	return safetext.Line(string(b))
}

// writeBlock prints lines of text, indented: a patch reads as it was written, with no raw
// control characters.
func writeBlock(w io.Writer, s, indent string) {
	for _, l := range strings.Split(strings.TrimSuffix(s, "\n"), "\n") {
		fmt.Fprintln(w, indent+safetext.Text(l))
	}
}
