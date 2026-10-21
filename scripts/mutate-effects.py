#!/usr/bin/env python3
"""The mutation check of the effect protocol's state machine (docs/statemachine-notes.md).

Each mutation puts back a bug that a review of M4 (docs/m4-notes.md, rounds 3 to 5) or the state
machine found, as an exact edit of the code at the commit that added this script; the state machine
must then fail on its default seeds and regressionSeeds. The script applies one mutation at a time,
runs `go test ./internal/gateway -run TestEffectProtocolStateMachine`, prints whether it was caught
(the first failing seed and the broken invariant) and restores the files with `git checkout`.

Run it from the repository root, with nothing uncommitted under internal/:

    python3 scripts/mutate-effects.py            # all mutations
    python3 scripts/mutate-effects.py R4 S2      # those whose names begin so

An edit that no longer matches exactly once stops the script: the code moved on, and the mutation
needs writing again against it."""
import re
import subprocess
import sys

G = "internal/gateway/"
SQL = "internal/ledger/db/ledger.sql.go"

ABANDON_UNCLAIMED = (SQL,
    "UPDATE effects SET status = 'ABANDONED', reason = ?, version = version + 1, updated_at = ?\n"
    "WHERE effect_id = ? AND version = ? AND status = 'PENDING' AND claimed_by IS NULL AND maybe_sent = 0\n",
    "UPDATE effects SET status = 'ABANDONED', reason = ?, version = version + 1, updated_at = ?\n"
    "WHERE effect_id = ? AND version = ? AND status = 'PENDING' AND claimed_by IS NULL\n")
HOLD = (SQL,
    "UPDATE effects SET status = 'AWAITING_APPROVAL', version = version + 1, updated_at = ?\n"
    "WHERE effect_id = ? AND version = ? AND status = 'PENDING' AND claimed_by IS NULL AND maybe_sent = 0\n",
    "UPDATE effects SET status = 'AWAITING_APPROVAL', version = version + 1, updated_at = ?\n"
    "WHERE effect_id = ? AND version = ? AND status = 'PENDING' AND claimed_by IS NULL\n")
NO_BOUND_AT_PROCEED = (G + "effects.go", "\t\tif bound != nil && expired(*bound) {\n\t\t\treturn tools.ErrStopped",
                       "\t\tif false {\n\t\t\treturn tools.ErrStopped")

MUTATIONS = {
    # Round 3, 1: a write that may have gone out is taken for one that did not (no maybe_sent).
    "R3.1a proceed never marks maybe_sent": [
        (SQL, "UPDATE effects SET claimed_at = ?, last_request_at = ?, maybe_sent = 1, updated_at = ?",
              "UPDATE effects SET claimed_at = ?, last_request_at = ?, maybe_sent = maybe_sent, updated_at = ?")],
    "R3.1b a write that may be out, never approved, is held for approval": [
        (G + "approvals.go",
         "\tcase e.MaybeSent != 0:\n\t\treturn g.settle(ctx, e, effectUnknown, nil, \"claimed before the policy asked for approval, and it may have gone out: \"+\n"
         "\t\t\t\"agentd does not send it again without an approval\")\n", ""),
        HOLD],
    "R3.1c gate takes a write that may be out for one that is not": [
        (G + "approvals.go", "\ta, err := g.Ledger.Q.GetApprovalByEffect(ctx, e.EffectID)\n\tapproved := err == nil",
         "\te.MaybeSent = 0\n\ta, err := g.Ledger.Q.GetApprovalByEffect(ctx, e.EffectID)\n\tapproved := err == nil"),
        ABANDON_UNCLAIMED, HOLD],
    # Round 3, 2: an approval held only until the run acted on it, not until the write took effect.
    "R3.2 an approved write that has not taken effect goes out past its time": [
        (G + "approvals.go", "\tcase approved:\n\t\treturn g.expireUnsent(ctx, e, a)\n", "\tcase approved:\n\t\treturn nil, nil\n"),
        NO_BOUND_AT_PROCEED],
    "R3.3 recovery sends a write a lookup found nothing of again past its approval's time": [
        (G + "effects.go",
         "\tbound, err := g.bound(ctx, e)\n\tswitch {\n\tcase err != nil:\n\t\treturn nil, err\n\tcase bound != nil && expired(*bound):\n"
         "\t\treturn g.release(ctx, e, true)\n\t}\n\treturn g.resend(ctx, run, t, e, payload, bound)",
         "\treturn g.resend(ctx, run, t, e, payload, nil)")],
    # Round 4.
    "R4.1 a write the tool did nothing with is given back still marked maybe_sent": [
        (G + "effects.go", "g.giveBack(ctx, e, e.MaybeSent == 0)", "g.giveBack(ctx, e, false)")],
    "R4.2 recovery checks the approval's time before it waits, not when it sends again": [
        (G + "effects.go",
         "\tfinder, canFind := t.(tools.Finder)\n\tswitch class := e.EffectClass; {",
         "\tearly := false\n\tif a, aerr := g.Ledger.Q.GetApprovalByEffect(ctx, e.EffectID); aerr == nil {\n\t\tearly = expired(a)\n\t}\n"
         "\tfinder, canFind := t.(tools.Finder)\n\tswitch class := e.EffectClass; {"),
        (G + "effects.go",
         "\tcase !mayResend:\n\t\treturn g.release(ctx, e, true)\n\t}\n\treturn g.resendNew(ctx, run, t, e, payload)\n",
         "\tcase !mayResend, early:\n\t\treturn g.release(ctx, e, true)\n\t}\n\te.MaybeSent = 0\n\treturn g.resend(ctx, run, t, e, payload, nil)\n")],
    "R4.3 the expiry of a write some request of which went out does not say so": [
        (G + "approvals.go",
         "\tif e.LastRequestAt.Valid {\n\t\t// A write of several requests may have made some, which stay (a branch, for a pull\n"
         "\t\t// request never opened).\n"
         "\t\treason += fmt.Sprintf(\". A request of it went out at %s, and what that request made may remain\", e.LastRequestAt.String)\n\t}\n",
         "")],
    # Round 5.
    "R5.1 an unknown outcome is recovered by maybe_sent as the attempt began": [
        (G + "effects.go",
         "\tcur, rerr := g.Ledger.Q.GetEffect(ctx, e.EffectID)\n\tif rerr != nil {\n\t\treturn nil, rerr\n\t}\n\te.MaybeSent = cur.MaybeSent\n", "")],
    "R5.1b an unknown outcome counts as in flight though no request went out": [
        (G + "effects.go", "\te.MaybeSent = cur.MaybeSent\n", "\te.MaybeSent = 1 + 0*cur.MaybeSent\n")],
    "R5.2 recovery sends an idempotent write nothing of which went out as if in flight": [
        (G + "effects.go", "case class == policy.EffectIdempotent && !cancelled && mayResend && e.MaybeSent != 0:",
         "case class == policy.EffectIdempotent && !cancelled && mayResend:")],
    "R5.3 proceed does not record the time of the request (last_request_at)": [
        (G + "effects.go", "ClaimedAt: valid(now), LastRequestAt: valid(now), UpdatedAt: now,",
         "ClaimedAt: valid(now), LastRequestAt: e.LastRequestAt, UpdatedAt: now,")],
    "R5.4 an approved write moves to PENDING past its approval's time": [
        (G + "approvals.go", "\t\tif expired(cur) {\n\t\t\treason := fmt.Sprintf(\"approved by %s at %s, but the run did not act on it",
         "\t\tif false {\n\t\t\treason := fmt.Sprintf(\"approved by %s at %s, but the run did not act on it")],
    # The bugs this state machine found (the fixes, undone).
    "S1 an approved write that only dry-ran reads as approved": [
        (G + "records.go", "effectDenied: \"not sent\", effectDryRun: \"not sent\",", "effectDenied: \"not sent\",")],
    "S2 a write that may have taken effect is recorded as one that did not": [
        (G + "effects.go", "\tif e.MaybeSent != 0 {\n\t\tstatus, reason = effectUnknown,", "\tif false {\n\t\tstatus, reason = effectUnknown,")],
    "S2b agentd abandons at start a cancelled run's write that may have taken effect": [
        (SQL, "WHERE status = 'PENDING' AND claimed_by IS NULL AND maybe_sent = 0\n  AND workflow_id IN",
              "WHERE status = 'PENDING' AND claimed_by IS NULL\n  AND workflow_id IN")],
    "S3 a new request goes out once the approval ran out while the tool prepared it": [NO_BOUND_AT_PROCEED],
    "S4 a finding that a lookup write never happened is taken at once for a run that is over": [
        (G + "effects.go",
         """		// The request may still land while GitHub works on it: the finding can wait, whatever
		// becomes of the effect then. A claim holds when its last request went out; an effect
		// given back has no claim, and its record holds it.
		if f, ok := t.(tools.Finder); ok {
			if sent := cmp.Or(e.ClaimedAt, e.LastRequestAt); sent.Valid {
				if sentAt, err := time.Parse(time.RFC3339Nano, sent.String); err == nil {
					if until := sentAt.Add(f.Settle()); ledger.Clock().Before(until) {
						return "", "", fmt.Errorf("%w: the request may still take effect until %s; look again then",
							ErrTooEarly, until.UTC().Format(time.RFC3339))
					}
				}
			}
		}
		cancelled, err := g.runCancelled(ctx, e.WorkflowID)
		if err != nil {
			return "", "", err
		}
		if cancelled || r.RunClosed {
			status = effectAbandoned
		}
""",
         """		cancelled, err := g.runCancelled(ctx, e.WorkflowID)
		if err != nil {
			return "", "", err
		}
		if cancelled || r.RunClosed {
			status = effectAbandoned
		} else if f, ok := t.(tools.Finder); ok && e.ClaimedAt.Valid {
			if sentAt, err := time.Parse(time.RFC3339Nano, cmp.Or(e.ClaimedAt.String)); err == nil { // cmp stays in use
				if until := sentAt.Add(f.Settle()); ledger.Clock().Before(until) {
					return "", "", fmt.Errorf("%w: the request may still take effect until %s; look again then",
						ErrTooEarly, until.UTC().Format(time.RFC3339))
				}
			}
		}
""")],
}


def apply(edits):
    for path, old, new in edits:
        text = open(path).read()
        n = text.count(old)
        if n != 1:
            sys.exit(f"{path}: {n} matches for {old!r}")
        open(path, "w").write(text.replace(old, new))


def main():
    only = sys.argv[1:]
    if subprocess.run(["git", "diff", "--quiet"]).returncode != 0:
        sys.exit("the worktree has changes: commit them first")
    for name, edits in MUTATIONS.items():
        if only and not any(name.startswith(o) for o in only):
            continue
        apply(edits)
        try:
            run = subprocess.run(["go", "test", "./internal/gateway", "-run", "TestEffectProtocolStateMachine", "-count=1"],
                                 capture_output=True, text=True, timeout=900)
            out = run.stdout + run.stderr
            failures = re.findall(r"seed (\d+): (\S.*)", out)
            first = [f for f in failures if re.match(r"\d", f[1]) or f[1].startswith(("approval", "recovery"))]
            if run.returncode == 0:
                print(f"SURVIVED  {name}")
            elif first:
                print(f"caught    {name}: seed {first[0][0]}: {first[0][1][:150]}")
            else:
                print(f"FAILED?   {name}: {out.strip().splitlines()[-1][:200] if out.strip() else ''}")
                print("\n".join(out.splitlines()[:15]))
        finally:
            subprocess.run(["git", "checkout", "--", "internal"], check=True)
        sys.stdout.flush()


main()
