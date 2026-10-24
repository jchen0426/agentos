package explain

import (
	"fmt"
	"regexp"
)

// Why a workflow says it reports: the only words of its own a report takes from it.
const (
	ReasonDone          = "done"
	ReasonWorkflowError = "workflow_error" // its own code failed: it reports, then fails the run
)

// ErrorType is what an exception's type a workflow names may look like: an identifier.
var ErrorType = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.]{0,63}$`)

// PublicErrorTypes are the exception types a report names, when the workflow says its own code failed
// with one: Python's built-in exceptions (not its warnings), and json's JSONDecodeError, which the
// workflow's own code can raise besides. The set is fixed, not a pattern: a subverted Worker chooses
// the name, and a report is public, so a name it could choose freely would carry whatever it likes out
// of agentd, a few dozen bytes a report. With this set it chooses one of these names, or none. Any other
// name, and none, is said as "an exception of another type". runs explain, which is local, quotes the
// workflow's own words (its failure in Temporal) as its claim.
var PublicErrorTypes = map[string]bool{
	"Exception": true, "ArithmeticError": true, "FloatingPointError": true, "OverflowError": true,
	"ZeroDivisionError": true, "AssertionError": true, "AttributeError": true, "BufferError": true, "EOFError": true,
	"ExceptionGroup": true, "ImportError": true, "ModuleNotFoundError": true, "LookupError": true, "IndexError": true,
	"KeyError": true, "MemoryError": true, "NameError": true, "UnboundLocalError": true, "OSError": true,
	"BlockingIOError": true, "ChildProcessError": true, "ConnectionError": true, "BrokenPipeError": true,
	"ConnectionAbortedError": true, "ConnectionRefusedError": true, "ConnectionResetError": true,
	"FileExistsError": true, "FileNotFoundError": true, "InterruptedError": true, "IsADirectoryError": true,
	"NotADirectoryError": true, "PermissionError": true, "ProcessLookupError": true, "TimeoutError": true,
	"ReferenceError": true, "RuntimeError": true, "NotImplementedError": true, "RecursionError": true,
	"StopAsyncIteration": true, "StopIteration": true, "SyntaxError": true, "IndentationError": true, "TabError": true,
	"SystemError": true, "TypeError": true, "ValueError": true, "UnicodeError": true, "UnicodeDecodeError": true,
	"UnicodeEncodeError": true, "UnicodeTranslateError": true,
	"JSONDecodeError": true, // json's: the workflow's code reads JSON
}

// Outcome is what a run's report says of the run: the verdict Explain gives a run that ends with
// this report, by the same rules over the run's history and agentd's records, the report's own calls
// left out (a report cannot report its own posting, which Explain, once the run has ended, covers
// too). The workflow says only why it reports: done, or that its own code failed (with its
// exception's type), which is its claim.
type Outcome struct {
	Verdict, Kind string
	// Step is the step the verdict names; Named says it is one of the task's own names (D12), as
	// against a call's kind and name, which the workflow chose.
	Step  string
	Named bool
	Cause string // agentd's words, which quote tools, models and people
	Claim string // what the workflow said of itself, as its own
	// Cut is the evidence of the run agentd did not read, past its bounds, besides what the verdict
	// rests on (the history, and agentd's record of each call's end, which agentd always reads: what
	// it cannot makes the verdict unknown): it does not change the verdict, only how much its cause
	// says.
	Cut []string
	// Detail is the error that made the verdict unknown, for agentd's log: the report does not show
	// it, as an internal error's text can name hosts, ports, TLS settings and the namespace.
	Detail string
}

// unread is the cause a report gives a verdict it does not know for want of the run's history.
const unreadCause = "agentd could not read the run in Temporal; " + inTheLog

// Report is the outcome of a run that reports now: in is what Temporal has of it (ReadTemporal), read
// reads the ledger as Explain does, and reason and errorType are why the workflow says it reports.
// Without the run's history the verdict is unknown, never ok: it is what a report rests on, when
// Temporal says it has none (past its retention), or answers its read with an error of its own
// (in.Unread, or as the history is read). A history Temporal could not be reached for, as it was
// read, fails it (Unreachable says so), as does a ledger that cannot be read: a report is rendered
// once, and should wait for the run's history rather than say for good that it was not there. A report
// is public: it shows no internal error's text, only that agentd's log has it.
func Report(in Input, read func(Wanted) (Ledger, error), reason, errorType string) (Outcome, error) {
	if in.Unread != "" {
		return unknownOutcome(unreadCause, in.Unread), nil
	}
	var ledgerErr error
	f, err := gather(in, func(w Wanted) (Ledger, error) {
		l, err := read(w)
		ledgerErr = err
		return l, err
	})
	switch {
	case ledgerErr != nil:
		return Outcome{}, ledgerErr
	case err != nil && Unreachable(err):
		return Outcome{}, err
	case err != nil:
		return unknownOutcome(unreadCause, err.Error()), nil
	case !f.history:
		return unknownOutcome("Temporal has no history of the run", ""), nil
	}
	f.public = true
	var steps []*step
	for _, s := range f.steps {
		if !s.isReport {
			steps = append(steps, s)
		}
	}
	f.steps = steps
	o := report(f, reason, errorType)
	o.Cut = f.l.CutWhat
	return bound(o), nil
}

// unknownOutcome is the outcome of a run whose history agentd could not read: cause is what the
// report says, detail what agentd's log does.
func unknownOutcome(cause, detail string) Outcome {
	return bound(Outcome{Verdict: VerdictUnknown, Kind: KindHistoryUnavailable, Cause: cause, Detail: detail})
}

// bound makes the outcome's texts one line of printable text, bounded, as an explanation's: the
// report fences them all the same.
func bound(o Outcome) Outcome {
	o.Step, o.Cause, o.Claim = line(o.Step, maxName), line(o.Cause, maxText), line(o.Claim, maxText)
	return o
}

func report(f *facts, reason, errorType string) Outcome {
	if reason == ReasonWorkflowError {
		name := errorType
		if !PublicErrorTypes[name] {
			name = "an exception of another type"
		}
		o := Outcome{Verdict: VerdictFailed, Kind: KindWorkflowError, Claim: fmt.Sprintf("the workflow reports that its own "+
			"code failed (%s), and fails the run once this report is out", name),
			Cause: "by the workflow's own account, its code failed before it made a call"}
		if s := f.lastStep(); s != nil {
			o.Step, o.Named = s.step, s.mapped
			o.Cause = fmt.Sprintf("by the workflow's own account, its code failed after %s %s", s.step, s.outcome())
		}
		return o
	}
	j := f.judge()
	o := Outcome{Verdict: j.verdict, Kind: j.kind, Cause: j.cause, Step: j.step, Named: j.named}
	if j.verdict == VerdictOK {
		o.Cause = "every call before the report went well"
	}
	return o
}
