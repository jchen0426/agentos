package explain

import (
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	enumspb "go.temporal.io/api/enums/v1"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/safetext"
)

// Bounds on what an explanation holds: the Worker shapes a history, and tools, models and people
// write the texts in it.
const (
	maxText             = 1000 // bytes of a cause, a claim or what a piece of evidence shows, once escaped
	maxName             = 200  // bytes of a step's, a call's or an activity's name, once escaped
	maxSources          = 12
	maxIncidents        = 50
	maxIncidentEvidence = 6
	maxUsage            = 20
)

// finish bounds the explanation and makes every text in it one line of printable text: whatever
// the Worker, a tool, a model or a person wrote, the verdict's own words around it included.
func finish(e *agentosv1.Explanation) {
	e.Step, e.Call, e.ActivityId = line(e.Step, maxName), line(e.Call, maxName), line(e.ActivityId, maxName)
	e.Kind, e.Verdict, e.State = line(e.Kind, maxName), line(e.Verdict, maxName), line(e.State, maxName)
	e.Cause, e.Claim, e.Next = line(e.Cause, maxText), line(e.Claim, maxText), line(e.Next, maxText)
	e.Sources = bounded(dedupe(e.Sources), maxSources)
	e.Evidence = nil
	for _, s := range e.Sources {
		e.Evidence = append(e.Evidence, render(s))
	}
	if n := len(e.Incidents); n > maxIncidents {
		e.IncidentsOmitted = int32(n - maxIncidents)
		e.Incidents = e.Incidents[:maxIncidents]
	}
	for _, inc := range e.Incidents {
		inc.Step, inc.ActivityId, inc.Kind = line(inc.Step, maxName), line(inc.ActivityId, maxName), line(inc.Kind, maxName)
		inc.Cause, inc.At = line(inc.Cause, maxText), line(inc.At, maxName)
		inc.Evidence = bounded(dedupe(inc.Evidence), maxIncidentEvidence)
	}
	if len(e.Usage) > maxUsage {
		e.Usage = e.Usage[:maxUsage]
	}
	for _, u := range e.Usage {
		u.Model = line(u.Model, maxName)
	}
	for i, m := range e.Missing {
		e.Missing[i] = line(m, maxText)
	}
}

// line is s as one line of printable text (safetext.Line), each run of spaces one space, at most n
// bytes: cut at a rune, and marked with an ellipsis. A run of spaces in a Worker's or a tool's text
// could pad it out to where a terminal wraps it, into what reads as a line of agentctl's own.
func line(s string, n int) string {
	s = spaces.ReplaceAllString(safetext.Line(s), " ")
	if len(s) <= n {
		return s
	}
	const ellipsis = "…"
	cut := n - len(ellipsis)
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + ellipsis
}

// spaces is a run of spaces: safetext.Line leaves only the ASCII space unescaped.
var spaces = regexp.MustCompile(` {2,}`)

// bounded is the first n of evidence, each made one line.
func bounded(ev []*agentosv1.Evidence, n int) []*agentosv1.Evidence {
	if len(ev) > n {
		ev = ev[:n]
	}
	for _, x := range ev {
		x.What, x.Table, x.Key = line(x.What, maxText), line(x.Table, maxName), line(x.Key, maxName)
	}
	return ev
}

// dedupe drops evidence that points where an earlier piece does.
func dedupe(ev []*agentosv1.Evidence) []*agentosv1.Evidence {
	type where struct {
		source     string
		event, seq int64
		table, key string
	}
	seen := map[where]bool{}
	var out []*agentosv1.Evidence
	for _, x := range ev {
		w := where{x.Source, x.EventId, x.Seq, x.Table, x.Key}
		if x.Source == sourceTemporal && x.EventId == 0 {
			w.key = x.What // Temporal's description of the execution now: no event of its own
		}
		if seen[w] {
			continue
		}
		seen[w] = true
		out = append(out, x)
	}
	return out
}

// Where evidence is.
const (
	sourceTemporal = "temporal"
	sourceAudit    = "audit"
	sourceLedger   = "ledger"
)

func auditSource(seq int64, what string) *agentosv1.Evidence {
	return &agentosv1.Evidence{Source: sourceAudit, Seq: seq, What: what}
}

// temporalSource is an event of the run's history; event 0 is what Temporal says of the execution
// now (DescribeWorkflowExecution).
func temporalSource(event int64, what string) *agentosv1.Evidence {
	return &agentosv1.Evidence{Source: sourceTemporal, EventId: event, What: what}
}

func ledgerSource(table, key, what string) *agentosv1.Evidence {
	return &agentosv1.Evidence{Source: sourceLedger, Table: table, Key: key, What: what}
}

// render is a piece of evidence as one of Explanation's lines, for clients that show only those.
func render(x *agentosv1.Evidence) string {
	switch x.Source {
	case sourceTemporal:
		if x.EventId == 0 {
			return "Temporal, now: " + x.What
		}
		return fmt.Sprintf("Temporal event %d: %s", x.EventId, x.What)
	case sourceAudit:
		return fmt.Sprintf("audit log seq %d: %s", x.Seq, x.What)
	}
	return fmt.Sprintf("ledger %s %s: %s", x.Table, x.Key, x.What)
}

// eventName is an event type as Temporal's Web UI names it: ActivityTaskCompleted.
func eventName(t enumspb.EventType) string { return t.String() }

// dollars shows micros as dollars, exactly: $5.00, $0.0045.
func dollars(micros int64) string {
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

// cents is micros rounded up to a cent, as agentctl budget extend --usd takes an amount: 0.03.
func cents(micros int64) string {
	c := (micros + 9_999) / 10_000
	return fmt.Sprintf("%d.%02d", c/100, c%100)
}

// stampOrEmpty is when an incident was, fixed-width so that incidents sort by it (ledger.Stamp);
// empty when nothing says.
func stampOrEmpty(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return ledger.Stamp(t)
}
