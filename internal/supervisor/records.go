package supervisor

import (
	"time"

	"github.com/jchen0426/agentos/internal/audit"
)

// exit is how a generation ended, without agentd's doing.
type exit struct {
	generation, pid int
	code            int    // its exit code, when it exited
	signal          string // or the signal that killed it
	startError      string // or why it never started
	ran             time.Duration
	ready           bool
	at              time.Time     // when it ended
	restartIn       time.Duration // the delay before the next generation; 0 for none (agentd stops)
	unresponsive    string        // the liveness rule it broke, and agentd stopped it for: not_polling, not_answering
	unresponsiveFor time.Duration // how long it had then
	oomKill         bool          // the kernel's out-of-memory killer killed it (Options.Limits)
}

// Event is the audit record of e: a record of agentd's own (no run's), with agentd's own words and
// numbers only, none of the Worker's. unrecorded counts the exits since the previous record that
// no record describes. Its keys are not the record fields audit.Projection reads, of any type.
func (e exit) Event(unrecorded int) audit.Event {
	p := map[string]any{"generation": e.generation, "pid": e.pid, "ran_ms": e.ran.Milliseconds(), "ready": e.ready,
		"exited_at": e.at.UTC().Format(time.RFC3339Nano), "unrecorded": unrecorded}
	switch {
	case e.startError != "":
		p["start_error"] = audit.Text(e.startError)
	case e.signal != "":
		p["signal"] = e.signal
	default:
		p["exit_code"] = e.code
	}
	if e.restartIn > 0 {
		p["restart_in_ms"] = e.restartIn.Milliseconds()
	}
	if e.unresponsive != "" {
		p["unresponsive"], p["unresponsive_ms"] = e.unresponsive, e.unresponsiveFor.Milliseconds()
	}
	if e.oomKill {
		p["oom_kill"] = true
	}
	return audit.Event{Type: ExitedType, Actor: audit.Agentd, Payload: p}
}

// records writes the audit records of the Worker's exits, at most one every so often: an exit with
// none for that long is recorded at once; later ones are held, and when the time is up one record
// describes the latest and counts the rest. Only the supervisor's loop uses it.
type records struct {
	every      time.Duration
	record     func(audit.Event)
	last       time.Time // when the last record was written; zero for none yet
	held       *exit     // the latest exit not recorded yet
	unrecorded int       // the exits before it since the last record, which no record will describe
	timer      *time.Timer
}

// exit records e, or holds it.
func (r *records) exit(e exit) {
	if r.record == nil {
		return
	}
	if r.held != nil {
		r.unrecorded++
	}
	r.held = &e
	now := time.Now()
	switch {
	case r.last.IsZero() || now.Sub(r.last) >= r.every:
		r.write(now)
	case r.timer == nil:
		r.timer = time.NewTimer(r.last.Add(r.every).Sub(now))
	}
}

// due fires when what is held may be recorded; nil while nothing is.
func (r *records) due() <-chan time.Time {
	if r.timer == nil {
		return nil
	}
	return r.timer.C
}

// fire records what is held, once due fired.
func (r *records) fire() {
	r.timer = nil
	if r.held != nil {
		r.write(time.Now())
	}
}

// flush records what is held, as the supervisor stops.
func (r *records) flush() {
	if r.held != nil {
		r.write(time.Now())
	}
}

func (r *records) write(now time.Time) {
	r.record(r.held.Event(r.unrecorded))
	r.last, r.held, r.unrecorded = now, nil, 0
	if r.timer != nil {
		r.timer.Stop()
		r.timer = nil
	}
}
