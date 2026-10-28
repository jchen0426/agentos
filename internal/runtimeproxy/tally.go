package runtimeproxy

import (
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
)

// Tally is what the proxy sees of one generation of agentd's own Worker, for the supervisor's
// liveness checks (M5 C2c): its polls that the rules let through and that are still open, when the
// last of them ended, and since when a task handed out to it has waited for an answer. It counts the
// calls of the connections the proxy accepted after the tally was made (Server.NewTally), so a
// connection of an earlier generation (a dead one's unanswered query, say) neither trips nor masks
// it; a Worker of an earlier generation that connects again afterwards counts as this one. A nil
// Tally counts nothing.
type Tally struct {
	mu         sync.Mutex
	polls      int       // open polls
	quietSince time.Time // when the last poll ended, or the tally began
	heldSince  time.Time // the first task handed out since the Worker's last answer; zero for none
}

// NewTally makes a fresh tally, which the connections accepted from now on count into: the
// supervisor makes one just before it starts a generation.
func (s *Server) NewTally() *Tally {
	t := &Tally{quietSince: time.Now()}
	s.tally.Store(t)
	return t
}

// Liveness is the tally: the polls open, since when none has been, and since when a task handed out
// has waited for an answer (zero for none).
func (t *Tally) Liveness() (polls int, quietSince, heldSince time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.polls, t.quietSince, t.heldSince
}

func (t *Tally) pollStarted() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.polls++
}

func (t *Tally) pollEnded() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.polls--; t.polls == 0 {
		t.quietSince = time.Now()
	}
}

// handedOut counts a task Temporal handed out on a poll: the first since the last answer starts
// the wait for one.
func (t *Tally) handedOut() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.heldSince.IsZero() {
		t.heldSince = time.Now()
	}
}

// answered counts an answer of the Worker's, a task's result, whether the rules let it through or
// not: the Worker answered.
func (t *Tally) answered() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.heldSince = time.Time{}
}

// answers are the methods that answer a task handed out on a poll.
var answers = map[string]bool{
	service + "RespondWorkflowTaskCompleted": true,
	service + "RespondWorkflowTaskFailed":    true,
	service + "RespondQueryTaskCompleted":    true,
}

const pollMethod = service + "PollWorkflowTaskQueue"

// handsOut reports whether raw, a PollWorkflowTaskQueueResponse on the wire, hands out a task: its
// task_token (field 1) is not empty. A long poll that found nothing answers with an empty response.
func handsOut(raw []byte) bool {
	for len(raw) > 0 {
		num, typ, n := protowire.ConsumeTag(raw)
		if n < 0 {
			return false
		}
		raw = raw[n:]
		if num == 1 && typ == protowire.BytesType {
			v, m := protowire.ConsumeBytes(raw)
			return m >= 0 && len(v) > 0
		}
		m := protowire.ConsumeFieldValue(num, typ, raw)
		if m < 0 {
			return false
		}
		raw = raw[m:]
	}
	return false
}
