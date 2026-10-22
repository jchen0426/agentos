package audit

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jchen0426/agentos/internal/ledger"
)

// Queue appends events in the background, for a path that must not wait for the ledger: the Runtime
// proxy's refusals, which come on the Worker's requests. It holds at most its size; an event that
// finds it full is dropped, and the next event appended says how many were ("dropped").
type Queue struct {
	l       *ledger.Ledger
	events  chan Event
	dropped atomic.Int64
	onError func(error)
}

// NewQueue is a queue of size events for l; onError hears of each append that fails.
func NewQueue(l *ledger.Ledger, size int, onError func(error)) *Queue {
	return &Queue{l: l, events: make(chan Event, size), onError: onError}
}

// Add queues e, whose payload must be a map, without waiting.
func (q *Queue) Add(e Event) {
	select {
	case q.events <- e:
	default:
		q.dropped.Add(1)
	}
}

// Run appends what is queued until ctx ends, and then what is left, for at most a few seconds.
func (q *Queue) Run(ctx context.Context) {
	for {
		select {
		case e := <-q.events:
			// Taken, it is appended, whether ctx ended meanwhile or not: within drainFor.
			one, cancel := context.WithTimeout(context.WithoutCancel(ctx), drainFor)
			q.append(one, e)
			cancel()
		case <-ctx.Done():
			q.drain(ctx)
			return
		}
	}
}

// drain appends what is left, for at most drainFor. What it cannot is dropped, and said (onError):
// no record comes after it to say so.
func (q *Queue) drain(ctx context.Context) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), drainFor)
	defer cancel()
	for {
		select {
		case e := <-q.events:
			if ctx.Err() != nil {
				q.dropped.Add(1)
				continue
			}
			q.append(ctx, e)
		default:
			if n := q.dropped.Load(); n > 0 && q.onError != nil {
				q.onError(fmt.Errorf("%d events were dropped, and are not in the audit log", n))
			}
			return
		}
	}
}

// drainFor bounds an append, and how long a queue appends what is left once its Run's context ends:
// a ledger that does not answer does not hold up agentd's shutdown. An append that takes longer is
// dropped, and counted.
var drainFor = 5 * time.Second

func (q *Queue) append(ctx context.Context, e Event) {
	var n int64
	if p, ok := e.Payload.(map[string]any); ok {
		if n = q.dropped.Swap(0); n > 0 {
			p["dropped"] = n
		}
	}
	if _, err := Record(ctx, q.l, e); err != nil {
		q.dropped.Add(n + 1) // said by the next one
		if q.onError != nil {
			q.onError(err)
		}
	}
}
