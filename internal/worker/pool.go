// Pool spreads work across a fixed set of workers behind one dispatch point.
package worker

import (
	"context"
	"log/slog"
	"time"
)

// Pool is a fixed set of persistent workers, each owning one long-lived handler
// process that serves up to cap messages at once. Dispatch places a message on
// the worker with the fewest in flight, blocking when every worker is at cap.
type Pool struct {
	workers []*worker
	log     *slog.Logger
}

// NewPool builds a pool of size workers, each allowed capacity messages in
// flight (size or capacity < 1 is treated as 1). It does not spawn anything;
// call Start.
func NewPool(cfg Config, pub Publisher, size, capacity int, log *slog.Logger) *Pool {
	if log == nil {
		log = slog.Default()
	}
	if size < 1 {
		size = 1
	}
	if capacity < 1 {
		capacity = 1
	}
	workers := make([]*worker, size)
	for i := range workers {
		workers[i] = newWorker(i, cfg, pub, capacity, log)
	}
	return &Pool{workers: workers, log: log}
}

// Start launches each worker's supervisor, which spawns its process and keeps
// it alive. It returns immediately; Dispatch waits for the processes to come up.
func (p *Pool) Start(ctx context.Context) {
	for _, w := range p.workers {
		go w.run(ctx)
	}
}

// Dispatch places d on the least-loaded worker, blocking until a slot frees or
// ctx is cancelled. It returns false (with d untouched) only if ctx is
// cancelled before a worker can take it. The engine consumes serially, so it is
// the only caller; a worker's load only falls (as dones arrive) between the
// pick and the handoff, never rises from another dispatcher.
func (p *Pool) Dispatch(ctx context.Context, d Delivery) bool {
	return p.leastLoaded().Dispatch(ctx, d)
}

func (p *Pool) leastLoaded() *worker {
	var best *worker = p.workers[0]
	var bestN int = best.outstanding()
	for _, w := range p.workers[1:] {
		var n int = w.outstanding()
		if n < bestN {
			best, bestN = w, n
		}
	}
	return best
}

// Shutdown stops the pool: it asks every worker to finish in-flight work and
// exit (closing stdin), waits up to grace for them, then SIGKILLs any that
// remain. Messages a killed worker never finished are nacked (dropped).
func (p *Pool) Shutdown(grace time.Duration) {
	for _, w := range p.workers {
		w.beginClose()
	}
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	ctx, cancel = context.WithTimeout(context.Background(), grace)
	defer cancel()
	for _, w := range p.workers {
		select {
		case <-w.stoppedCh:
		case <-ctx.Done():
			w.kill()
			<-w.stoppedCh
		}
	}
}
