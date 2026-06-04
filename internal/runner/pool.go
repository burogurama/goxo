// Bounded concurrency for the ephemeral runner.
package runner

import (
	"context"
	"sync"
)

// Pool bounds how many handler processes run at once. Submit blocks until a
// slot frees, then handles the delivery on its own goroutine.
type Pool struct {
	runner *Runner
	sem    chan struct{}
	wg     sync.WaitGroup
}

// NewPool bounds the pool to max concurrent handlers (max < 1 is treated as
// 1).
func NewPool(r *Runner, max int) *Pool {
	if max < 1 {
		max = 1
	}
	return &Pool{runner: r, sem: make(chan struct{}, max)}
}

// Submit blocks until a slot is free or ctx is done, then handles d on its own
// goroutine. It returns ctx.Err() if it gave up waiting before acquiring a
// slot (in which case d is not handled and the caller owns the nack).
func (p *Pool) Submit(ctx context.Context, d Delivery) error {
	select {
	case p.sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer func() { <-p.sem }()
		p.runner.Handle(ctx, d)
	}()
	return nil
}

// Wait blocks until all in-flight handlers finish.
func (p *Pool) Wait() {
	p.wg.Wait()
}
