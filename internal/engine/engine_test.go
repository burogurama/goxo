package engine

import (
	"context"
	"io"
	"log/slog"

	"testing"

	"github.com/burogurama/goxo/internal/bus"
	"github.com/burogurama/goxo/internal/worker"
)

// TestHandleDropsRejectedMessage checks the admission wiring: a message that
// trips a flow-control limit is acked (deliberately dropped, never requeued)
// and never reaches the pool. The pool is built but not started, so a dispatch
// would block forever — proving the rejected message is not dispatched.
func TestHandleDropsRejectedMessage(t *testing.T) {
	const self = "agent/ostorlab/nmap"
	var acked, nacked bool
	var pool *worker.Pool = worker.NewPool(worker.Config{
		Command: []string{"/nonexistent/handler"}, Protocol: Protocol,
	}, nil, 1, 1, slog.New(slog.NewTextHandler(io.Discard, nil)))
	e := &engine{
		pool:   pool,
		log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		agent:  self,
		cyclic: 1, // chain already contains self once, so this rejects
	}
	e.handle(context.Background(), bus.Delivery{
		Selector: "v3.asset.ip",
		Chain:    []string{self},
		Ack:      func() error { acked = true; return nil },
		Nack:     func() error { nacked = true; return nil },
	})
	if !acked {
		t.Error("rejected message must be acked (dropped)")
	}
	if nacked {
		t.Error("rejected message must not be nacked")
	}
}
