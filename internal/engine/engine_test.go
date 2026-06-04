package engine

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/burogurama/goxo/internal/bus"
	"github.com/burogurama/goxo/internal/runner"
)

// TestHandleDropsRejectedMessage checks the admission wiring: a message that
// trips a flow-control limit is acked (deliberately dropped, never requeued)
// and the handler is never spawned.
func TestHandleDropsRejectedMessage(t *testing.T) {
	const self = "agent/ostorlab/nmap"
	var acked, nacked bool
	e := &engine{
		// A command that would fail loudly proves the handler is not invoked.
		runner: runner.New(runner.Config{Command: []string{"/nonexistent/handler"}, Timeout: time.Second}, nil,
			slog.New(slog.NewTextHandler(io.Discard, nil))),
		log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		agent:  self,
		cyclic: 1, // chain already contains self once, so this rejects
	}
	e.handle(bus.Delivery{
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
