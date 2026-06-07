// Package engine wires goxo's parts into a running OXO sidecar: it loads the
// codec descriptor set, connects the bus, serves the OXO healthcheck endpoint,
// and drives the handler pool
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/burogurama/goxo/internal/bus"
	"github.com/burogurama/goxo/internal/codec"
	"github.com/burogurama/goxo/internal/health"
	"github.com/burogurama/goxo/internal/manifest"
	"github.com/burogurama/goxo/internal/note"
	"github.com/burogurama/goxo/internal/settings"
	"github.com/burogurama/goxo/internal/worker"
)

// Run connects the bus, serves the healthcheck endpoint, starts the worker
// pool, then consumes inbound messages until interrupted. It returns nil on a
// clean shutdown (SIGINT/SIGTERM) and an error if setup or consumption fails. On
// shutdown the pool is drained: each worker finishes its in-flight messages
// within the grace window before it is killed. An agent with no input selectors
// has nothing to consume and returns.
func Run(ctx context.Context, log *slog.Logger, m *manifest.Manifest, s *settings.Settings, p Params) error {
	if log == nil {
		log = slog.Default()
	}
	if m.Name == "" {
		return errors.New("engine: no agent name (manifest is missing name)")
	}
	if len(p.Handler) == 0 {
		return errors.New("engine: no handler command (set GOXO_HANDLER)")
	}
	if p.FdsetPath == "" {
		return errors.New("engine: no descriptor set (set GOXO_FDSET)")
	}
	var inputs []string = inSelectors(s, m)

	var cdc *codec.Codec
	var err error
	cdc, err = loadCodec(p.FdsetPath)
	if err != nil {
		return err
	}

	var wcfg worker.Config
	wcfg, err = workerConfig(m, s, p)
	if err != nil {
		return err
	}

	var b *bus.Bus
	b, err = bus.Connect(busConfig(m, s, p), cdc, log)
	if err != nil {
		return err
	}
	defer func() { _ = b.Close() }()

	var hc *health.Server
	hc, err = health.Serve(healthAddr(s), log)
	if err != nil {
		return err
	}
	defer func() { _ = hc.Close() }()
	log.Info("goxo: healthcheck listening", "addr", hc.Addr())

	var runCtx context.Context
	var stop context.CancelFunc
	runCtx, stop = signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	var pool *worker.Pool = worker.NewPool(wcfg, busPublisher{b}, p.PoolSize, p.WorkerCap, log)
	pool.Start(runCtx)
	// Drained before the bus closes (deferred later, so it runs first): each
	// worker finishes in-flight messages within the grace window, then is killed.
	defer pool.Shutdown(p.ShutdownGrace)

	eng := &engine{
		pool:     pool,
		log:      log,
		agent:    m.Name,
		cyclic:   s.CyclicProcessingLimit,
		depth:    s.DepthProcessingLimit,
		accepted: s.AcceptedAgents,
	}

	if len(inputs) == 0 {
		log.Info("goxo: no inputs, nothing to consume", "agent", m.Name)
		return nil
	}

	log.Info("goxo: consuming",
		"exchange", s.BusExchangeTopic, "inputs", inputs, "agent", m.Name,
		"pool", p.PoolSize, "cap", p.WorkerCap)
	err = b.Consume(runCtx, func(d bus.Delivery) { eng.handle(runCtx, d) })
	if errors.Is(err, context.Canceled) {
		log.Info("goxo: shutting down")
		return nil
	}
	return err
}

// engine is the per-message handler bound once at startup: it admits a delivery
// against the flow-control limits, then dispatches it to the pool.
type engine struct {
	pool     *worker.Pool
	log      *slog.Logger
	agent    string
	cyclic   uint32
	depth    uint32
	accepted []string
}

// handle admits one delivery and dispatches it. A rejected message is acked and
// dropped. Dispatch returns once a worker owns the message; the worker acks or
// nacks it  later. If the pool is shutting down before it can take the message,
// it is left unsettled so the broker redelivers it on the next run.
func (e *engine) handle(ctx context.Context, d bus.Delivery) {
	var (
		ok     bool
		reason string
	)
	ok, reason = admit(d.Chain, e.agent, e.cyclic, e.depth, e.accepted)
	if !ok {
		e.log.Info("goxo: drop message", "selector", d.Selector, "reason", reason)
		_ = d.Ack()
		return
	}
	e.pool.Dispatch(ctx, worker.Delivery{
		Selector: d.Selector,
		Data:     d.Data,
		Chain:    d.Chain,
		Meta:     note.Meta{MessageID: d.MessageID, Headers: d.Headers},
		Ack:      func() { _ = d.Ack() },
		Nack:     func() { _ = d.Nack() },
	})
}

// healthAddr is the healthcheck listen address from settings, with OXO's
// defaults (0.0.0.0:5000) filled in when the runtime leaves them unset.
func healthAddr(s *settings.Settings) string {
	var host string = s.HealthcheckHost
	if host == "" {
		host = "0.0.0.0"
	}
	var port uint32 = s.HealthcheckPort
	if port == 0 {
		port = 5000
	}
	return net.JoinHostPort(host, strconv.Itoa(int(port)))
}

// loadCodec reads a FileDescriptorSet from disk and builds the codec over it.
func loadCodec(path string) (*codec.Codec, error) {
	var (
		fdset []byte
		err   error
	)
	fdset, err = os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("engine: read descriptor set %s: %w", path, err)
	}
	var reg *codec.Registry
	reg, err = codec.NewRegistry(fdset)
	if err != nil {
		return nil, err
	}
	return codec.New(reg), nil
}

// busPublisher adapts the bus to the worker's Publisher: it appends this agent
// to the inbound chain and routes the emit. Publishing is bound to a background
// context so an emit from a message that is finishing during shutdown still
// goes out.
type busPublisher struct {
	bus *bus.Bus
}

func (p busPublisher) Publish(chain []string, selector string, data map[string]any) error {
	return p.bus.Publish(context.Background(), chain, selector, data)
}
