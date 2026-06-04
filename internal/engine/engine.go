// Package engine wires goxo's parts into a running OXO sidecar: it loads the
// codec descriptor set, connects the bus, serves the OXO healthcheck endpoint,
// runs the handler's start phase once at boot, and drives a handler process per
// inbound message. Each delivery is admitted against the stateless flow-control
// limits, handed to the runner, and acked or nacked by the runner's outcome;
// the handler's emits are relayed back through the bus carrying the message's
// agent chain. An agent with no input selectors runs its start phase and then
// exits.
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
	"github.com/burogurama/goxo/internal/runner"
	"github.com/burogurama/goxo/internal/settings"
)

// Run connects the bus, serves the healthcheck endpoint, runs the handler's
// start phase once, then consumes inbound messages until interrupted. It
// returns nil on a clean shutdown (SIGINT/SIGTERM) and an error if setup or
// consumption fails. A handler in flight when the signal arrives runs to
// completion before Run returns, so its message is not dropped. An agent with
// no input selectors returns after its start phase rather than consuming.
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

	var rcfg runner.Config
	rcfg, err = runnerConfig(m, s, p)
	if err != nil {
		return err
	}

	var b *bus.Bus
	b, err = bus.Connect(busConfig(m, s), cdc, log)
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

	eng := &engine{
		runner:   runner.New(rcfg, busPublisher{b}, log),
		log:      log,
		agent:    m.Name,
		cyclic:   s.CyclicProcessingLimit,
		depth:    s.DepthProcessingLimit,
		accepted: s.AcceptedAgents,
	}

	var runCtx context.Context
	var stop context.CancelFunc
	runCtx, stop = signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The start phase runs once at boot, before any message is consumed. It is
	// always offered — a handler with no start hook just replies done. A failure
	// is fatal: no message is handled before start succeeds, so goxo returns
	// rather than serve. A signal during start cancels runCtx, which is a clean
	// stop rather than a start failure.
	if err := eng.runner.Start(runCtx); err != nil {
		if runCtx.Err() != nil {
			log.Info("goxo: shutting down")
			return nil
		}
		return fmt.Errorf("engine: start phase failed: %w", err)
	}

	if len(inputs) == 0 {
		log.Info("goxo: no inputs, exiting after start", "agent", m.Name)
		return nil
	}

	log.Info("goxo: consuming",
		"exchange", s.BusExchangeTopic, "inputs", inputs, "agent", m.Name)
	err = b.Consume(runCtx, eng.handle)
	if errors.Is(err, context.Canceled) {
		log.Info("goxo: shutting down")
		return nil
	}
	return err
}

// engine is the per-message handler bound once at startup: it admits a delivery
// against the flow-control limits, then drives the handler.
type engine struct {
	runner   *runner.Runner
	log      *slog.Logger
	agent    string
	cyclic   uint32
	depth    uint32
	accepted []string
}

// handle admits one delivery and runs it. A rejected message is acked and
// dropped — the rejection is permanent, so requeuing it would only loop. The
// handler runs on a background context so a shutdown signal lets it finish
// rather than killing it mid-message; its own timeout still bounds it.
func (e *engine) handle(d bus.Delivery) {
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
	e.runner.Handle(context.Background(), runner.Delivery{
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

// busPublisher adapts the bus to the runner's Publisher: it appends this agent
// to the inbound chain and routes the emit. Publishing is bound to a background
// context so an emit from a message that is finishing during shutdown still
// goes out.
type busPublisher struct {
	bus *bus.Bus
}

func (p busPublisher) Publish(chain []string, selector string, data map[string]any) error {
	return p.bus.Publish(context.Background(), chain, selector, data)
}
