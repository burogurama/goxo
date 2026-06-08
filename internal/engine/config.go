// Config assembly: the engine builds the bus and worker configs it hands to its
// dependencies from three sources — the agent definition (manifest), the scan
// settings (AgentInstanceSettings), and Params. Params carries the run inputs
// that neither the manifest nor the settings provides: the handler command, the
// codec descriptor set, and per-run knobs.
package engine

import (
	"fmt"
	"time"

	"github.com/burogurama/goxo/internal/bus"
	"github.com/burogurama/goxo/internal/manifest"
	"github.com/burogurama/goxo/internal/note"
	"github.com/burogurama/goxo/internal/settings"
	"github.com/burogurama/goxo/internal/worker"
)

// Protocol is the note IPC version the engine speaks to handlers.
const Protocol = 2

// Params are the run inputs that come from neither AgentInstanceSettings nor the
// agent definition: the handler to spawn, the codec descriptor set, and per-run
// knobs. The handler command and descriptor set are goxo-runtime concerns rather
// than OXO definition fields, so they stay caller-supplied (the environment).
type Params struct {
	Handler       []string      // handler command and arguments
	FdsetPath     string        // path to the codec FileDescriptorSet
	Universe      string        // scan universe (OXO's UNIVERSE), informational
	Timeout       time.Duration // per-message handler timeout; 0 means none
	PoolSize      int           // number of long-lived handler processes
	WorkerCap     int           // messages each process may have in flight
	ShutdownGrace time.Duration // time a worker gets to drain before SIGKILL
}

// busConfig maps the manifest and settings onto the bus transport config. The
// agent name (the chain identity) comes from the definition; the bus URL and the
// queue (settings.service_name when set) come from the settings; the inputs
// prefer the settings selectors over the definition's. Prefetch is sized to the
// pool's total capacity so the broker never delivers more than the pool can hold.
func busConfig(m *manifest.Manifest, s *settings.Settings, p Params) bus.Config {
	return bus.Config{
		URL:      s.BusURL,
		Exchange: s.BusExchangeTopic,
		Agent:    m.Name,
		Service:  s.ServiceName,
		Inputs:   inSelectors(s, m),
		Prefetch: p.PoolSize * p.WorkerCap,
	}
}

// workerConfig briefs the handler processes: the command to spawn, the agent
// identity, the merged args the handler receives as config, and the declared
// I/O. The name and outputs come from the definition; the inputs prefer the
// settings selectors over the definition's.
func workerConfig(m *manifest.Manifest, s *settings.Settings, p Params) (worker.Config, error) {
	var (
		cfg map[string]any
		err error
	)
	cfg, err = mergeArgs(m.Args, s.Args)
	if err != nil {
		return worker.Config{}, err
	}
	return worker.Config{
		Command: p.Handler,
		Identity: note.Identity{
			Agent:    m.Name,
			Key:      s.Key,
			Universe: p.Universe,
		},
		Config:   cfg,
		Inputs:   inSelectors(s, m),
		Outputs:  m.OutSelectors,
		Protocol: Protocol,
		Timeout:  p.Timeout,
	}, nil
}

// inSelectors resolves the selectors the agent consumes, preferring the settings
// selectors over the definition's when the settings provide any. This mirrors
// OXO's precedence.
func inSelectors(s *settings.Settings, m *manifest.Manifest) []string {
	if len(s.InSelectors) > 0 {
		return s.InSelectors
	}
	return m.InSelectors
}

// mergeArgs builds the config dict the handler receives by overlaying the
// settings argument values onto the definition's defaults, keyed by name. A
// definition arg with no default contributes a nil value, matching OXO; a
// settings arg is decoded by its type tag. It returns nil when there are no
// arguments at all so an argument-free agent gets an empty config rather than an
// empty map.
func mergeArgs(defs []manifest.Arg, over []settings.Arg) (map[string]any, error) {
	if len(defs) == 0 && len(over) == 0 {
		return nil, nil
	}
	out := make(map[string]any, len(defs)+len(over))
	for _, d := range defs {
		out[d.Name] = d.Value
	}
	for _, a := range over {
		var (
			v   any
			err error
		)
		v, err = a.Decode()
		if err != nil {
			return nil, fmt.Errorf("engine: decode arg %q: %w", a.Name, err)
		}
		out[a.Name] = v
	}
	return out, nil
}
