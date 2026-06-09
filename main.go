// Command goxo is the Go engine that hosts polyglot OXO agent handlers.
// It does the OXO heavy-lifting (RabbitMQ bus, proto codec, flow-control
// admission) and drives a handler process over a small IPC protocol.
//
// Settings come from the AgentInstanceSettings proto the OXO runtime writes
// (OXO_SETTINGS_PATH, default /tmp/settings.binproto). The agent name, the
// declared selectors, and the argument defaults come from the agent definition
// (OXO_DEFINITION_PATH, default /tmp/ostorlab.yaml). The remaining inputs — the
// handler command, the codec descriptor set, and per-run knobs — come from
// goxo's own configuration file (GOXO_CONFIG, default /goxo.yaml), and each may
// be overridden by an environment variable:
//
//	file key        env variable          meaning
//	handler         GOXO_HANDLER          handler command (the env form is whitespace-split)
//	fdset           GOXO_FDSET            path to the codec FileDescriptorSet
//	timeout         GOXO_HANDLER_TIMEOUT  per-message timeout (Go duration; default 0, no timeout)
//	pool            GOXO_POOL_SIZE        number of long-lived handler processes (default 1)
//	cap             GOXO_WORKER_CAP       messages each process may handle at once (default 1)
//	shutdown_grace  GOXO_SHUTDOWN_GRACE   time a worker gets to drain before SIGKILL (default 10s)
//	—               UNIVERSE              scan universe, informational
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/burogurama/goxo/internal/config"
	"github.com/burogurama/goxo/internal/engine"
	"github.com/burogurama/goxo/internal/manifest"
	"github.com/burogurama/goxo/internal/settings"
)

func main() {
	var log *slog.Logger = slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("goxo: fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	var s *settings.Settings
	var err error
	s, err = settings.Load(envOr("OXO_SETTINGS_PATH", settings.DefaultPath))
	if err != nil {
		return err
	}

	var m *manifest.Manifest
	m, err = manifest.Load(envOr("OXO_DEFINITION_PATH", manifest.DefaultPath))
	if err != nil {
		return err
	}

	var c config.Config
	c, err = loadConfig()
	if err != nil {
		return err
	}

	p := engine.Params{
		Handler:       handlerOr("GOXO_HANDLER", c.Handler),
		FdsetPath:     envOr("GOXO_FDSET", c.Fdset),
		Universe:      os.Getenv("UNIVERSE"),
		Timeout:       durationOr("GOXO_HANDLER_TIMEOUT", c.HandlerTimeout),
		PoolSize:      intOr("GOXO_POOL_SIZE", c.WorkerPoolSize),
		WorkerCap:     intOr("GOXO_WORKER_CAP", c.WorkerQueueSize),
		ShutdownGrace: durationOr("GOXO_SHUTDOWN_GRACE", c.WorkerShutdownGrace),
	}
	return engine.Run(context.Background(), log, m, s, p)
}

// loadConfig reads goxo.yaml: the file named by GOXO_CONFIG, whose absence is
// an error, or the default path, whose absence yields the defaults.
func loadConfig() (config.Config, error) {
	if path := os.Getenv("GOXO_CONFIG"); path != "" {
		return config.Load(path)
	}
	var (
		c   config.Config
		err error
	)
	c, err = config.Load(config.DefaultPath)
	if errors.Is(err, os.ErrNotExist) {
		return config.Default(), nil
	}
	return c, err
}

// handlerOr returns the whitespace-split environment command for key, or
// fallback when it is unset.
func handlerOr(key string, fallback []string) []string {
	if v := os.Getenv(key); v != "" {
		return strings.Fields(v)
	}
	return fallback
}

// envOr returns the environment value for key, or fallback when it is unset.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// durationOr parses the environment value for key as a Go duration, falling
// back when it is unset or unparseable.
func durationOr(key string, fallback time.Duration) time.Duration {
	var raw string = os.Getenv(key)
	if raw == "" {
		return fallback
	}
	var (
		d   time.Duration
		err error
	)
	d, err = time.ParseDuration(raw)
	if err != nil {
		return fallback
	}
	return d
}

// intOr parses the environment value for key as an integer, falling back when
// it is unset or unparseable.
func intOr(key string, fallback int) int {
	var raw string = os.Getenv(key)
	if raw == "" {
		return fallback
	}
	var (
		n   int
		err error
	)
	n, err = strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return n
}
