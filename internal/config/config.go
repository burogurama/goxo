// Package config reads goxo's own configuration file (goxo.yaml): the engine
// knobs an agent image bakes in next to its OXO definition — the handler
// command, the codec descriptor set, and the run limits. The file overlays the
// defaults; environment variables overlay the file (that overlay is the
// caller's job).
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// DefaultPath is where goxo looks for its configuration file when GOXO_CONFIG
// is unset.
const DefaultPath = "/goxo.yaml"

// Config is the engine configuration carried by goxo.yaml. A field absent from
// the file holds its default (see Default).
type Config struct {
	Handler             []string      // handler command and arguments
	Fdset               string        // path to the codec FileDescriptorSet
	HandlerTimeout      time.Duration // per-message handler timeout; 0 means none
	WorkerPoolSize      int           // number of long-lived handler processes
	WorkerQueueSize     int           // messages each process may have in flight
	WorkerShutdownGrace time.Duration // time a worker gets to drain before SIGKILL
}

// Default is the configuration with no file at all: no handler, no descriptor
// set, no timeout, one process handling one message at a time, a 10s drain
// window.
func Default() Config {
	return Config{WorkerPoolSize: 1, WorkerQueueSize: 1, WorkerShutdownGrace: 10 * time.Second}
}

// wire mirrors the on-disk YAML shape. Durations travel as Go duration strings;
// the handler is an argv list or a whitespace-split string.
type wire struct {
	Handler       command `yaml:"handler"`
	Fdset         string  `yaml:"fdset"`
	Timeout       string  `yaml:"timeout"`
	Pool          int     `yaml:"pool"`
	Cap           int     `yaml:"cap"`
	ShutdownGrace string  `yaml:"shutdown_grace"`
}

// command decodes the handler field: a sequence is the argv as-is; a scalar is
// split on whitespace.
type command []string

func (c *command) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode {
		var s string
		if err := n.Decode(&s); err != nil {
			return err
		}
		*c = strings.Fields(s)
		return nil
	}
	var args []string
	if err := n.Decode(&args); err != nil {
		return err
	}
	*c = args
	return nil
}

// Load reads and parses the configuration file at path. A missing file
// surfaces as an error matching os.ErrNotExist, so a caller may treat it as
// "no file, use the defaults".
func Load(path string) (Config, error) {
	var (
		data []byte
		err  error
	)
	data, err = os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: read %s: %w", path, err)
	}
	var c Config
	c, err = Parse(data)
	if err != nil {
		return Config{}, fmt.Errorf("config: parse %s: %w", path, err)
	}
	return c, nil
}

// Parse decodes one goxo.yaml document over the defaults. An unknown key or a
// malformed duration is an error; an empty document is the defaults.
func Parse(data []byte) (Config, error) {
	var w wire
	var dec *yaml.Decoder = yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&w); err != nil && !errors.Is(err, io.EOF) {
		return Config{}, err
	}
	var c Config = Default()
	if len(w.Handler) > 0 {
		c.Handler = w.Handler
	}
	if w.Fdset != "" {
		c.Fdset = w.Fdset
	}
	if w.Pool != 0 {
		c.WorkerPoolSize = w.Pool
	}
	if w.Cap != 0 {
		c.WorkerQueueSize = w.Cap
	}
	var err error
	if c.HandlerTimeout, err = durationOr(w.Timeout, c.HandlerTimeout); err != nil {
		return Config{}, fmt.Errorf("timeout: %w", err)
	}
	if c.WorkerShutdownGrace, err = durationOr(w.ShutdownGrace, c.WorkerShutdownGrace); err != nil {
		return Config{}, fmt.Errorf("shutdown_grace: %w", err)
	}
	return c, nil
}

// durationOr parses raw as a Go duration, or returns fallback when raw is
// empty.
func durationOr(raw string, fallback time.Duration) (time.Duration, error) {
	if raw == "" {
		return fallback, nil
	}
	return time.ParseDuration(raw)
}
