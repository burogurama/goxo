// Package manifest reads the OXO agent definition (ostorlab.yaml) — the static
// half of an agent's configuration that ships in the agent image rather than in
// the per-run settings. It supplies the agent name, the declared input and
// output selectors, and the argument defaults the runtime overlays with
// settings values.
package manifest

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// DefaultPath is where the OXO runtime mounts the agent definition.
const DefaultPath = "/tmp/ostorlab.yaml"

// Manifest is the slice of the agent definition the engine consumes. Absent
// YAML fields decode to their zero value.
type Manifest struct {
	Name         string
	InSelectors  []string
	OutSelectors []string
	Args         []Arg
}

// Arg is one declared argument: its name and its default value. The value is
// the YAML-typed default (string, number, bool, list, or map), or nil when the
// declaration carries no default.
type Arg struct {
	Name  string
	Value any
}

// wire mirrors the on-disk YAML shape so decoding maps cleanly onto Manifest.
type wire struct {
	Name         string    `yaml:"name"`
	InSelectors  []string  `yaml:"in_selectors"`
	OutSelectors []string  `yaml:"out_selectors"`
	Args         []wireArg `yaml:"args"`
}

type wireArg struct {
	Name  string `yaml:"name"`
	Value any    `yaml:"value"`
}

// Load reads and parses the agent definition at path.
func Load(path string) (*Manifest, error) {
	var (
		data []byte
		err  error
	)
	data, err = os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("manifest: read %s: %w", path, err)
	}
	return Parse(data)
}

// Parse decodes agent definition YAML into a Manifest.
func Parse(data []byte) (*Manifest, error) {
	var w wire
	if err := yaml.Unmarshal(data, &w); err != nil {
		return nil, fmt.Errorf("manifest: parse yaml: %w", err)
	}
	out := &Manifest{
		Name:         w.Name,
		InSelectors:  w.InSelectors,
		OutSelectors: w.OutSelectors,
	}
	if len(w.Args) > 0 {
		out.Args = make([]Arg, len(w.Args))
		for i, a := range w.Args {
			out.Args[i] = Arg{Name: a.Name, Value: a.Value}
		}
	}
	return out, nil
}
