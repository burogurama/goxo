package manifest

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

const sampleYAML = `
kind: Agent
name: agent/ostorlab/nmap
version: 1.0.0
in_selectors:
  - v3.asset.ip.v4
  - v3.asset.domain_name
out_selectors:
  - v3.asset.ip.v4.port.service
  - v3.report.vulnerability
args:
  - name: ports
    type: string
    value: "0-65535"
  - name: max_network_breadth
    type: number
    value: 32
  - name: version_info
    type: boolean
    value: true
  - name: scripts
    type: array
    value:
      - banner
      - http-title
  - name: scripts_sources
    type: array
`

func TestParse(t *testing.T) {
	var (
		m   *Manifest
		err error
	)
	m, err = Parse([]byte(sampleYAML))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if m.Name != "agent/ostorlab/nmap" {
		t.Errorf("Name = %q", m.Name)
	}
	if !reflect.DeepEqual(m.InSelectors, []string{"v3.asset.ip.v4", "v3.asset.domain_name"}) {
		t.Errorf("InSelectors = %v", m.InSelectors)
	}
	if !reflect.DeepEqual(m.OutSelectors, []string{"v3.asset.ip.v4.port.service", "v3.report.vulnerability"}) {
		t.Errorf("OutSelectors = %v", m.OutSelectors)
	}
	// Values keep their YAML type; a declaration with no value yields nil.
	want := []Arg{
		{Name: "ports", Value: "0-65535"},
		{Name: "max_network_breadth", Value: 32},
		{Name: "version_info", Value: true},
		{Name: "scripts", Value: []any{"banner", "http-title"}},
		{Name: "scripts_sources", Value: nil},
	}
	if !reflect.DeepEqual(m.Args, want) {
		t.Errorf("Args = %#v, want %#v", m.Args, want)
	}
}

func TestParseNoArgsYieldsNilArgs(t *testing.T) {
	var (
		m   *Manifest
		err error
	)
	m, err = Parse([]byte("name: agent/ostorlab/nmap\n"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if m.Args != nil {
		t.Errorf("Args = %#v, want nil", m.Args)
	}
}

func TestParseInvalidYAMLErrors(t *testing.T) {
	if _, err := Parse([]byte("name: [unterminated\n")); err == nil {
		t.Fatal("expected error for malformed yaml")
	}
}

func TestLoad(t *testing.T) {
	var path string = filepath.Join(t.TempDir(), "ostorlab.yaml")
	if err := os.WriteFile(path, []byte(sampleYAML), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	var (
		m   *Manifest
		err error
	)
	m, err = Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if m.Name != "agent/ostorlab/nmap" {
		t.Errorf("Name = %q", m.Name)
	}
}

func TestLoadMissingFileErrors(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "absent.yaml")); err == nil {
		t.Fatal("expected error for missing file")
	}
}
