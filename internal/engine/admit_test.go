package engine

import "testing"

func TestAdmit(t *testing.T) {
	const self = "agent/ostorlab/nmap"
	cases := []struct {
		name     string
		chain    []string
		cyclic   uint32
		depth    uint32
		accepted []string
		wantOK   bool
	}{
		{name: "empty chain passes", chain: nil, wantOK: true},
		{name: "limits disabled by zero", chain: []string{self, self, self}, cyclic: 0, depth: 0, wantOK: true},
		{name: "cyclic under limit", chain: []string{self, "other"}, cyclic: 2, wantOK: true},
		{name: "cyclic at limit rejected", chain: []string{self, self}, cyclic: 2, wantOK: false},
		{name: "depth under limit", chain: []string{"a", "b"}, depth: 3, wantOK: true},
		{name: "depth at limit rejected", chain: []string{"a", "b", "c"}, depth: 3, wantOK: false},
		{name: "accepted sender passes", chain: []string{"upstream"}, accepted: []string{"upstream"}, wantOK: true},
		{name: "unaccepted sender rejected", chain: []string{"stranger"}, accepted: []string{"upstream"}, wantOK: false},
		{name: "accepted ignored on empty chain", chain: nil, accepted: []string{"upstream"}, wantOK: true},
		{name: "only last agent is the sender", chain: []string{"upstream", "stranger"}, accepted: []string{"upstream"}, wantOK: false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var (
				ok     bool
				reason string
			)
			ok, reason = admit(c.chain, self, c.cyclic, c.depth, c.accepted)
			if ok != c.wantOK {
				t.Errorf("admit = %v (%q), want %v", ok, reason, c.wantOK)
			}
			if !ok && reason == "" {
				t.Error("rejection must carry a reason")
			}
			if ok && reason != "" {
				t.Errorf("acceptance must not carry a reason, got %q", reason)
			}
		})
	}
}
