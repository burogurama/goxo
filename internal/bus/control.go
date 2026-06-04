// Control-envelope and routing helpers: every OXO message on the wire is a
// v3.control.Message wrapping the inner proto bytes plus the agents path the
// message has travelled. These functions wrap and unwrap that envelope through
// the codec and derive selectors from routing keys.
package bus

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/burogurama/goxo/internal/codec"
)

// controlSelector is the OXO selector of the envelope itself. It resolves like
// any other selector and carries the agents path and the inner message bytes.
const controlSelector = "v3.control"

// selectorFromRoutingKey strips the trailing id segment a publisher appends to
// a routing key: "v3.asset.ip.<uuid>" becomes "v3.asset.ip".
func selectorFromRoutingKey(routingKey string) string {
	var i int = strings.LastIndex(routingKey, ".")
	if i < 0 {
		return routingKey
	}
	return routingKey[:i]
}

// messageIDFromRoutingKey returns the trailing id segment a publisher appends,
// or "" if the key has no dot.
func messageIDFromRoutingKey(routingKey string) string {
	var i int = strings.LastIndex(routingKey, ".")
	if i < 0 {
		return ""
	}
	return routingKey[i+1:]
}

// wrapControl builds the envelope bytes for an inner message: it records the
// agents path and carries the inner proto bytes (base64 in the dict the codec
// consumes).
func wrapControl(c *codec.Codec, agents []string, inner []byte) ([]byte, error) {
	path := make([]any, len(agents))
	for i, a := range agents {
		path[i] = a
	}
	dict := map[string]any{
		"control": map[string]any{"agents": path},
		"message": base64.StdEncoding.EncodeToString(inner),
	}
	var (
		body []byte
		err  error
	)
	body, err = c.Encode(controlSelector, dict)
	if err != nil {
		return nil, fmt.Errorf("bus: wrap control: %w", err)
	}
	return body, nil
}

// unwrapControl parses envelope bytes into the agents path and the inner proto
// bytes. A missing control or message field yields an empty path or empty
// inner rather than an error.
func unwrapControl(c *codec.Codec, body []byte) (agents []string, inner []byte, err error) {
	var dict map[string]any
	dict, err = c.Decode(controlSelector, body)
	if err != nil {
		return nil, nil, fmt.Errorf("bus: unwrap control: %w", err)
	}
	agents = agentsFromDict(dict)
	inner, err = innerFromDict(dict)
	if err != nil {
		return nil, nil, err
	}
	return agents, inner, nil
}

// agentsFromDict reads control.agents, tolerating its absence.
func agentsFromDict(dict map[string]any) []string {
	control, ok := dict["control"].(map[string]any)
	if !ok {
		return nil
	}
	raw, ok := control["agents"].([]any)
	if !ok {
		return nil
	}
	agents := make([]string, 0, len(raw))
	for _, a := range raw {
		if s, ok := a.(string); ok {
			agents = append(agents, s)
		}
	}
	return agents
}

// innerFromDict decodes the base64 message field, tolerating its absence.
func innerFromDict(dict map[string]any) ([]byte, error) {
	s, ok := dict["message"].(string)
	if !ok {
		return nil, nil
	}
	var (
		inner []byte
		err   error
	)
	inner, err = base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("bus: decode inner message: %w", err)
	}
	return inner, nil
}

// newID returns a random UUIDv4 in canonical form, used as the routing-key
// suffix so each published message has a unique key.
func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("bus: generate id: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
