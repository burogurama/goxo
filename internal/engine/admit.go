// Flow-control admission: whether an inbound message may be processed, decided
// from its control chain alone — the stateless cyclic, depth, and
// accepted-sender checks that mirror OXO's _is_valid_message.
package engine

import "fmt"

// admit decides whether an inbound message may be processed, from its control
// chain alone. It mirrors OXO's _is_valid_message: a rejected message is
// dropped, not retried. The three checks are stateless — no broker or store is
// consulted — so they belong in the engine and need no persistence.
//
//   - cyclic: this agent has already appeared in the chain limit times, so the
//     message is looping back through it.
//   - depth: the chain is limit agents long, so the pipeline has run too deep.
//   - accepted: the immediate sender (last in the chain) is not in the agent's
//     accepted set.
//
// A zero cyclic or depth limit disables that check, matching OXO's proto2
// defaults. An empty accepted set accepts every sender.
func admit(chain []string, agent string, cyclic, depth uint32, accepted []string) (bool, string) {
	if cyclic != 0 && uint32(count(chain, agent)) >= cyclic {
		return false, fmt.Sprintf("cyclic limit %d reached for %s", cyclic, agent)
	}
	if depth != 0 && uint32(len(chain)) >= depth {
		return false, fmt.Sprintf("depth limit %d reached (chain length %d)", depth, len(chain))
	}
	if len(chain) > 0 && len(accepted) > 0 {
		sender := chain[len(chain)-1]
		if !contains(accepted, sender) {
			return false, fmt.Sprintf("sender %s not in accepted agents", sender)
		}
	}
	return true, ""
}

// count returns how many times v appears in s.
func count(s []string, v string) int {
	var n int
	for _, x := range s {
		if x == v {
			n++
		}
	}
	return n
}

// contains reports whether v is in s.
func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
