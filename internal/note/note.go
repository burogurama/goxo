// Package note defines the IPC protocol between the goxo engine and a handler
// process: small JSON "notes" exchanged over the handler's stdin/stdout.
// Engine→handler notes are init, deliver, emit_ack and shutdown; handler→engine
// notes are emit and done.
package note

// Note types. Engine→handler: init, deliver, emit_ack, shutdown. Handler→engine:
// emit, done.
const (
	TypeInit     = "init"
	TypeDeliver  = "deliver"
	TypeEmitAck  = "emit_ack"
	TypeShutdown = "shutdown"
	TypeEmit     = "emit"
	TypeDone     = "done"
)

// Outcome statuses carried by emit_ack and done.
const (
	StatusOK    = "ok"
	StatusError = "error"
)

// Identity names the agent instance the handler runs as. It mirrors the OXO
// agent identity (key, universe).
type Identity struct {
	Agent    string `json:"agent"`
	Key      string `json:"key"`
	Universe string `json:"universe,omitempty"`
}

// Meta carries per-message metadata that travels with a deliver.
type Meta struct {
	MessageID string         `json:"message_id,omitempty"`
	Headers   map[string]any `json:"headers,omitempty"`
}

// Init is the first note the engine sends, carrying the protocol version,
// identity, agent config, and declared input selectors.
type Init struct {
	Type     string         `json:"type"`
	Protocol int            `json:"protocol"`
	Identity Identity       `json:"identity"`
	Config   map[string]any `json:"config,omitempty"`
	Inputs   []string       `json:"inputs,omitempty"`
}

// Deliver hands the handler one decoded scan-message. Data is the message fields
// as a plain JSON-like dict.
type Deliver struct {
	Type     string         `json:"type"`
	ID       int64          `json:"id"`
	Selector string         `json:"selector"`
	Data     map[string]any `json:"data"`
	Meta     Meta           `json:"meta"`
}

// EmitAck answers a handler emit: ok if the engine published it, error (with a
// reason) if it rejected or failed it. It is advisory — done stays
// authoritative for the message outcome.
type EmitAck struct {
	Type   string `json:"type"`
	ID     int64  `json:"id"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// Shutdown asks a handler to stop within DeadlineMS.
type Shutdown struct {
	Type       string `json:"type"`
	DeadlineMS int64  `json:"deadline_ms,omitempty"`
}

// Emit is a handler's request to publish Data on one of its output selectors.
// Deliver is the id of the message this emit answers; when it is absent the
// engine stamps this agent as the sole chain element. ID is the emit's own id,
// echoed by the emit_ack.
type Emit struct {
	Type     string         `json:"type"`
	ID       int64          `json:"id"`
	Deliver  int64          `json:"deliver,omitempty"`
	Selector string         `json:"selector"`
	Data     map[string]any `json:"data"`
}

// Done ends the handler's work for an id: ok if it processed cleanly, error
// (with a reason) if it failed.
type Done struct {
	Type   string `json:"type"`
	ID     int64  `json:"id"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}
