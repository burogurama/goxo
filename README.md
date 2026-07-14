# goxo

A language-agnostic engine for writing [OXO](https://github.com/Ostorlab/oxo) agents.

goxo runs as the agent process OXO sees, and does all the OXO heavy-lifting —
the RabbitMQ bus, the protobuf codec, flow-control admission, the healthcheck.
Your agent logic runs as a separate **handler** process that goxo spawns and
talks to over a tiny JSON protocol on stdin/stdout. The handler only ever sees
plain JSON-like dicts; it never touches protobuf, RabbitMQ, or any OXO detail.

That split is the point: because the engine↔handler contract is just
JSON-over-pipes, a handler can be written in any language. goxo is the shared
engine; each language needs only a thin client of the protocol below to become
an OXO agent SDK. [oxo-go](https://github.com/burogurama/oxo-go) is the Go one.

```
                  ┌─────────────────────────────┐
   OXO scan  ╾───╼│ goxo                         │
  (RabbitMQ,      │  bus · codec · flow control  │
   protobuf)      │  healthcheck · runner        │
                  └──────────────┬──────────────┘
                                 │ JSON notes over stdin/stdout
                                  │ (init · deliver · pickup · emit · done)
                  ┌──────────────┴──────────────┐
                  │ handler process              │
                  │  your agent, in any language │
                  └─────────────────────────────┘
```

## How a run works

goxo spawns a **fresh handler process per message** — one process for each
delivered message. The process is briefed, does its work, and exits; that gives
crash isolation per message.

- **Per message** goxo decodes the OXO protobuf to a dict, admits the delivery
  against the flow-control limits (cyclic / depth / accepted-agents from the
  scan settings), spawns the handler, and hands it the message. The handler's
  emits are encoded and routed back onto the bus carrying the inbound agent
  chain. The handler's `done` decides the bus ack (ok) or nack (error); a
  rejected or poison message is dropped, never requeued.

## Running it

goxo reads the scan inputs the OXO runtime mounts. Its own knobs — the handler
command, the codec descriptor set, and the run limits — come from `goxo.yaml`
(at `GOXO_CONFIG`, default `/goxo.yaml`), and each may be overridden by an
environment variable. Precedence: defaults < `goxo.yaml` < environment.

```yaml
# goxo.yaml
handler: [python, /app/handler.py]  # or a string, split on whitespace
fdset: /app/agent.fdset
timeout: 30s                        # per-message (Go duration); absent = no timeout
pool: 4                             # long-lived handler processes
cap: 8                              # messages each process handles at once
shutdown_grace: 10s                 # drain window before a worker is killed
```

| File key | Variable | Meaning | Default |
| --- | --- | --- | --- |
| `handler` | `GOXO_HANDLER` | Handler command (the env form is whitespace-split) | required |
| `fdset` | `GOXO_FDSET` | Path to the protobuf `FileDescriptorSet` for the codec | required |
| `timeout` | `GOXO_HANDLER_TIMEOUT` | Per-message timeout (Go duration) | none |
| `pool` | `GOXO_POOL_SIZE` | Number of long-lived handler processes | `1` |
| `cap` | `GOXO_WORKER_CAP` | Messages each process may handle at once | `1` |
| `shutdown_grace` | `GOXO_SHUTDOWN_GRACE` | Time a worker gets to drain before SIGKILL | `10s` |
| — | `GOXO_CONFIG` | Path to `goxo.yaml` | `/goxo.yaml` |
| — | `UNIVERSE` | Scan universe (informational) | — |
| — | `OXO_SETTINGS_PATH` | `AgentInstanceSettings` proto written by the runtime | `/tmp/settings.binproto` |
| — | `OXO_DEFINITION_PATH` | Agent definition (`ostorlab.yaml`) | `/tmp/ostorlab.yaml` |

The agent name, the consumed selectors, and the declared output selectors come
from the definition / settings; the input selectors prefer the settings and
fall back to the manifest.

## The note protocol

This is the contract every SDK implements. Each note is one frame: a **4-byte
big-endian length prefix** followed by a **JSON body**. The current protocol
version is `2` (sent in `init`).

| Note | Direction | Purpose |
| --- | --- | --- |
| `init` | engine → handler | Always first. Carries protocol version, agent identity, config, and the input selectors. |
| `deliver` | engine → handler | One decoded message: selector, data dict, metadata, and an `id`. |
| `emit_ack` | engine → handler | Answers an `emit`: `ok`, or `error` with a reason (e.g. undeclared output). Advisory. |
| `shutdown` | engine → handler | Asks the handler to stop within a deadline. The engine also closes stdin, which is the authoritative shutdown cue. |
| `pickup` | handler → engine | Sent the instant a deliver is read, before any handler code runs. Tells the engine the message was picked up (dropped on crash) vs. unread (requeued on crash). |
| `emit` | handler → engine | Publish `data` on an output selector. Carries an `id` and an optional `deliver` id linking it to the originating message. |
| `done` | handler → engine | Ends the work for a deliver: `ok` or `error`. For a delivery this maps to the bus ack/nack. |

A handler phase is therefore: read `init`, read a `deliver`, optionally send a
`pickup`, send zero or more `emit`s (each answered by an `emit_ack`), then send
a terminal `done`. `done` is always an explicit note, never an exit code, and
stays authoritative for the message outcome.

One handler process serves exactly one `deliver` and then exits. Handling more
than one message in a single handler process is not yet supported.

Logs go to **stderr** — the handler must keep **stdout** free of anything but
notes.

## Build

```bash
go build -o goxo .              # the engine binary
go test ./...
docker build -t goxo:latest .   # the base image agents extend
```

Requires Go 1.22+. Runtime dependencies: `google.golang.org/protobuf`,
`github.com/rabbitmq/amqp091-go`, `gopkg.in/yaml.v3`.

## Packaging an agent

An agent image extends the goxo base image and adds three things you supply:
the handler binary, the codec descriptor set, and the agent's `ostorlab.yaml`.
The steps below build an image for an agent named `scanner`.

### 1. Build the handler

Compile your handler for the image's platform. Any language works — it only has
to speak the note protocol, so use that language's SDK (for Go,
[gost](https://github.com/burogurama/gost)).

### 2. Generate the descriptor set

Build a self-contained `FileDescriptorSet` over the protos the agent consumes
and emits. `--include_imports` pulls in transitively imported protos so goxo can
resolve every type:

```bash
protoc --include_imports --descriptor_set_out=scanner.fdset \
  -I path/to/oxo/protos v3/asset/ip.proto v3/report/vulnerability.proto
```

### 3. Write `oxo.yaml`

Declare the agent name and its input/output selectors:

```yaml
kind: Agent
name: scanner
version: 0.1.0
in_selectors:
  - v3.asset.ip
out_selectors:
  - v3.report.vulnerability
```

### 4. Write `goxo.yaml` and the Dockerfile

The goxo base image owns the engine entrypoint and the healthcheck port. The
agent overlay adds its files and a `goxo.yaml` pointing goxo at them:

```yaml
# goxo.yaml
handler: [/usr/local/bin/scanner]
fdset: /opt/goxo/scanner.fdset
```

```dockerfile
FROM goxo:latest

COPY scanner /usr/local/bin/scanner
COPY scanner.fdset /opt/goxo/scanner.fdset
COPY goxo.yaml /goxo.yaml
```

### 5. Build the image

```bash
oxo agent build -f oxo.yaml
```
