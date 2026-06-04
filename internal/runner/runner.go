// Package runner drives a handler process for one engine→handler phase: the
// engine spawns a fresh process, sends it init then a deliver or start note,
// relays the handler's emits to the bus, and waits for done. The process then
// exits. One phase, one process; crash isolation per run.
package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"time"

	"github.com/burogurama/goxo/internal/note"
)

// Publisher hands a handler emit to the bus. The codec+bus implement it; the
// runner relays the inbound chain plus selector+data and reports back via
// emit_ack. The chain is opaque routing context the runner carries from the
// delivery to its emits — the bus appends this agent and routes.
type Publisher interface {
	Publish(chain []string, selector string, data map[string]any) error
}

// Delivery is one decoded scan-message to hand a handler, plus the bus
// callbacks the runner invokes once the handler finishes. Chain is the inbound
// agents path, relayed to emits but never shown to the handler. Nack does not
// requeue (poison messages are dropped).
type Delivery struct {
	Selector string
	Data     map[string]any
	Chain    []string
	Meta     note.Meta
	Ack      func()
	Nack     func()
}

// Config is everything needed to spawn and brief one handler run.
type Config struct {
	Command  []string
	Env      []string
	Identity note.Identity
	Config   map[string]any
	Inputs   []string
	Outputs  []string
	Protocol int
	Timeout  time.Duration
}

// Runner spawns one handler process per delivery and relays its notes.
type Runner struct {
	cfg     Config
	pub     Publisher
	log     *slog.Logger
	outputs map[string]bool
}

// New builds a Runner. It precomputes the declared-output set used to police
// emits. A nil logger defaults to slog.Default.
func New(cfg Config, pub Publisher, log *slog.Logger) *Runner {
	if log == nil {
		log = slog.Default()
	}
	outputs := make(map[string]bool, len(cfg.Outputs))
	for _, s := range cfg.Outputs {
		outputs[s] = true
	}
	return &Runner{cfg: cfg, pub: pub, log: log, outputs: outputs}
}

// Handle runs one delivery to completion: spawn, init, deliver, relay emits,
// await done, then ack or nack. It never returns an error — the outcome is
// expressed by calling d.Ack or d.Nack exactly once.
func (r *Runner) Handle(parent context.Context, d Delivery) {
	var (
		res runResult
		err error
	)
	res, err = r.run(parent, d.Selector, d.Chain, func(w *note.Writer) error {
		return w.Deliver(note.Deliver{ID: 1, Selector: d.Selector, Data: d.Data, Meta: d.Meta})
	})
	if err != nil {
		r.log.Error("handler run failed", "selector", d.Selector, "err", err)
		d.Nack()
		return
	}
	// An explicit done is authoritative even if the deadline fired during
	// cleanup, so it is checked before the deadline.
	switch {
	case res.done != nil && res.done.Status == note.StatusOK:
		d.Ack()
	case res.done != nil:
		r.log.Warn("handler reported error", "selector", d.Selector, "err", res.done.Error)
		d.Nack()
	case res.deadline:
		r.log.Warn("handler timed out", "selector", d.Selector, "timeout", r.cfg.Timeout)
		d.Nack()
	default:
		r.log.Warn("handler exited before done", "selector", d.Selector)
		d.Nack()
	}
}

// Start runs the handler's start phase once: spawn, init, start, relay emits,
// await done. A start emit originates here rather than from an inbound message,
// so it carries an empty chain and the bus stamps this agent as the chain head.
// It returns an error if the handler cannot be briefed, times out, exits before
// reporting done, or reports done with an error status.
func (r *Runner) Start(parent context.Context) error {
	var (
		res runResult
		err error
	)
	res, err = r.run(parent, "start", nil, func(w *note.Writer) error {
		return w.Start(note.Start{})
	})
	if err != nil {
		return err
	}
	switch {
	case res.done != nil && res.done.Status == note.StatusOK:
		return nil
	case res.done != nil:
		return fmt.Errorf("runner: start reported error: %s", res.done.Error)
	case res.deadline:
		return fmt.Errorf("runner: start timed out after %v", r.cfg.Timeout)
	default:
		return errors.New("runner: handler exited before start done")
	}
}

// runResult is the outcome of one handler run: the handler's done note (nil if
// none arrived) and whether the run's deadline fired.
type runResult struct {
	done     *note.Done
	deadline bool
}

// run spawns the handler, sends init then the phase note (deliver or start),
// relays the handler's emits — carrying chain — to the bus, and waits for done.
// label is the phase's log context. It returns the run outcome, or an error if
// the process could not be spawned or briefed.
func (r *Runner) run(parent context.Context, label string, chain []string, phase func(*note.Writer) error) (runResult, error) {
	if len(r.cfg.Command) == 0 {
		return runResult{}, errors.New("runner: handler command is empty")
	}
	if r.cfg.Timeout <= 0 {
		return runResult{}, fmt.Errorf("runner: handler timeout must be positive: %v", r.cfg.Timeout)
	}

	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	ctx, cancel = context.WithTimeout(parent, r.cfg.Timeout)
	defer cancel()

	var cmd *exec.Cmd = exec.CommandContext(ctx, r.cfg.Command[0], r.cfg.Command[1:]...)
	cmd.Env = append(os.Environ(), r.cfg.Env...)
	cmd.Stderr = os.Stderr

	var (
		stdin io.WriteCloser
		err   error
	)
	stdin, err = cmd.StdinPipe()
	if err != nil {
		return runResult{}, fmt.Errorf("runner: stdin pipe: %w", err)
	}
	var stdout io.ReadCloser
	stdout, err = cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return runResult{}, fmt.Errorf("runner: stdout pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		return runResult{}, fmt.Errorf("runner: start process: %w", err)
	}

	var w *note.Writer = note.NewWriter(stdin)
	if err := w.Init(note.Init{
		Protocol: r.cfg.Protocol,
		Identity: r.cfg.Identity,
		Config:   r.cfg.Config,
		Inputs:   r.cfg.Inputs,
	}); err != nil {
		return runResult{}, r.briefFailed(cmd, stdin, stdout, "write init", err)
	}
	if err := phase(w); err != nil {
		return runResult{}, r.briefFailed(cmd, stdin, stdout, "write "+label, err)
	}

	// Emit acks are written back on a dedicated goroutine so the read loop
	// below never blocks on stdin while the handler isn't reading it (and so
	// isn't reading our acks). The goroutine closes stdin — the handler's EOF
	// signal — when the acks channel closes.
	acks := make(chan note.EmitAck, 64)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for ack := range acks {
			if err := w.EmitAck(ack); err != nil {
				r.log.Debug("write emit_ack failed", "err", err)
			}
		}
		_ = stdin.Close()
	}()

	var reader *note.Reader = note.NewReader(stdout)
	var done *note.Done
	var n note.HandlerNote
readLoop:
	for {
		n, err = reader.Read()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				r.log.Warn("read handler note failed", "phase", label, "err", err)
			}
			break
		}
		switch {
		case n.Emit != nil:
			r.handleEmit(acks, chain, *n.Emit)
		case n.Done != nil:
			done = n.Done
			// Authoritative outcome received; stop relaying.
			break readLoop
		}
	}
	close(acks)
	// Drain anything the handler writes after done so it never blocks on a full
	// stdout pipe — that would also stall the emit_ack writer sharing its stdin.
	// Reading to EOF also lets cmd.Wait close the pipe safely. This runs
	// alongside the writer goroutine, so neither side wedges.
	_, _ = io.Copy(io.Discard, stdout)
	<-writerDone
	_ = cmd.Wait()

	return runResult{done: done, deadline: ctx.Err() == context.DeadlineExceeded}, nil
}

// handleEmit relays one handler emit to the bus and answers with an emit_ack.
// An emit to an undeclared output is dropped leniently (logged, nacked to the
// handler) rather than failing the whole delivery — done stays authoritative.
func (r *Runner) handleEmit(acks chan<- note.EmitAck, chain []string, e note.Emit) {
	if !r.outputs[e.Selector] {
		r.log.Warn("emit to undeclared output dropped", "selector", e.Selector)
		sendAck(acks, note.EmitAck{ID: e.ID, Status: note.StatusError,
			Error: "selector not in declared outputs"})
		return
	}
	if err := r.pub.Publish(chain, e.Selector, e.Data); err != nil {
		r.log.Error("publish failed", "selector", e.Selector, "err", err)
		sendAck(acks, note.EmitAck{ID: e.ID, Status: note.StatusError, Error: err.Error()})
		return
	}
	sendAck(acks, note.EmitAck{ID: e.ID, Status: note.StatusOK})
}

// briefFailed tears down a half-started run after an init or phase write
// failure and returns the wrapped error. It runs only before the emit_ack
// writer goroutine starts, so closing stdin here cannot race that goroutine's
// own stdin Close.
func (r *Runner) briefFailed(cmd *exec.Cmd, stdin io.Closer, stdout io.Reader, what string, err error) error {
	_ = stdin.Close()
	_, _ = io.Copy(io.Discard, stdout)
	_ = cmd.Wait()
	return fmt.Errorf("runner: %s: %w", what, err)
}

// sendAck queues an emit_ack without blocking. If the buffer is full the ack
// is dropped — it is advisory, and done remains authoritative.
func sendAck(acks chan<- note.EmitAck, ack note.EmitAck) {
	select {
	case acks <- ack:
	default:
	}
}
