// Package worker runs the engine's handler processes. A worker owns one
// long-lived handler process, sends it init once, then hands it deliver notes,
// relays its emits to the bus, and settles each message on the matching done. A
// process may have several messages in flight at once, tagged by id; if it exits
// unexpectedly the messages it had picked up are dropped and those it had not are
// requeued, and the worker respawns it. A Pool spreads a fixed set of workers and
// dispatches each message to the least loaded one.
package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/burogurama/goxo/internal/note"
)

// respawnBackoff is the pause before a worker respawns a process that failed to
// spawn, so a permanently broken handler command doesn't spin a tight loop.
const respawnBackoff = 200 * time.Millisecond

// Publisher hands a handler emit to the bus. The bus implements it; the worker
// relays the originating message's chain plus the emit's selector and data, and
// the bus appends this agent and routes.
type Publisher interface {
	Publish(chain []string, selector string, data map[string]any) error
}

// Delivery is one decoded scan-message to hand a worker, plus the bus callbacks
// the worker invokes once the handler finishes it. Chain is the inbound agents
// path, stamped on the message's emits but never shown to the handler. Ack
// settles the message; Nack drops it without requeue; Requeue returns it to the
// broker for redelivery. Exactly one of the three is called, once, when the
// message settles.
type Delivery struct {
	Selector string
	Data     map[string]any
	Chain    []string
	Meta     note.Meta
	Ack      func()
	Nack     func()
	Requeue  func()
}

// Config is everything needed to spawn and brief one handler process. Timeout
// bounds a single message; a value <= 0 means no per-message timeout.
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

// inflight is one message a worker has handed its process but not yet seen a
// done for. chain is stamped on the message's emits; ack, nack and requeue
// settle the bus message exactly once (the map entry is the gate — whoever
// deletes it owns the settle). picked records that the handler reported taking
// the message off the wire. timer fires the per-message timeout when one is set.
type inflight struct {
	chain   []string
	ack     func()
	nack    func()
	requeue func()
	picked  bool
	timer   *time.Timer
}

// outNote is one engine→handler note queued for a process's writer goroutine.
// Exactly one field is set.
type outNote struct {
	deliver *note.Deliver
	emitAck *note.EmitAck
}

// proc is one live handler process and the queue feeding its writer goroutine.
// out is never closed; requestStop (once) signals the writer to drain and close
// stdin.
type proc struct {
	cmd     *exec.Cmd
	out     chan outNote
	stop    chan struct{}
	stopOne sync.Once
}

func (p *proc) requestStop() { p.stopOne.Do(func() { close(p.stop) }) }

// send queues o for the writer, returning false if the process is stopping or
// ctx is cancelled before it can be queued.
func (p *proc) send(ctx context.Context, o outNote) bool {
	select {
	case <-p.stop:
		return false
	case <-ctx.Done():
		return false
	case p.out <- o:
		return true
	}
}

// worker owns one handler process and the bookkeeping for its in-flight
// messages. A single supervisor goroutine spawns, reads, and respawns the
// process; cond guards the in-flight map and wakes Dispatch when a slot frees, a
// process becomes ready, or the worker starts closing.
type worker struct {
	idx     int
	cfg     Config
	pub     Publisher
	log     *slog.Logger
	outputs map[string]bool
	cap     int

	mu      sync.Mutex
	cond    *sync.Cond
	cur     *proc
	msgs    map[int64]*inflight
	nextID  int64
	closing bool

	stoppedCh chan struct{}
	stoppedOn sync.Once
}

// newWorker builds a worker. capacity is the most messages its process may have
// in flight at once (capacity < 1 is treated as 1). A nil logger uses the
// default.
func newWorker(idx int, cfg Config, pub Publisher, capacity int, log *slog.Logger) *worker {
	if log == nil {
		log = slog.Default()
	}
	if capacity < 1 {
		capacity = 1
	}
	outputs := make(map[string]bool, len(cfg.Outputs))
	for _, s := range cfg.Outputs {
		outputs[s] = true
	}
	w := &worker{
		idx:       idx,
		cfg:       cfg,
		pub:       pub,
		log:       log,
		outputs:   outputs,
		cap:       capacity,
		msgs:      make(map[int64]*inflight),
		nextID:    1,
		stoppedCh: make(chan struct{}),
	}
	w.cond = sync.NewCond(&w.mu)
	return w
}

// run is the worker's supervisor: it spawns the process, reads its notes until
// it exits, nacks any message still in flight, and respawns — until the worker
// is closing (or ctx is done), when it marks itself stopped and returns.
func (w *worker) run(ctx context.Context) {
	for {
		var (
			p      *proc
			stdout io.Reader
			err    error
		)
		p, stdout, err = w.spawn()
		if err != nil {
			if w.shuttingDown(ctx) {
				w.markStopped()
				return
			}
			w.log.Error("worker spawn failed; retrying", "worker", w.idx, "err", err)
			if !sleepCtx(ctx, respawnBackoff) {
				w.markStopped()
				return
			}
			continue
		}

		w.mu.Lock()
		w.cur = p
		w.cond.Broadcast()
		w.mu.Unlock()

		// Read until the process closes stdout: it has exited or is exiting.
		w.readLoop(p, stdout)

		// Process is gone. Stop its writer, reap it, and drop the live pointer
		// so Dispatch waits for the next process.
		p.requestStop()
		_ = p.cmd.Wait()

		w.mu.Lock()
		w.cur = nil
		stale := w.msgs
		w.msgs = make(map[int64]*inflight)
		closing := w.closing || ctx.Err() != nil
		w.cond.Broadcast()
		w.mu.Unlock()

		// Messages still in flight got no done. One the handler had picked up is
		// dropped (poison); one it never picked up is requeued.
		for id, m := range stale {
			if m.timer != nil {
				m.timer.Stop()
			}
			if m.picked {
				w.log.Warn("handler exited with picked-up message in flight", "worker", w.idx, "id", id)
				m.nack()
			} else {
				w.log.Warn("handler exited with un-picked-up message; requeueing", "worker", w.idx, "id", id)
				m.requeue()
			}
		}

		if closing {
			w.markStopped()
			return
		}
	}
}

// spawn starts the handler process, writes its one init note, and launches the
// writer goroutine that owns its stdin. It returns the process and its stdout.
func (w *worker) spawn() (*proc, io.Reader, error) {
	if len(w.cfg.Command) == 0 {
		return nil, nil, errors.New("worker: handler command is empty")
	}
	var cmd *exec.Cmd = exec.Command(w.cfg.Command[0], w.cfg.Command[1:]...)
	cmd.Env = append(os.Environ(), w.cfg.Env...)
	cmd.Stderr = os.Stderr

	var (
		stdin io.WriteCloser
		err   error
	)
	stdin, err = cmd.StdinPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("worker: stdin pipe: %w", err)
	}
	var stdout io.ReadCloser
	stdout, err = cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, nil, fmt.Errorf("worker: stdout pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		return nil, nil, fmt.Errorf("worker: start process: %w", err)
	}

	var nw *note.Writer = note.NewWriter(stdin)
	if err := nw.Init(note.Init{
		Protocol: w.cfg.Protocol,
		Identity: w.cfg.Identity,
		Config:   w.cfg.Config,
		Inputs:   w.cfg.Inputs,
	}); err != nil {
		_ = stdin.Close()
		_, _ = io.Copy(io.Discard, stdout)
		_ = cmd.Wait()
		return nil, nil, fmt.Errorf("worker: write init: %w", err)
	}

	var p *proc = &proc{cmd: cmd, out: make(chan outNote, w.cap+1), stop: make(chan struct{})}
	go w.writeLoop(p, nw, stdin)
	return p, stdout, nil
}

// writeLoop serializes engine→handler notes onto the process's stdin; it is the
// only writer of stdin. On stop it drains what is already queued, then closes
// stdin to signal EOF.
func (w *worker) writeLoop(p *proc, nw *note.Writer, stdin io.WriteCloser) {
	defer func() { _ = stdin.Close() }()
	for {
		select {
		case <-p.stop:
			for {
				select {
				case o := <-p.out:
					w.writeNote(nw, o)
				default:
					return
				}
			}
		case o := <-p.out:
			w.writeNote(nw, o)
		}
	}
}

func (w *worker) writeNote(nw *note.Writer, o outNote) {
	var err error
	switch {
	case o.deliver != nil:
		err = nw.Deliver(*o.deliver)
	case o.emitAck != nil:
		err = nw.EmitAck(*o.emitAck)
	}
	if err != nil {
		w.log.Debug("write handler note failed", "worker", w.idx, "err", err)
	}
}

// readLoop relays the handler's emit and done notes until its stdout closes.
func (w *worker) readLoop(p *proc, stdout io.Reader) {
	var reader *note.Reader = note.NewReader(stdout)
	for {
		var (
			n   note.HandlerNote
			err error
		)
		n, err = reader.Read()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				w.log.Warn("read handler note failed", "worker", w.idx, "err", err)
			}
			return
		}
		switch {
		case n.Emit != nil:
			w.onEmit(p, *n.Emit)
		case n.Done != nil:
			w.onDone(*n.Done)
		case n.Pickup != nil:
			w.onPickup(n.Pickup.ID)
		}
	}
}

// onEmit relays one emit to the bus and answers with an emit_ack. The emit's
// Deliver id selects the originating message's chain; an emit naming no live
// message carries a nil chain, so the bus stamps this agent as the sole chain
// element. An emit to an undeclared output or a failing publish is dropped
// leniently.
func (w *worker) onEmit(p *proc, e note.Emit) {
	var chain []string
	w.mu.Lock()
	if m, ok := w.msgs[e.Deliver]; ok {
		chain = m.chain
	}
	w.mu.Unlock()

	if !w.outputs[e.Selector] {
		w.log.Warn("emit to undeclared output dropped", "worker", w.idx, "selector", e.Selector)
		w.ackEmit(p, note.EmitAck{ID: e.ID, Status: note.StatusError, Error: "selector not in declared outputs"})
		return
	}
	if err := w.pub.Publish(chain, e.Selector, e.Data); err != nil {
		w.log.Error("publish failed", "worker", w.idx, "selector", e.Selector, "err", err)
		w.ackEmit(p, note.EmitAck{ID: e.ID, Status: note.StatusError, Error: err.Error()})
		return
	}
	w.ackEmit(p, note.EmitAck{ID: e.ID, Status: note.StatusOK})
}

// ackEmit queues an emit_ack without blocking the read loop; a full queue or a
// stopping process drops it.
func (w *worker) ackEmit(p *proc, a note.EmitAck) {
	select {
	case p.out <- outNote{emitAck: &a}:
	default:
	}
}

// onPickup marks the message picked up by the handler. A pickup for an unknown
// id (already settled, or never sent) is ignored.
func (w *worker) onPickup(id int64) {
	w.mu.Lock()
	if m, ok := w.msgs[id]; ok {
		m.picked = true
	}
	w.mu.Unlock()
}

// onDone settles the message the done names: ack on ok, nack on error. A done
// for an unknown id (already timed out, or never sent) is ignored.
func (w *worker) onDone(d note.Done) {
	w.mu.Lock()
	var (
		m  *inflight
		ok bool
	)
	m, ok = w.msgs[d.ID]
	if ok {
		delete(w.msgs, d.ID)
		if m.timer != nil {
			m.timer.Stop()
		}
		w.cond.Broadcast()
	}
	w.mu.Unlock()
	if !ok {
		return
	}
	if d.Status == note.StatusOK {
		m.ack()
		return
	}
	w.log.Warn("handler reported error", "worker", w.idx, "id", d.ID, "err", d.Error)
	m.nack()
}

// Dispatch hands d to this worker's process: it waits until the process is live
// and has a free slot (fewer than cap in flight), assigns the message an id,
// registers it, and queues the deliver note. It returns false (leaving d
// untouched) only if ctx is cancelled or the worker starts closing before a
// slot frees; otherwise the worker owns settling d.
func (w *worker) Dispatch(ctx context.Context, d Delivery) bool {
	var stop func() bool = context.AfterFunc(ctx, func() {
		w.mu.Lock()
		w.cond.Broadcast()
		w.mu.Unlock()
	})
	defer stop()

	w.mu.Lock()
	for {
		if w.closing || ctx.Err() != nil {
			w.mu.Unlock()
			return false
		}
		if w.cur != nil && len(w.msgs) < w.cap {
			break
		}
		w.cond.Wait()
	}
	var id int64 = w.nextID
	w.nextID++
	var p *proc = w.cur
	var m *inflight = &inflight{chain: d.Chain, ack: d.Ack, nack: d.Nack, requeue: d.Requeue}
	if w.cfg.Timeout > 0 {
		m.timer = w.armTimeout(id)
	}
	w.msgs[id] = m
	w.mu.Unlock()

	var nd note.Deliver = note.Deliver{ID: id, Selector: d.Selector, Data: d.Data, Meta: d.Meta}
	if !p.send(ctx, outNote{deliver: &nd}) {
		// The process stopped before it took the note: the handler never saw the
		// message, so reclaim the entry (if the death path has not already) and
		// requeue it, so it settles exactly once.
		w.mu.Lock()
		_, still := w.msgs[id]
		if still {
			delete(w.msgs, id)
			if m.timer != nil {
				m.timer.Stop()
			}
			w.cond.Broadcast()
		}
		w.mu.Unlock()
		if still {
			d.Requeue()
		}
	}
	return true
}

// armTimeout fires the per-message timeout: it nacks the message (dropped) and
// frees its slot. A done that arrives later finds no entry and is ignored.
func (w *worker) armTimeout(id int64) *time.Timer {
	return time.AfterFunc(w.cfg.Timeout, func() {
		w.mu.Lock()
		var (
			m  *inflight
			ok bool
		)
		m, ok = w.msgs[id]
		if ok {
			delete(w.msgs, id)
			w.cond.Broadcast()
		}
		w.mu.Unlock()
		if !ok {
			return
		}
		w.log.Warn("handler message timed out", "worker", w.idx, "id", id, "timeout", w.cfg.Timeout)
		m.nack()
	})
}

// outstanding is how many messages this worker has in flight: queued plus
// picked up but not yet done. Pool uses it to pick the least-loaded worker.
func (w *worker) outstanding() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.msgs)
}

// beginClose asks the worker to finish in-flight work and exit: it marks the
// worker closing (so Dispatch stops waiting) and tells the live process to drain
// and close stdin.
func (w *worker) beginClose() {
	w.mu.Lock()
	w.closing = true
	var p *proc = w.cur
	w.cond.Broadcast()
	w.mu.Unlock()
	if p != nil {
		p.requestStop()
	}
}

// kill SIGKILLs the live process, used when a worker overruns the shutdown
// grace.
func (w *worker) kill() {
	w.mu.Lock()
	var p *proc = w.cur
	w.mu.Unlock()
	if p != nil && p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
	}
}

func (w *worker) markStopped() { w.stoppedOn.Do(func() { close(w.stoppedCh) }) }

func (w *worker) shuttingDown(ctx context.Context) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closing || ctx.Err() != nil
}

// sleepCtx waits d or until ctx is done, returning false if ctx ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	var t *time.Timer = time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
