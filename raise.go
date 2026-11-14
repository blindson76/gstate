package gstate

import (
	"context"
	"errors"
	"maps"
)

// ErrInternalEventLimit reports that one step raised more internal events than
// the actor's budget allows, which usually means internal events raise each
// other in a cycle. The actor shuts down and preserves its last configuration
// for inspection.
var ErrInternalEventLimit = errors.New("gstate: internal event budget exhausted")

// internalEventBudget caps the internal events processed in one step.
const internalEventBudget = 10000

// ParamError is the [Params] key under which [InvokeBuilder.RaiseOnError]
// stores the service error on internal events. External events carry it in
// [RaisedEvent.Err] instead.
const ParamError = "error"

// Params are named event parameters. Maps passed to the library are copied
// before each delivery, so receivers may modify the map they get.
type Params map[string]any

// RaiseKind selects where a raised event goes.
type RaiseKind int

const (
	// External delivers the event to the handler installed with
	// [Machine.WithRaiseHandler]. It is the default.
	External RaiseKind = iota
	// Internal feeds the event back into the raising actor, like SCXML
	// <raise>. Internal events are processed in order after the current step
	// (including its eventless transitions) settles and before the next
	// mailbox event. They need no raise handler; when one is installed it
	// also receives them, with [RaisedEvent.Internal] set. See
	// [Machine.WithInternalViaHandler] to route them only through the handler.
	Internal
)

// RaiseOption configures a raised event. [RaiseKind] values ([External],
// [Internal]) select the destination and [Params] values add parameters;
// several Params options are merged, later keys winning.
type RaiseOption interface {
	applyRaise(*raiseSpec)
}

type raiseSpec struct {
	kind   RaiseKind
	params Params
}

func (k RaiseKind) applyRaise(s *raiseSpec) { s.kind = k }

func (p Params) applyRaise(s *raiseSpec) {
	if len(p) == 0 {
		return
	}
	if s.params == nil {
		s.params = make(Params, len(p))
	}
	maps.Copy(s.params, p)
}

func resolveRaiseOptions(opts []RaiseOption) raiseSpec {
	var s raiseSpec
	for _, o := range opts {
		if o != nil {
			o.applyRaise(&s)
		}
	}
	return s
}

// RaiseDef describes one event raised by a transition or an invoke completion.
type RaiseDef[E ~string, D any] struct {
	// Event is the raised event.
	Event E
	// Kind is [External] or [Internal].
	Kind RaiseKind
	// Params are static parameters.
	Params Params
	// ParamsFunc, when set, computes parameters from the actor data at raise
	// time (after the transition's Assign actions). Its keys override Params.
	ParamsFunc func(D) Params
}

func newRaiseDef[E ~string, D any](event E, fn func(D) Params, opts []RaiseOption) RaiseDef[E, D] {
	spec := resolveRaiseOptions(opts)
	return RaiseDef[E, D]{Event: event, Kind: spec.kind, Params: spec.params, ParamsFunc: fn}
}

// Event is the event a transition is handling, as seen by guards and actions
// registered with [TransitionBuilder.GuardEvent] and
// [TransitionBuilder.AssignEvent]. Eventless steps (Always, After, OnDone,
// invoke completion and invoke updates) see the zero Event.
type Event[E ~string] struct {
	// Name is the event.
	Name E `json:"name"`
	// Params are the event's parameters; nil when it has none.
	Params Params `json:"params,omitempty"`
	// Internal reports whether the event was raised with [Internal].
	Internal bool `json:"internal,omitempty"`
}

type internalEvent[E ~string] struct {
	ctx   context.Context
	event Event[E]
}

// invokeRaiserKey is the context key under which an invoke's raiser is stored.
type invokeRaiserKey struct{}

// invokeRaiser raises an event on behalf of a running invoke. The event is
// passed as a string so [Raise] works with untyped constants and any E.
type invokeRaiser func(ctx context.Context, event string, opts []RaiseOption) bool

// Raise emits event from inside an [InvokeDef.Func] or [InvokeDef.Run]. ctx
// must be the context passed to the service, or one derived from it.
//
// By default the event is external: it is delivered to the handler installed
// with [Machine.WithRaiseHandler], with the invoke's state as
// [RaisedEvent.State], before Raise returns. Pass [Internal] to feed it back
// into the actor instead; it is processed, with any transitions it triggers,
// before Raise returns, and also handed to the raise handler (only handed to
// it under [Machine.WithInternalViaHandler]). Pass [Params] to attach
// parameters.
//
// The event is converted to the machine's event type, so untyped string
// constants work: Raise(ctx, "PROGRESS", gstate.Params{"pct": 50}).
//
// Raise reports whether the event was delivered or queued. It returns false
// when ctx does not come from an invoke, the event is empty, the invoke has
// finished, its state has exited or the actor has stopped, and, for external
// events, when no raise handler is installed. Like mutate, Raise must not be
// called from a lock-held actor callback.
func Raise[E ~string](ctx context.Context, event E, opts ...RaiseOption) bool {
	raiser, ok := ctx.Value(invokeRaiserKey{}).(invokeRaiser)
	if !ok || event == "" {
		return false
	}
	return raiser(ctx, string(event), opts)
}

// raiseLocked raises defs on behalf of sourceID. Every event is buffered for
// the raise handler when one is installed; internal events are also queued
// for local processing unless they are routed via the handler. err is
// attached to every event (RaisedEvent.Err externally, Params[ParamError]
// internally) when non-nil. It reports whether any event was buffered or
// queued. Must be called with a.mu held.
func (a *Actor[S, E, D]) raiseLocked(ctx context.Context, sourceID S, defs []RaiseDef[E, D], err error) bool {
	raised := false
	for _, def := range defs {
		if def.Event == "" || (def.Kind == External && a.raiseHandler == nil) {
			continue
		}
		var params Params
		if len(def.Params) > 0 {
			params = maps.Clone(def.Params)
		}
		if def.ParamsFunc != nil {
			if dyn := def.ParamsFunc(a.data); len(dyn) > 0 {
				if params == nil {
					params = make(Params, len(dyn))
				}
				maps.Copy(params, dyn)
			}
		}
		raised = true
		if def.Kind == Internal && err != nil {
			if params == nil {
				params = Params{}
			}
			params[ParamError] = err
		}
		if a.raiseHandler != nil {
			a.pendingRaises = append(a.pendingRaises, pendingRaise[S, E]{
				ctx: ctx,
				event: RaisedEvent[S, E]{
					ActorID: a.id, State: sourceID, Event: def.Event,
					Params: maps.Clone(params), Err: err, Internal: def.Kind == Internal,
				},
			})
		}
		if def.Kind == Internal && (!a.internalViaHandler || a.raiseHandler == nil) {
			a.internalQueue = append(a.internalQueue, internalEvent[E]{
				ctx:   ctx,
				event: Event[E]{Name: def.Event, Params: params, Internal: true},
			})
		}
	}
	return raised
}

// drainInternalLocked processes queued internal events in order. Events they
// raise internally are appended and processed in the same drain, up to
// internalEventBudget. Must be called with a.mu held.
func (a *Actor[S, E, D]) drainInternalLocked() {
	for steps := 0; len(a.internalQueue) > 0; steps++ {
		select {
		case <-a.stopped:
			a.internalQueue = nil
			return
		default:
		}
		if steps == internalEventBudget {
			a.internalQueue = nil
			a.terminalErr = ErrInternalEventLimit
			a.initiateShutdownLocked()
			return
		}
		next := a.internalQueue[0]
		a.internalQueue = a.internalQueue[1:]
		a.processEventLocked(next.ctx, next.event, nil)
	}
	a.internalQueue = nil
}

// unlockAndDispatch finishes a step: it processes queued internal events,
// releases the actor lock and then delivers external events raised while it
// was held. Must be called with a.mu held.
func (a *Actor[S, E, D]) unlockAndDispatch() {
	a.drainInternalLocked()
	pending := a.pendingRaises
	a.pendingRaises = nil
	a.mu.Unlock()
	for _, p := range pending {
		a.raiseHandler(p.ctx, p.event)
	}
}

// guardLocked evaluates t's guard against the event being handled. A
// transition without a guard passes. Must be called with a.mu held.
func (a *Actor[S, E, D]) guardLocked(t *TransitionDef[S, E, D]) bool {
	switch {
	case t.EventGuard != nil:
		return t.EventGuard(a.data, a.currentEvent)
	case t.Guard != nil:
		return t.Guard(a.data)
	}
	return true
}

// actionLocked runs t's actions, reporting whether it had any. Must be called
// with a.mu held.
func (a *Actor[S, E, D]) actionLocked(t *TransitionDef[S, E, D]) bool {
	switch {
	case t.EventAction != nil:
		a.data = t.EventAction(a.data, a.currentEvent)
	case t.Action != nil:
		a.data = t.Action(a.data)
	default:
		return false
	}
	return true
}

func (t *TransitionDef[S, E, D]) hasGuard() bool { return t.Guard != nil || t.EventGuard != nil }

func (t *TransitionDef[S, E, D]) hasAction() bool { return t.Action != nil || t.EventAction != nil }
