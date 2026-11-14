package gstate

import "context"

// InvokeHandle is the typed interface an [InvokeDef.Run] service uses to talk
// to its actor: update data, raise events to the external system, or both at
// once. A handle is valid only while its invoke runs. Once the service
// returns, its state exits or the actor stops, every method is a no-op that
// returns false.
//
// Handle methods take the actor lock. Do not call them from a lock-held actor
// callback, including from inside a function passed to Mutate or Update.
type InvokeHandle[S ~string, E ~string, D Cloner[D]] struct {
	a     *Actor[S, E, D]
	state S
	gen   uint64
	ctx   context.Context
}

// State returns the state whose invoke owns this handle.
func (h *InvokeHandle[S, E, D]) State() S { return h.state }

// Mutate applies fn to the live actor data, with the same semantics as the
// mutate callback of [InvokeDef.Func]. It reports whether the update was
// committed.
func (h *InvokeHandle[S, E, D]) Mutate(fn func(D) D) bool {
	return h.commit(fn, nil)
}

// Raise emits events to the handler installed with [Machine.WithRaiseHandler],
// in order and before Raise returns, with this invoke's state as
// [RaisedEvent.State]. Empty events are skipped. Raise reports whether any
// event was delivered; it returns false when the handle is stale, no handler
// is installed or no nonempty event was given.
func (h *InvokeHandle[S, E, D]) Raise(events ...E) bool {
	return h.raise(h.ctx, events)
}

// Update applies fn to the live actor data and raises events in the same
// atomic step: the handler sees the events only after the update is
// committed, and no other step can run in between. Events go out after fn and
// before any eventless transitions the update triggers. Update reports whether
// the update was committed; without a raise handler the events are discarded
// but the update still commits.
func (h *InvokeHandle[S, E, D]) Update(fn func(D) D, events ...E) bool {
	return h.commit(fn, events)
}

// validLocked reports whether the handle may act. Must be called with a.mu held.
func (h *InvokeHandle[S, E, D]) validLocked() bool {
	select {
	case <-h.a.stopped:
		return false
	default:
	}
	// Reject once the invoke has finished, the state has exited, or a newer
	// entry has superseded this invoke's generation.
	return h.a.invokeGens[h.state] == h.gen
}

func (h *InvokeHandle[S, E, D]) commit(fn func(D) D, events []E) bool {
	a := h.a
	a.mu.Lock()
	defer a.unlockAndDispatch()
	if !h.validLocked() {
		return false
	}
	// A committed invoke update is an eventless targetless action. Reuse
	// transition notifications and settle before returning.
	var zero E
	a.executeTransition(h.ctx, h.state, &TransitionDef[S, E, D]{Action: fn, Raise: nonEmpty(events)}, zero)
	a.stabilizeLocked(h.ctx)
	a.getSortedActiveStatesLocked()
	a.checkAndInitiateAutoStopLocked()
	return true
}

func (h *InvokeHandle[S, E, D]) raise(ctx context.Context, events []E) bool {
	a := h.a
	a.mu.Lock()
	defer a.unlockAndDispatch()
	events = nonEmpty(events)
	if !h.validLocked() || a.raiseHandler == nil || len(events) == 0 {
		return false
	}
	a.queueRaisesLocked(ctx, h.state, events, nil)
	return true
}

func nonEmpty[E ~string](events []E) []E {
	var out []E
	for _, ev := range events {
		if ev != "" {
			out = append(out, ev)
		}
	}
	return out
}

// InvokeBuilder configures declarative behavior of an invoke's completion.
// It is returned by [StateBuilder.Invoke], [StateBuilder.InvokeWith] and their
// Replace variants. A builder returned by a rejected call is detached: its
// settings have no effect.
type InvokeBuilder[S ~string, E ~string, D Cloner[D]] struct {
	def *InvokeDef[S, E, D]
}

// RaiseOnDone appends events raised to the external system when the service
// returns nil. They are delivered once the completion step, including the
// OnDone transition, has committed, and are raised even when OnDone has no
// target. Empty events are ignored. A cancelled invoke (state exited or actor
// stopped) raises nothing.
func (b *InvokeBuilder[S, E, D]) RaiseOnDone(events ...E) *InvokeBuilder[S, E, D] {
	b.def.DoneRaise = append(b.def.DoneRaise, nonEmpty(events)...)
	return b
}

// RaiseOnError appends events raised to the external system when the service
// returns an error. Each [RaisedEvent] carries that error in Err. They are
// raised even when OnError has no target. Empty events are ignored.
func (b *InvokeBuilder[S, E, D]) RaiseOnError(events ...E) *InvokeBuilder[S, E, D] {
	b.def.ErrorRaise = append(b.def.ErrorRaise, nonEmpty(events)...)
	return b
}
