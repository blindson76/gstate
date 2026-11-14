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

// Raise emits event with the given options, like [TransitionBuilder.Raise]:
// external by default, or back into the actor with [Internal], with optional
// [Params]. Before Raise returns, internal events are processed with any
// transitions they trigger (unless [Machine.WithInternalViaHandler] routes
// them only to the handler), and every event, internal or external, has
// reached the raise handler if one is installed. The invoke's state is the
// raising state. Raise reports whether the event was
// delivered or queued; it returns false when the handle is stale, the event
// is empty, or an external event has no raise handler.
func (h *InvokeHandle[S, E, D]) Raise(event E, opts ...RaiseOption) bool {
	return h.raise(h.ctx, event, opts)
}

// Update applies fn to the live actor data and raises event in the same
// atomic step: the event is raised only after the update is committed, and no
// other step can run in between. The event goes out after fn and before any
// eventless transitions the update triggers. Options are as for Raise; an
// empty event raises nothing. Update reports whether the update was
// committed; without a raise handler an external event is discarded but the
// update still commits.
func (h *InvokeHandle[S, E, D]) Update(fn func(D) D, event E, opts ...RaiseOption) bool {
	var raises []RaiseDef[E, D]
	if event != "" {
		raises = []RaiseDef[E, D]{newRaiseDef[E, D](event, nil, opts)}
	}
	return h.commit(fn, raises)
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

func (h *InvokeHandle[S, E, D]) commit(fn func(D) D, raises []RaiseDef[E, D]) bool {
	a := h.a
	a.mu.Lock()
	defer a.unlockAndDispatch()
	if !h.validLocked() {
		return false
	}
	// A committed invoke update is an eventless targetless action. Reuse
	// transition notifications and settle before returning.
	var zero E
	a.executeTransition(h.ctx, h.state, &TransitionDef[S, E, D]{Action: fn, Raise: raises}, zero)
	a.stabilizeLocked(h.ctx)
	a.getSortedActiveStatesLocked()
	a.checkAndInitiateAutoStopLocked()
	return true
}

func (h *InvokeHandle[S, E, D]) raise(ctx context.Context, event E, opts []RaiseOption) bool {
	a := h.a
	a.mu.Lock()
	defer a.unlockAndDispatch()
	if event == "" || !h.validLocked() {
		return false
	}
	return a.raiseLocked(ctx, h.state, []RaiseDef[E, D]{newRaiseDef[E, D](event, nil, opts)}, nil)
}

// InvokeBuilder configures declarative behavior of an invoke's completion.
// It is returned by [StateBuilder.Invoke], [StateBuilder.InvokeWith] and their
// Replace variants. A builder returned by a rejected call is detached: its
// settings have no effect.
type InvokeBuilder[S ~string, E ~string, D Cloner[D]] struct {
	def *InvokeDef[S, E, D]
}

// RaiseOnDone appends an event raised when the service returns nil. Options
// are as for [TransitionBuilder.Raise]. The event is raised as part of the
// completion step, even when OnDone has no target; external events reach the
// handler once that step, including the OnDone transition, has committed. A
// cancelled invoke (state exited or actor stopped) raises nothing. Empty
// events are ignored. Call it again to raise more events, in order.
func (b *InvokeBuilder[S, E, D]) RaiseOnDone(event E, opts ...RaiseOption) *InvokeBuilder[S, E, D] {
	if event != "" {
		b.def.DoneRaise = append(b.def.DoneRaise, newRaiseDef[E, D](event, nil, opts))
	}
	return b
}

// RaiseOnError is [InvokeBuilder.RaiseOnDone] for a service that returns an
// error. External events carry the error in [RaisedEvent.Err]; internal events
// carry it in their [Params] under [ParamError].
func (b *InvokeBuilder[S, E, D]) RaiseOnError(event E, opts ...RaiseOption) *InvokeBuilder[S, E, D] {
	if event != "" {
		b.def.ErrorRaise = append(b.def.ErrorRaise, newRaiseDef[E, D](event, nil, opts))
	}
	return b
}
