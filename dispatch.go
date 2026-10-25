package gstate

import (
	"context"
	"time"
)

// TriggerKind identifies which background service produced an [InternalTrigger].
type TriggerKind string

const (
	// TriggerAfter is produced when a delayed ([StateBuilder.After]) transition fires.
	TriggerAfter TriggerKind = "after"
	// TriggerInvokeDone is produced when an invoke returns nil and has an OnDone target.
	TriggerInvokeDone TriggerKind = "invoke.done"
	// TriggerInvokeError is produced when an invoke returns an error and has an OnError target.
	TriggerInvokeError TriggerKind = "invoke.error"
)

// InternalTrigger describes a transition requested by a background service
// (a delayed transition or an invoke completion) rather than by an external
// event. It is plain data so it can be serialized, replicated (for example
// through a consensus log) and applied on every replica with
// [Actor.FireInternal].
type InternalTrigger[S ~string] struct {
	Kind   TriggerKind `json:"kind"`
	Source S           `json:"source"`
	Target S           `json:"target"`
	// Epoch is the entry epoch of Source at the time the service was started.
	// FireInternal ignores the trigger unless Source is still active with the
	// same epoch, so a trigger from an earlier visit of the state is a no-op.
	Epoch uint64 `json:"epoch"`
}

// WithInternalDispatch returns an [Option] that routes internal transitions
// through fn instead of executing them locally.
//
// When set, a fired delayed transition or a completed invoke does not change
// the actor's state. Instead fn receives an [InternalTrigger] describing the
// transition, and the caller is responsible for delivering it back with
// [Actor.FireInternal] — typically after replicating it, so that every
// replica applies the same transition at the same point in its event order.
//
// fn is called from the timer or invoke goroutine without the actor's lock
// held, so it may block or call into the actor. It is only called while the
// source state is still active in the epoch that started the service.
// Triggers can still become stale before they are applied; FireInternal
// rejects those.
func (m *Machine[S, E, D]) WithInternalDispatch(fn func(InternalTrigger[S])) Option[S, E, D] {
	return func(c *config[S, E, D]) { c.dispatch = fn }
}

// FireInternal applies an internal transition previously handed out by the
// [WithInternalDispatch] callback. It reports whether the transition ran.
//
// The trigger is ignored (false) when its source state is no longer active or
// has been exited and re-entered since the trigger was produced. Because
// epochs advance deterministically with state entries and are carried in
// [Snapshot], replicas that apply the same sequence of events and triggers
// accept and reject the same triggers.
//
// FireInternal runs synchronously in the caller's goroutine, like Send in
// [Machine.WithRunToCompletion] mode, and works whether or not services are
// active on this actor.
func (a *Actor[S, E, D]) FireInternal(t InternalTrigger[S]) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	select {
	case <-a.stopped:
		return false
	default:
	}
	if !a.active[t.Source] || a.epochs[t.Source] != t.Epoch {
		return false
	}
	ctx := context.Background()
	var zero E
	a.executeTransition(ctx, t.Source, &TransitionDef[S, E, D]{Target: t.Target}, zero, nil)
	a.handleAlwaysInternal(ctx)
	return true
}

// Deactivate stops invoke and timer services for all active states, the
// reverse of [Actor.Activate]. State and data are kept; states entered while
// deactivated do not start services. A later Activate starts services again
// from scratch, so delayed transitions restart with their full duration.
//
// Invokes are cancelled through their context; their completion does not
// produce a transition or a trigger.
func (a *Actor[S, E, D]) Deactivate() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.servicesActive {
		return
	}
	a.servicesActive = false
	a.stopServicesLocked()
}

// stopServicesLocked cancels every running invoke and timer. Callers hold a.mu.
func (a *Actor[S, E, D]) stopServicesLocked() {
	for _, cancel := range a.invocations {
		cancel()
	}
	a.invocations = make(map[S]context.CancelFunc)
	a.invokeGens = make(map[S]uint64)

	for _, timers := range a.timers {
		for _, t := range timers {
			t.Stop()
		}
	}
	a.timers = make(map[S][]*time.Timer)
}

// internalTransition either executes a service-triggered transition locally
// or, with [WithInternalDispatch], hands it to the dispatcher.
func (a *Actor[S, E, D]) internalTransition(ctx context.Context, kind TriggerKind, sourceID, targetID S, epoch uint64) {
	if a.dispatch == nil {
		a.executeInternalTransition(ctx, sourceID, targetID)
		return
	}

	a.mu.RLock()
	current := a.servicesActive && a.active[sourceID] && a.epochs[sourceID] == epoch
	select {
	case <-a.stopped:
		current = false
	default:
	}
	a.mu.RUnlock()
	if !current {
		return
	}
	a.dispatch(InternalTrigger[S]{Kind: kind, Source: sourceID, Target: targetID, Epoch: epoch})
}
