package gstate

import (
	"context"
	"testing"
)

// TestRunToCompletion_SendIsSynchronous verifies that in RTC mode Send
// processes the event and completes all resulting transitions (including
// Always chains) before returning, so no external synchronisation is needed.
func TestRunToCompletion_SendIsSynchronous(t *testing.T) {
	m := tinyMachine()
	a := Start(m, Context{}, m.WithRunToCompletion())
	defer a.Stop()

	if got := a.State(); got != "a" {
		t.Fatalf("initial state = %q, want a", got)
	}

	a.Send("GO")

	// No sleep or barrier needed: RTC mode guarantees the state has changed.
	if got := a.State(); got != "b" {
		t.Errorf("after Send(GO) state = %q, want b", got)
	}
}

// TestRunToCompletion_SendCtxCancelled verifies that a pre-cancelled context
// causes SendCtx to return ctx.Err() without delivering the event.
func TestRunToCompletion_SendCtxCancelled(t *testing.T) {
	m := tinyMachine()
	a := Start(m, Context{}, m.WithRunToCompletion())
	defer a.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := a.SendCtx(ctx, "GO")
	if err != context.Canceled {
		t.Errorf("SendCtx with cancelled ctx = %v, want context.Canceled", err)
	}
	// Event must not have been delivered.
	if got := a.State(); got != "a" {
		t.Errorf("state = %q after dropped event, want a", got)
	}
}

// TestRunToCompletion_StoppedReturnsError verifies that SendCtx returns
// ErrActorStopped after the actor has been stopped.
func TestRunToCompletion_StoppedReturnsError(t *testing.T) {
	m := tinyMachine()
	a := Start(m, Context{}, m.WithRunToCompletion())
	a.Stop()

	err := a.SendCtx(context.Background(), "GO")
	if err != ErrActorStopped {
		t.Errorf("SendCtx after Stop = %v, want ErrActorStopped", err)
	}
}

// TestRunToCompletion_AlwaysTransitionsRunInline verifies that Always
// transitions chained after an event transition are processed synchronously
// within the same Send call in RTC mode.
func TestRunToCompletion_AlwaysTransitionsRunInline(t *testing.T) {
	m := New[StateID, EventID, Context]("rtc-always").
		Initial("start").
		State("start", func(s *StateBuilder[StateID, EventID, Context]) {
			s.On("STEP").GoTo("middle")
		}).
		State("middle", func(s *StateBuilder[StateID, EventID, Context]) {
			// Unconditional Always: immediately jump to "end".
			s.Always().GoTo("end")
		}).
		State("end", func(_ *StateBuilder[StateID, EventID, Context]) {}).
		Build()

	a := Start(m, Context{}, m.WithRunToCompletion())
	defer a.Stop()

	a.Send("STEP")

	// In RTC mode the Always chain must have fired inside Send.
	if got := a.State(); got != "end" {
		t.Errorf("state after STEP = %q, want end (Always chain not processed)", got)
	}
}

// TestRunToCompletion_SendWithIsSynchronous mirrors TestRunToCompletion_SendIsSynchronous
// for the SendWith / SendCtxWith variants.
func TestRunToCompletion_SendWithIsSynchronous(t *testing.T) {
	m := tinyMachine()
	a := Start(m, Context{}, m.WithRunToCompletion())
	defer a.Stop()

	a.SendWith("GO", nil)

	if got := a.State(); got != "b" {
		t.Errorf("after SendWith(GO) state = %q, want b", got)
	}
}

// TestRunToCompletion_ObserversFireSynchronously verifies that transition
// observers fire inside the Send call in RTC mode (i.e. the observer's
// callback has already run before Send returns).
func TestRunToCompletion_ObserversFireSynchronously(t *testing.T) {
	m := tinyMachine()
	fired := false
	obs := &funcTransitionObserver[StateID, EventID, Context]{
		fn: func(_ context.Context, _ *TransitionEvent[StateID, EventID, Context]) {
			fired = true
		},
	}

	a := Start(m, Context{}, m.WithRunToCompletion(), m.WithObservers(obs))
	defer a.Stop()

	a.Send("GO")

	if !fired {
		t.Error("transition observer did not fire synchronously in RTC mode")
	}
}

// funcTransitionObserver is a minimal TransitionObserver for testing.
type funcTransitionObserver[S ~string, E ~string, D Cloner[D]] struct {
	BaseObserver[S, E, D]
	fn func(context.Context, *TransitionEvent[S, E, D])
}

func (o *funcTransitionObserver[S, E, D]) OnTransition(ctx context.Context, e *TransitionEvent[S, E, D]) {
	o.fn(ctx, e)
}
