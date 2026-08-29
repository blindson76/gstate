package gstate

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// TestDispatchReturnsAfterProcessing verifies that Dispatch blocks until
// the actor has processed the event: the state is already updated when
// Dispatch returns (no observer, no sleep needed).
func TestDispatchReturnsAfterProcessing(t *testing.T) {
	m := tinyMachine()
	a := Start(m, Context{})
	defer a.Stop()

	if err := a.Dispatch("GO"); err != nil {
		t.Fatalf("Dispatch err = %v, want nil", err)
	}
	if got := a.State(); got != "b" {
		t.Errorf("State after Dispatch = %q, want %q", got, "b")
	}
}

// TestDispatchCtxReturnsAfterProcessing mirrors TestDispatchReturnsAfterProcessing
// but uses the explicit DispatchCtx variant.
func TestDispatchCtxReturnsAfterProcessing(t *testing.T) {
	m := tinyMachine()
	a := Start(m, Context{})
	defer a.Stop()

	if err := a.DispatchCtx(context.Background(), "GO"); err != nil {
		t.Fatalf("DispatchCtx err = %v, want nil", err)
	}
	if got := a.State(); got != "b" {
		t.Errorf("State after DispatchCtx = %q, want %q", got, "b")
	}
}

// TestDispatchWithPropagatesArgs verifies that DispatchWith threads the
// args value through to a GuardWith/AssignWith handler.
func TestDispatchWithPropagatesArgs(t *testing.T) {
	m := New[StateID, EventID, Context]("args-machine").
		Initial("idle").
		State("idle", func(s *StateBuilder[StateID, EventID, Context]) {
			s.On("INC").
				AssignWith(func(c Context, args any) Context {
					c.Count += args.(int)
					return c
				}).
				GoTo("idle")
		}).
		Build()

	a := Start(m, Context{})
	defer a.Stop()

	if err := a.DispatchWith("INC", 5); err != nil {
		t.Fatalf("DispatchWith err = %v, want nil", err)
	}
	if got := a.Data().Count; got != 5 {
		t.Errorf("Count = %d, want 5", got)
	}
}

// TestDispatchChainedAlwaysComplete verifies that when an Always transition
// fires in response to the event, Dispatch returns only after that chained
// transition has also completed (the machine is in a stable configuration).
func TestDispatchChainedAlwaysComplete(t *testing.T) {
	m := New[StateID, EventID, Context]("chain").
		Initial("idle").
		State("idle", func(s *StateBuilder[StateID, EventID, Context]) {
			s.On("GO").GoTo("mid")
		}).
		State("mid", func(s *StateBuilder[StateID, EventID, Context]) {
			// Always fires immediately after entering mid → continues to final.
			s.Always().GoTo("final")
		}).
		State("final", func(_ *StateBuilder[StateID, EventID, Context]) {}).
		Build()

	a := Start(m, Context{})
	defer a.Stop()

	if err := a.Dispatch("GO"); err != nil {
		t.Fatalf("Dispatch err = %v, want nil", err)
	}
	// After Dispatch returns the machine must have settled in "final",
	// not "mid" (chained Always must have executed).
	if got := a.State(); got != "final" {
		t.Errorf("State = %q, want %q", got, "final")
	}
}

// TestDispatchCtxCancelledBeforeEnqueue returns ctx.Err() when the context
// is already cancelled before the event reaches the mailbox.
func TestDispatchCtxCancelledBeforeEnqueue(t *testing.T) {
	m := tinyMachine()
	a := Start(m, Context{})
	defer a.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	err := a.DispatchCtx(ctx, "GO")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("DispatchCtx(cancelled ctx) err = %v, want context.Canceled", err)
	}
	// State must be unchanged.
	if got := a.State(); got != "a" {
		t.Errorf("State = %q, want %q (event must not have been delivered)", got, "a")
	}
}

// TestDispatchStoppedBeforeEnqueue returns ErrActorStopped when Stop is
// called before the event can be enqueued.
func TestDispatchStoppedBeforeEnqueue(t *testing.T) {
	m := tinyMachine()
	a := Start(m, Context{})
	a.Stop()

	err := a.Dispatch("GO")
	if !errors.Is(err, ErrActorStopped) {
		t.Fatalf("Dispatch on stopped actor: err = %v, want ErrActorStopped", err)
	}
}

// TestDispatchIntoFinalAutoStop verifies that when Dispatch causes a
// transition into a top-level Final state (triggering auto-stop), Dispatch
// still returns nil — the event was fully processed. Auto-stop races
// with the caller but the double-select in Dispatch prioritises done.
func TestDispatchIntoFinalAutoStop(t *testing.T) {
	m := New[StateID, EventID, Context]("final-dispatch").
		Initial("active").
		State("active", func(s *StateBuilder[StateID, EventID, Context]) {
			s.On("FINISH").GoTo("done")
		}).
		State("done", func(s *StateBuilder[StateID, EventID, Context]) {
			s.Type(Final)
		}).
		Build()

	a := Start(m, Context{})
	defer a.Stop()

	err := a.Dispatch("FINISH")
	if err != nil && !errors.Is(err, ErrActorStopped) {
		t.Fatalf("Dispatch(FINISH) err = %v, want nil or ErrActorStopped", err)
	}
	// Regardless of which value Dispatch returned, the actor must have
	// processed the event (active is no longer the state).
	snap := a.Snapshot()
	for _, sID := range snap.Active {
		if sID == "active" {
			t.Errorf("Snapshot still contains 'active' after FINISH; active = %v", snap.Active)
		}
	}
}

// TestDispatchDataUpdateVisibleAfterReturn verifies that data mutations
// made inside a transition's Assign action are visible to the caller as
// soon as Dispatch returns.
func TestDispatchDataUpdateVisibleAfterReturn(t *testing.T) {
	var entryCount atomic.Int32

	m := New[StateID, EventID, Context]("data").
		Initial("idle").
		State("idle", func(s *StateBuilder[StateID, EventID, Context]) {
			s.Entry(func(c Context) Context {
				entryCount.Add(1)
				return c
			})
			s.On("INC").
				Assign(func(c Context) Context {
					c.Count++
					return c
				}).
				GoTo("idle")
		}).
		Build()

	a := Start(m, Context{})
	defer a.Stop()

	const n = 10
	for range n {
		if err := a.Dispatch("INC"); err != nil {
			t.Fatalf("Dispatch err = %v", err)
		}
	}

	if got := a.Data().Count; got != n {
		t.Errorf("Count = %d, want %d", got, n)
	}
	// Each Dispatch causes a self-transition: Exit + Entry fires once.
	// Initial Start fires Entry once; n dispatches fire Entry n more times.
	if got := entryCount.Load(); got != int32(n+1) {
		t.Errorf("entryCount = %d, want %d", got, n+1)
	}
}
