package gstate

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestSendWithAssignWith verifies that args passed via SendWith reach AssignWith.
func TestSendWithAssignWith(t *testing.T) {
	type MyData struct {
		Value int
	}
	type myDataType = MyData
	_ = myDataType{}

	type D struct{ Value int }
	type S string
	type E string
	clone := func(d D) D { return d }
	_ = clone

	// Use the package-level test types to keep things simple.
	m := New[StateID, EventID, Context]("sendwith_assign").
		Initial("idle").
		State("idle", func(s *StateBuilder[StateID, EventID, Context]) {
			s.On("SET").
				AssignWith(func(d Context, args any) Context {
					if v, ok := args.(int); ok {
						d.Count = v
					}
					return d
				}).
				GoTo("idle")
		}).
		Build()

	a := Start(m, Context{})
	defer a.Stop()

	a.SendWith("SET", 42)

	// Wait for the transition by observing data.
	deadline := time.After(time.Second)
	for {
		snap := a.Snapshot()
		if snap.Data.Count == 42 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("data.Count = %d, want 42", snap.Data.Count)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// TestSendWithGuardWith verifies that args passed via SendWith reach GuardWith.
func TestSendWithGuardWith(t *testing.T) {
	entered := make(chan struct{}, 1)

	m := New[StateID, EventID, Context]("sendwith_guard").
		Initial("a").
		State("a", func(s *StateBuilder[StateID, EventID, Context]) {
			// Only transition if the int arg is > 0.
			s.On("GO").
				GuardWith(func(_ Context, args any) bool {
					v, ok := args.(int)
					return ok && v > 0
				}).
				GoTo("b")
		}).
		State("b", func(s *StateBuilder[StateID, EventID, Context]) {
			s.Entry(func(d Context) Context {
				select {
				case entered <- struct{}{}:
				default:
				}
				return d
			})
		}).
		Build()

	a := Start(m, Context{})
	defer a.Stop()

	// Negative arg: guard should block the transition.
	a.SendWith("GO", -1)
	// Positive arg: guard should allow the transition.
	a.SendWith("GO", 1)

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("state b was never entered; GuardWith may not have fired")
	}
}

// TestSendCtxWithPropagatesArgs verifies SendCtxWith threads args and context.
func TestSendCtxWithPropagatesArgs(t *testing.T) {
	got := make(chan int, 1)

	m := New[StateID, EventID, Context]("sendctxwith").
		Initial("s").
		State("s", func(s *StateBuilder[StateID, EventID, Context]) {
			s.On("DATA").
				AssignWith(func(d Context, args any) Context {
					if v, ok := args.(int); ok {
						select {
						case got <- v:
						default:
						}
					}
					return d
				}).
				GoTo("s")
		}).
		Build()

	a := Start(m, Context{})
	defer a.Stop()

	if err := a.SendCtxWith(context.Background(), "DATA", 99); err != nil {
		t.Fatalf("SendCtxWith returned error: %v", err)
	}

	select {
	case v := <-got:
		if v != 99 {
			t.Fatalf("args = %d, want 99", v)
		}
	case <-time.After(time.Second):
		t.Fatal("AssignWith was never called")
	}
}

// TestSendCtxWithCancelledCtx verifies that a cancelled context prevents delivery.
func TestSendCtxWithCancelledCtx(t *testing.T) {
	m := tinyMachine()
	a := Start(m, Context{})
	defer a.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := a.SendCtxWith(ctx, "GO", "payload")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// TestSendCtxWithErrActorStopped verifies the stopped sentinel is returned post-Stop.
func TestSendCtxWithErrActorStopped(t *testing.T) {
	m := tinyMachine()
	a := Start(m, Context{})
	a.Stop()

	err := a.SendCtxWith(context.Background(), "GO", "data")
	if !errors.Is(err, ErrActorStopped) {
		t.Fatalf("err = %v, want ErrActorStopped", err)
	}
}

// TestSendWithNilArgsFallsThrough verifies that SendWith with nil args still fires
// a transition that uses a plain Assign (no args required).
func TestSendWithNilArgsFallsThrough(t *testing.T) {
	m := New[StateID, EventID, Context]("sendwith_nil").
		Initial("a").
		State("a", func(s *StateBuilder[StateID, EventID, Context]) {
			s.On("BUMP").
				Assign(func(d Context) Context { d.Count++; return d }).
				GoTo("a")
		}).
		Build()

	a := Start(m, Context{})
	defer a.Stop()

	a.SendWith("BUMP", nil)

	deadline := time.After(time.Second)
	for {
		if a.Snapshot().Data.Count == 1 {
			return
		}
		select {
		case <-deadline:
			t.Fatal("plain Assign was never reached via SendWith")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}
