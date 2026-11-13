package gstate

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestInvokeRaiseDeliversInOrderBeforeCompletion(t *testing.T) {
	results := make(chan bool, 2)
	m := New[StateID, EventID, Context]("invoke-raise").
		Initial("working").
		State("working", func(s *StateBuilder[StateID, EventID, Context]) {
			s.Invoke(func(ctx context.Context, _ Context, _ func(func(Context) Context)) error {
				results <- Raise(ctx, EventID("PROGRESS_1"))
				results <- Raise(ctx, EventID("PROGRESS_2"))
				return nil
			}, "done", "failed")
		}).
		State("done", func(s *StateBuilder[StateID, EventID, Context]) { s.Type(Final) }).
		State("failed", func(s *StateBuilder[StateID, EventID, Context]) { s.Type(Final) }).
		Build()

	handler, ch := raiseRecorder()
	actor := Start(m, Context{}, m.WithRaiseHandler(handler), m.WithActorID("inv-1"))
	defer actor.Stop()

	for _, want := range []EventID{"PROGRESS_1", "PROGRESS_2"} {
		got := awaitRaised(t, ch)
		if got.Event != want || got.State != "working" || got.ActorID != "inv-1" {
			t.Fatalf("raised = %+v, want Event=%s State=working ActorID=inv-1", got, want)
		}
		if ok := <-results; !ok {
			t.Fatalf("Raise(%s) = false, want true", want)
		}
	}
	select {
	case <-actor.Done():
	case <-time.After(time.Second):
		t.Fatal("invoke completion did not reach final state")
	}
}

func TestInvokeRaiseAcceptsUntypedString(t *testing.T) {
	m := New[StateID, EventID, Context]("invoke-raise-untyped").
		Initial("working").
		State("working", func(s *StateBuilder[StateID, EventID, Context]) {
			s.Invoke(func(ctx context.Context, _ Context, _ func(func(Context) Context)) error {
				Raise(ctx, "PING") // E infers as string; converted to the machine's event type.
				<-ctx.Done()
				return nil
			}, "", "")
		}).
		Build()

	handler, ch := raiseRecorder()
	actor := Start(m, Context{}, m.WithRaiseHandler(handler))
	defer actor.Stop()

	if got := awaitRaised(t, ch); got.Event != "PING" {
		t.Fatalf("raised = %q, want PING", got.Event)
	}
}

func TestInvokeRaiseAfterCompletionIsRejected(t *testing.T) {
	retained := make(chan context.Context, 1)
	m := New[StateID, EventID, Context]("invoke-raise-stale").
		Initial("working").
		State("working", func(s *StateBuilder[StateID, EventID, Context]) {
			s.Invoke(func(ctx context.Context, _ Context, _ func(func(Context) Context)) error {
				retained <- ctx
				return errors.New("boom")
			}, "done", "failed")
		}).
		State("done", func(*StateBuilder[StateID, EventID, Context]) {}).
		State("failed", func(s *StateBuilder[StateID, EventID, Context]) {
			s.On("PING").Raise("PONG")
		}).
		Build()

	handler, ch := raiseRecorder()
	actor := Start(m, Context{}, m.WithRaiseHandler(handler))
	defer actor.Stop()

	ctx := <-retained
	// PONG is raised from the failed state, so the invoke has completed and
	// its state exited by the time it is delivered.
	actor.Send("PING")
	if got := awaitRaised(t, ch); got.Event != "PONG" {
		t.Fatalf("raised = %q, want PONG", got.Event)
	}
	if Raise(ctx, EventID("LATE")) {
		t.Fatal("Raise after invoke completion = true, want false")
	}
	select {
	case got := <-ch:
		t.Fatalf("unexpected delivery after completion: %+v", got)
	default:
	}
}

func TestInvokeRaiseWithoutHandlerReturnsFalse(t *testing.T) {
	result := make(chan bool, 1)
	m := New[StateID, EventID, Context]("invoke-raise-nohandler").
		Initial("working").
		State("working", func(s *StateBuilder[StateID, EventID, Context]) {
			s.Invoke(func(ctx context.Context, _ Context, _ func(func(Context) Context)) error {
				result <- Raise(ctx, EventID("X"))
				return nil
			}, "", "")
		}).
		Build()

	actor := Start(m, Context{})
	defer actor.Stop()
	if <-result {
		t.Fatal("Raise without handler = true, want false")
	}
}

func TestRaiseOutsideInvokeReturnsFalse(t *testing.T) {
	if Raise(context.Background(), "X") {
		t.Fatal("Raise outside an invoke = true, want false")
	}
}

func TestInvokeRaiseHandlerMaySendToActor(t *testing.T) {
	m := New[StateID, EventID, Context]("invoke-raise-reentrant").
		Initial("working").
		State("working", func(s *StateBuilder[StateID, EventID, Context]) {
			s.Invoke(func(ctx context.Context, _ Context, _ func(func(Context) Context)) error {
				Raise(ctx, EventID("FINISH"))
				<-ctx.Done()
				return nil
			}, "", "")
			s.On("FINISH").GoTo("done")
		}).
		State("done", func(s *StateBuilder[StateID, EventID, Context]) { s.Type(Final) }).
		Build()

	var actor *Actor[StateID, EventID, Context]
	ready := make(chan struct{})
	handler := func(_ context.Context, e RaisedEvent[StateID, EventID]) {
		<-ready
		actor.Send(e.Event)
	}
	actor = Start(m, Context{}, m.WithRaiseHandler(handler), m.WithMailboxSize(1))
	close(ready)
	defer actor.Stop()

	select {
	case <-actor.Done():
	case <-time.After(time.Second):
		t.Fatalf("actor did not reach final state; state=%s", actor.State())
	}
}
