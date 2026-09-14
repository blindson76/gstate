package gstate

import (
	"context"
	"testing"
	"time"
)

func invokeMachine() *Machine[StateID, EventID, Context] {
	return New[StateID, EventID, Context]("invoke-activation").
		Initial("loading").
		State("loading", func(s *StateBuilder[StateID, EventID, Context]) {
			s.Invoke(func(ctx context.Context, _ Context, _ func(func(Context) Context)) error {
				<-ctx.Done()
				return ctx.Err()
			}, "", "")
		}).
		Build()
}

func TestStartWithActivateFalseDefersServicesUntilActivate(t *testing.T) {
	m := invokeMachine()
	rec := &RecordingObserver[StateID, EventID, Context]{}
	bar := newKindBarrier(KindInvokeStarted, 1)

	a := Start(m, Context{}, false, m.WithObservers(rec, bar))
	defer a.Stop()

	if got := len(rec.InvokeStarted()); got != 0 {
		t.Fatalf("invoke started before Activate: got %d", got)
	}

	a.Activate()

	select {
	case <-bar.done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for invoke start after Activate")
	}

	if got := len(rec.InvokeStarted()); got != 1 {
		t.Fatalf("invoke starts after Activate = %d, want 1", got)
	}
}

func TestHydrateWithActivateFalseDefersServicesUntilActivate(t *testing.T) {
	m := invokeMachine()
	original := Start(m, Context{}, false)
	snap := original.Snapshot()
	original.Stop()

	rec := &RecordingObserver[StateID, EventID, Context]{}
	bar := newKindBarrier(KindInvokeStarted, 1)
	revived := Hydrate(m, snap, false, m.WithObservers(rec, bar))
	defer revived.Stop()

	if got := len(rec.InvokeStarted()); got != 0 {
		t.Fatalf("invoke started before Activate: got %d", got)
	}

	revived.Activate()

	select {
	case <-bar.done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for invoke start after Activate")
	}

	if got := len(rec.InvokeStarted()); got != 1 {
		t.Fatalf("invoke starts after Activate = %d, want 1", got)
	}
}

func TestActivateIsIdempotentWhenServicesAlreadyStarted(t *testing.T) {
	m := invokeMachine()
	rec := &RecordingObserver[StateID, EventID, Context]{}
	bar := newKindBarrier(KindInvokeStarted, 1)
	a := Start(m, Context{}, true, m.WithObservers(rec, bar))
	defer a.Stop()

	select {
	case <-bar.done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for initial invoke start")
	}

	a.Activate()
	a.Activate()

	if got := len(rec.InvokeStarted()); got != 1 {
		t.Fatalf("invoke starts after repeated Activate = %d, want 1", got)
	}
}
