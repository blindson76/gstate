package gstate

import (
	"context"
	"encoding/xml"
	"strings"
	"testing"
	"time"
)

// raiseRecorder returns a raise handler that forwards every delivered event
// to a buffered channel, plus the channel itself.
func raiseRecorder() (func(context.Context, RaisedEvent[StateID, EventID]), chan RaisedEvent[StateID, EventID]) {
	ch := make(chan RaisedEvent[StateID, EventID], 16)
	return func(_ context.Context, e RaisedEvent[StateID, EventID]) { ch <- e }, ch
}

func awaitRaised(t *testing.T, ch <-chan RaisedEvent[StateID, EventID]) RaisedEvent[StateID, EventID] {
	t.Helper()
	select {
	case e := <-ch:
		return e
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for raised event")
	}
	return RaisedEvent[StateID, EventID]{}
}

func TestAfterRaiseWithoutTargetDeliversEventAndStays(t *testing.T) {
	// A real timer duration is the behavior under test (After); the raise
	// handler deterministically signals delivery.
	m := New[StateID, EventID, Context]("after-raise").
		Initial("waiting").
		State("waiting", func(s *StateBuilder[StateID, EventID, Context]) {
			s.After(time.Millisecond).Raise("TIMEOUT")
		}).
		Build()

	handler, ch := raiseRecorder()
	actor := Start(m, Context{}, m.WithRaiseHandler(handler), m.WithActorID("a-1"))
	defer actor.Stop()

	got := awaitRaised(t, ch)
	if got.Event != "TIMEOUT" || got.State != "waiting" || got.ActorID != "a-1" {
		t.Fatalf("raised = %+v, want Event=TIMEOUT State=waiting ActorID=a-1", got)
	}
	if actor.State() != "waiting" {
		t.Fatalf("state = %s, want waiting (targetless raise must not exit)", actor.State())
	}
}

func TestAfterRaiseWithTargetDeliversInOrderAndTransitions(t *testing.T) {
	// A real timer duration is the behavior under test (After); the raise
	// handler deterministically signals delivery.
	m := New[StateID, EventID, Context]("after-raise-goto").
		Initial("a").
		State("a", func(s *StateBuilder[StateID, EventID, Context]) {
			s.After(time.Millisecond).Raise("FIRST").Raise("SECOND").GoTo("b")
		}).
		State("b", func(*StateBuilder[StateID, EventID, Context]) {}).
		Build()

	handler, ch := raiseRecorder()
	actor := Start(m, Context{}, m.WithRaiseHandler(handler))
	defer actor.Stop()

	if e := awaitRaised(t, ch); e.Event != "FIRST" {
		t.Fatalf("first raised = %q, want FIRST", e.Event)
	}
	if e := awaitRaised(t, ch); e.Event != "SECOND" {
		t.Fatalf("second raised = %q, want SECOND", e.Event)
	}
	if actor.State() != "b" {
		t.Fatalf("state = %s, want b", actor.State())
	}
}

func TestEventTransitionRaise(t *testing.T) {
	m := New[StateID, EventID, Context]("on-raise").
		Initial("idle").
		State("idle", func(s *StateBuilder[StateID, EventID, Context]) {
			s.On("GO").Raise("STARTED").GoTo("running")
		}).
		State("running", func(*StateBuilder[StateID, EventID, Context]) {}).
		Build()

	handler, ch := raiseRecorder()
	actor := Start(m, Context{}, m.WithRaiseHandler(handler))
	defer actor.Stop()

	actor.Send("GO")
	if e := awaitRaised(t, ch); e.Event != "STARTED" || e.State != "idle" {
		t.Fatalf("raised = %+v, want Event=STARTED State=idle", e)
	}
}

func TestAlwaysRaiseOnStart(t *testing.T) {
	m := New[StateID, EventID, Context]("always-raise").
		Initial("boot").
		State("boot", func(s *StateBuilder[StateID, EventID, Context]) {
			s.Always().Raise("BOOTED").GoTo("ready")
		}).
		State("ready", func(*StateBuilder[StateID, EventID, Context]) {}).
		Build()

	handler, ch := raiseRecorder()
	actor := Start(m, Context{}, m.WithRaiseHandler(handler))
	defer actor.Stop()

	// Start stabilizes synchronously and dispatches after releasing the lock,
	// so the event is already buffered when Start returns.
	select {
	case e := <-ch:
		if e.Event != "BOOTED" {
			t.Fatalf("raised = %q, want BOOTED", e.Event)
		}
	default:
		t.Fatal("expected BOOTED to be delivered before Start returned")
	}
}

func TestRaiseGuardFalseDoesNotDeliver(t *testing.T) {
	m := New[StateID, EventID, Context]("raise-guard").
		Initial("idle").
		State("idle", func(s *StateBuilder[StateID, EventID, Context]) {
			s.On("GO").Guard(func(Context) bool { return false }).Raise("NOPE").GoTo("running")
			s.On("PING").Raise("PONG")
		}).
		State("running", func(*StateBuilder[StateID, EventID, Context]) {}).
		Build()

	handler, ch := raiseRecorder()
	actor := Start(m, Context{}, m.WithRaiseHandler(handler))
	defer actor.Stop()

	actor.Send("GO")
	actor.Send("PING") // mailbox is FIFO: PONG arriving proves GO was processed.
	if e := awaitRaised(t, ch); e.Event != "PONG" {
		t.Fatalf("raised = %q, want PONG (guarded raise must not deliver)", e.Event)
	}
}

func TestRaiseHandlerMaySendToActor(t *testing.T) {
	// The handler runs outside the actor lock, so it may feed events back.
	m := New[StateID, EventID, Context]("raise-reentrant").
		Initial("a").
		State("a", func(s *StateBuilder[StateID, EventID, Context]) {
			s.On("GO").Raise("ECHO").GoTo("b")
		}).
		State("b", func(s *StateBuilder[StateID, EventID, Context]) {
			s.On("ECHO").GoTo("c")
		}).
		State("c", func(s *StateBuilder[StateID, EventID, Context]) { s.Type(Final) }).
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

	actor.Send("GO")
	select {
	case <-actor.Done():
	case <-time.After(time.Second):
		t.Fatalf("actor did not reach final state; state=%s", actor.State())
	}
}

func TestRaiseWithoutHandlerIsNoop(t *testing.T) {
	m := New[StateID, EventID, Context]("raise-nohandler").
		Initial("a").
		State("a", func(s *StateBuilder[StateID, EventID, Context]) {
			s.On("GO").Raise("X").GoTo("b")
		}).
		State("b", func(s *StateBuilder[StateID, EventID, Context]) { s.Type(Final) }).
		Build()

	actor := Start(m, Context{})
	actor.Send("GO")
	select {
	case <-actor.Done():
	case <-time.After(time.Second):
		t.Fatal("actor did not reach final state")
	}
}

func TestRaiseEmptyEventIgnored(t *testing.T) {
	m := New[StateID, EventID, Context]("raise-empty").
		Initial("a").
		State("a", func(s *StateBuilder[StateID, EventID, Context]) {
			s.On("GO").Raise("").GoTo("b")
		}).
		State("b", func(*StateBuilder[StateID, EventID, Context]) {}).
		Build()
	if got := m.States["a"].Transitions["GO"][0].Raise; len(got) != 0 {
		t.Fatalf("Raise = %v, want empty", got)
	}
}

func TestRaiseExports(t *testing.T) {
	m := New[StateID, EventID, Context]("raise-export").
		Initial("a").
		State("a", func(s *StateBuilder[StateID, EventID, Context]) {
			s.After(time.Second).Raise("TIMEOUT").GoTo("b")
		}).
		State("b", func(*StateBuilder[StateID, EventID, Context]) {}).
		Build()

	doc, err := ToSCXML(m)
	if err != nil {
		t.Fatal(err)
	}
	out, err := xml.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), `<send event="TIMEOUT"></send>`) {
		t.Errorf("SCXML missing <send event=\"TIMEOUT\">: %s", out)
	}

	if mmd := ToMermaid(m); !strings.Contains(mmd, "raise TIMEOUT") {
		t.Errorf("Mermaid missing raise label:\n%s", mmd)
	}
}
