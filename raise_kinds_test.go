package gstate

import (
	"context"
	"encoding/xml"
	"errors"
	"strings"
	"testing"
	"time"
)

type sb = StateBuilder[StateID, EventID, Context]

func finalState(s *sb) { s.Type(Final) }

// --- internal raise ------------------------------------------------------

func TestInternalRaiseIsProcessedBeforeNextMailboxEvent(t *testing.T) {
	m := New[StateID, EventID, Context]("internal-order").
		Initial("a").
		State("a", func(s *sb) { s.On("GO").Raise("NEXT", Internal).GoTo("b") }).
		State("b", func(s *sb) {
			s.On("NEXT").GoTo("c")
			s.On("CHECK").GoTo("wrong")
		}).
		State("c", func(s *sb) { s.On("CHECK").GoTo("right") }).
		State("wrong", finalState).
		State("right", finalState).
		Build()

	handler, ch := raiseRecorder()
	actor := Start(m, Context{}, m.WithRaiseHandler(handler))
	defer actor.Stop()
	actor.Send("GO")
	actor.Send("CHECK")
	awaitDone(t, actor)
	if actor.State() != "right" {
		t.Fatalf("state = %s, want right (internal NEXT must run before mailbox CHECK)", actor.State())
	}
	// The handler sees the internal event too, flagged as such, and nothing else.
	if got := awaitRaised(t, ch); got.Event != "NEXT" || !got.Internal {
		t.Fatalf("raised = %+v, want internal NEXT", got)
	}
	select {
	case got := <-ch:
		t.Fatalf("unexpected extra raise: %+v", got)
	default:
	}
}

func TestInternalRaiseDuringStart(t *testing.T) {
	m := New[StateID, EventID, Context]("internal-start").
		Initial("boot").
		State("boot", func(s *sb) {
			s.Always().Raise("BOOTED", Internal).GoTo("waiting")
		}).
		State("waiting", func(s *sb) { s.On("BOOTED").GoTo("ready") }).
		State("ready", func(*sb) {}).
		Build()

	actor := Start(m, Context{})
	defer actor.Stop()
	if actor.State() != "ready" {
		t.Fatalf("state = %s, want ready before Start returns", actor.State())
	}
}

func TestInternalRaiseBudget(t *testing.T) {
	m := New[StateID, EventID, Context]("internal-loop").
		Initial("a").
		State("a", func(s *sb) { s.On("PING").Raise("PING", Internal) }).
		Build()

	actor := Start(m, Context{})
	actor.Send("PING")
	awaitDone(t, actor)
	if !errors.Is(actor.Err(), ErrInternalEventLimit) {
		t.Fatalf("Err() = %v, want ErrInternalEventLimit", actor.Err())
	}
}

func TestInternalEventNotice(t *testing.T) {
	notices := make(chan *EventNotice[StateID, EventID, Context], 4)
	m := New[StateID, EventID, Context]("internal-notice").
		Initial("a").
		State("a", func(s *sb) { s.On("GO").Raise("NEXT", Internal, Params{"k": "v"}).GoTo("b") }).
		State("b", func(s *sb) { s.On("NEXT").GoTo("c") }).
		State("c", finalState).
		Build()
	obs := ObserverFuncs[StateID, EventID, Context]{
		EventReceivedFunc: func(_ context.Context, e *EventNotice[StateID, EventID, Context]) { notices <- e },
	}
	actor := Start(m, Context{}, m.WithObservers(obs))
	defer actor.Stop()
	actor.Send("GO")
	awaitDone(t, actor)

	if e := <-notices; e.Event != "GO" || e.Internal {
		t.Fatalf("first notice = %+v, want external GO", e)
	}
	if e := <-notices; e.Event != "NEXT" || !e.Internal || e.Params["k"] != "v" {
		t.Fatalf("second notice = %+v, want internal NEXT with k=v", e)
	}
}

// --- params ---------------------------------------------------------------

func TestExternalRaiseParamsAreCopied(t *testing.T) {
	m := New[StateID, EventID, Context]("params-external").
		Initial("a").
		State("a", func(s *sb) { s.On("GO").Raise("X", Params{"a": 1}) }).
		Build()

	ch := make(chan any, 2)
	handler := func(_ context.Context, e RaisedEvent[StateID, EventID]) {
		ch <- e.Params["a"]
		e.Params["a"] = 99 // must not leak into the machine definition
	}
	actor := Start(m, Context{}, m.WithRaiseHandler(handler))
	defer actor.Stop()
	actor.Send("GO")
	actor.Send("GO")
	for range 2 {
		select {
		case got := <-ch:
			if got != 1 {
				t.Fatalf("Params[a] = %v, want 1", got)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for raised event")
		}
	}
}

func TestRaiseWithDynamicParams(t *testing.T) {
	m := New[StateID, EventID, Context]("params-dynamic").
		Initial("a").
		State("a", func(s *sb) {
			s.On("GO").
				Assign(func(c Context) Context { c.Count = 5; return c }).
				RaiseWith("X", func(c Context) Params { return Params{"count": c.Count, "k": "dynamic"} },
					Params{"k": "static", "s": true})
		}).
		Build()

	handler, ch := raiseRecorder()
	actor := Start(m, Context{}, m.WithRaiseHandler(handler))
	defer actor.Stop()
	actor.Send("GO")
	got := awaitRaised(t, ch)
	if got.Params["count"] != 5 || got.Params["k"] != "dynamic" || got.Params["s"] != true {
		t.Fatalf("Params = %v, want count=5 k=dynamic s=true", got.Params)
	}
}

func TestInternalParamsReachGuardEventAndAssignEvent(t *testing.T) {
	seen := make(chan Event[EventID], 1)
	m := New[StateID, EventID, Context]("params-internal").
		Initial("a").
		State("a", func(s *sb) {
			s.On("GO").RaiseWith("SET", func(Context) Params { return Params{"n": 7} }, Internal).GoTo("b")
		}).
		State("b", func(s *sb) {
			s.On("SET").
				GuardEvent(func(_ Context, e Event[EventID]) bool { return e.Params["n"].(int) > 100 }).
				GoTo("wrong")
			s.On("SET").
				GuardEvent(func(_ Context, e Event[EventID]) bool { return e.Params["n"].(int) > 5 }).
				AssignEvent(func(c Context, e Event[EventID]) Context {
					seen <- e
					c.Count = e.Params["n"].(int)
					return c
				}).
				GoTo("c")
		}).
		State("c", finalState).
		State("wrong", finalState).
		Build()

	actor := Start(m, Context{})
	defer actor.Stop()
	actor.Send("GO")
	awaitDone(t, actor)
	if actor.State() != "c" || actor.Data().Count != 7 {
		t.Fatalf("state=%s Count=%d, want c and 7", actor.State(), actor.Data().Count)
	}
	if e := <-seen; e.Name != "SET" || !e.Internal {
		t.Fatalf("event = %+v, want internal SET", e)
	}
}

func TestSendParams(t *testing.T) {
	m := New[StateID, EventID, Context]("send-params").
		Initial("a").
		State("a", func(s *sb) {
			s.On("SET").AssignEvent(func(c Context, e Event[EventID]) Context {
				if e.Internal {
					c.Count = -1
					return c
				}
				c.Count = e.Params["n"].(int)
				return c
			}).GoTo("b")
		}).
		State("b", finalState).
		Build()

	actor := Start(m, Context{})
	defer actor.Stop()
	if err := actor.SendCtxParams(context.Background(), "SET", Params{"n": 3}); err != nil {
		t.Fatal(err)
	}
	awaitDone(t, actor)
	if actor.Data().Count != 3 {
		t.Fatalf("Count = %d, want 3", actor.Data().Count)
	}
}

func TestAssignAndAssignEventCompose(t *testing.T) {
	m := New[StateID, EventID, Context]("assign-compose").
		Initial("a").
		State("a", func(s *sb) {
			s.On("GO").
				Assign(func(c Context) Context { c.Count++; return c }).
				AssignEvent(func(c Context, e Event[EventID]) Context { c.Count *= e.Params["n"].(int); return c }).
				Assign(func(c Context) Context { c.Count += 10; return c }).
				GoTo("b")
		}).
		State("b", finalState).
		Build()

	actor := Start(m, Context{})
	defer actor.Stop()
	actor.SendParams("GO", Params{"n": 3})
	awaitDone(t, actor)
	if got := actor.Data().Count; got != 13 {
		t.Fatalf("Count = %d, want ((0+1)*3)+10 = 13", got)
	}
}

func TestGuardAndGuardEventLastWins(t *testing.T) {
	m := New[StateID, EventID, Context]("guard-last-wins").
		Initial("a").
		State("a", func(s *sb) {
			s.On("GO").
				GuardEvent(func(Context, Event[EventID]) bool { return false }).
				Guard(func(Context) bool { return true }).
				GoTo("b")
		}).
		State("b", finalState).
		Build()

	actor := Start(m, Context{})
	defer actor.Stop()
	actor.Send("GO")
	awaitDone(t, actor)
}

func TestAlwaysSeesZeroEvent(t *testing.T) {
	seen := make(chan Event[EventID], 1)
	m := New[StateID, EventID, Context]("always-zero-event").
		Initial("a").
		State("a", func(s *sb) { s.On("GO").GoTo("b") }).
		State("b", func(s *sb) {
			s.Always().GuardEvent(func(_ Context, e Event[EventID]) bool {
				seen <- e
				return true
			}).GoTo("c")
		}).
		State("c", finalState).
		Build()

	actor := Start(m, Context{})
	defer actor.Stop()
	actor.SendParams("GO", Params{"k": 1})
	awaitDone(t, actor)
	if e := <-seen; e.Name != "" || e.Params != nil {
		t.Fatalf("Always saw %+v, want zero Event", e)
	}
}

// --- invoke ----------------------------------------------------------------

func TestInvokeHandleRaiseInternalWithoutHandler(t *testing.T) {
	result := make(chan bool, 1)
	m := New[StateID, EventID, Context]("handle-internal").
		Initial("working").
		State("working", func(s *sb) {
			s.InvokeWith(func(ctx context.Context, _ Context, h *invokeHandleT) error {
				result <- h.Raise("PROGRESS", Internal, Params{"pct": 50})
				<-ctx.Done()
				return nil
			}, "", "")
			s.On("PROGRESS").AssignEvent(func(c Context, e Event[EventID]) Context {
				c.Count = e.Params["pct"].(int)
				return c
			}).GoTo("done")
		}).
		State("done", finalState).
		Build()

	actor := Start(m, Context{})
	defer actor.Stop()
	if !<-result {
		t.Fatal("h.Raise internal = false, want true")
	}
	awaitDone(t, actor)
	if actor.Data().Count != 50 {
		t.Fatalf("Count = %d, want 50", actor.Data().Count)
	}
}

func TestInvokeHandleUpdateWithParams(t *testing.T) {
	m := New[StateID, EventID, Context]("handle-update-params").
		Initial("working").
		State("working", func(s *sb) {
			s.InvokeWith(func(ctx context.Context, _ Context, h *invokeHandleT) error {
				h.Update(func(c Context) Context { c.Count = 1; return c }, "SAVED", Params{"id": "x1"})
				<-ctx.Done()
				return nil
			}, "", "")
		}).
		Build()

	handler, ch := raiseRecorder()
	actor := Start(m, Context{}, m.WithRaiseHandler(handler))
	defer actor.Stop()
	if got := awaitRaised(t, ch); got.Event != "SAVED" || got.Params["id"] != "x1" {
		t.Fatalf("raised = %+v, want SAVED with id=x1", got)
	}
}

func TestContextRaiseInternal(t *testing.T) {
	m := New[StateID, EventID, Context]("ctx-internal").
		Initial("working").
		State("working", func(s *sb) {
			s.Invoke(func(ctx context.Context, _ Context, _ func(func(Context) Context)) error {
				Raise(ctx, "NEXT", Internal)
				<-ctx.Done()
				return nil
			}, "", "")
			s.On("NEXT").GoTo("done")
		}).
		State("done", finalState).
		Build()

	actor := Start(m, Context{})
	defer actor.Stop()
	awaitDone(t, actor)
}

func TestRaiseOnErrorInternalCarriesError(t *testing.T) {
	boom := errors.New("boom")
	got := make(chan error, 1)
	m := New[StateID, EventID, Context]("invoke-error-internal").
		Initial("working").
		State("working", func(s *sb) {
			s.Invoke(func(context.Context, Context, func(func(Context) Context)) error {
				return boom
			}, "", "").RaiseOnError("FAILED", Internal, Params{"retry": true})
			s.On("FAILED").AssignEvent(func(c Context, e Event[EventID]) Context {
				err, _ := e.Params[ParamError].(error)
				if e.Params["retry"] != true {
					err = errors.New("missing static param")
				}
				got <- err
				return c
			}).GoTo("failed")
		}).
		State("failed", finalState).
		Build()

	actor := Start(m, Context{})
	defer actor.Stop()
	select {
	case err := <-got:
		if !errors.Is(err, boom) {
			t.Fatalf("ParamError = %v, want boom", err)
		}
	case <-time.After(time.Second):
		t.Fatal("internal FAILED not processed")
	}
	awaitDone(t, actor)
}

// --- export -------------------------------------------------------------------

func TestRaiseKindsExport(t *testing.T) {
	m := New[StateID, EventID, Context]("raise-kinds-export").
		Initial("a").
		State("a", func(s *sb) {
			s.On("GO").
				Raise("NEXT", Internal).
				Raise("SET", Internal, Params{"n": 7}).
				Raise("OUT", Params{"name": "x"}).
				GoTo("b")
		}).
		State("b", func(*sb) {}).
		Build()

	doc, err := ToSCXML(m)
	if err != nil {
		t.Fatal(err)
	}
	out, err := xml.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`<raise event="NEXT"></raise>`,
		`<send event="SET" target="#_internal"><param name="n" expr="7"></param></send>`,
		`<send event="OUT"><param name="name" expr="&#39;x&#39;"></param></send>`,
	} {
		if !strings.Contains(string(out), want) {
			t.Errorf("SCXML missing %s:\n%s", want, out)
		}
	}
	mmd := ToMermaid(m)
	for _, want := range []string{"raise internal NEXT", "raise internal SET", "raise OUT"} {
		if !strings.Contains(mmd, want) {
			t.Errorf("Mermaid missing %q:\n%s", want, mmd)
		}
	}
}
