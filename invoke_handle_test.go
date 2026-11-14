package gstate

import (
	"context"
	"encoding/xml"
	"errors"
	"strings"
	"testing"
	"time"
)

type invokeHandleT = InvokeHandle[StateID, EventID, Context]

func awaitDone(t *testing.T, a *Actor[StateID, EventID, Context]) {
	t.Helper()
	select {
	case <-a.Done():
	case <-time.After(time.Second):
		t.Fatalf("actor did not stop; state=%s", a.State())
	}
}

// --- C: InvokeWith + InvokeHandle -----------------------------------------

func TestInvokeWithHandleRaise(t *testing.T) {
	results := make(chan bool, 1)
	m := New[StateID, EventID, Context]("handle-raise").
		Initial("working").
		State("working", func(s *StateBuilder[StateID, EventID, Context]) {
			s.InvokeWith(func(_ context.Context, _ Context, h *invokeHandleT) error {
				if h.State() != "working" {
					t.Errorf("h.State() = %s, want working", h.State())
				}
				ok := h.Raise("STEP_1")
				if h.Raise("") {
					t.Error("h.Raise(\"\") = true, want false")
				}
				results <- ok && h.Raise("STEP_2")
				return nil
			}, "done", "")
		}).
		State("done", func(s *StateBuilder[StateID, EventID, Context]) { s.Type(Final) }).
		Build()

	handler, ch := raiseRecorder()
	actor := Start(m, Context{}, m.WithRaiseHandler(handler))
	defer actor.Stop()

	for _, want := range []EventID{"STEP_1", "STEP_2"} {
		if got := awaitRaised(t, ch); got.Event != want || got.State != "working" {
			t.Fatalf("raised = %+v, want Event=%s State=working", got, want)
		}
	}
	if !<-results {
		t.Fatal("h.Raise = false, want true")
	}
	awaitDone(t, actor)
}

func TestInvokeWithHandleUpdateIsAtomic(t *testing.T) {
	results := make(chan bool, 1)
	m := New[StateID, EventID, Context]("handle-update").
		Initial("working").
		State("working", func(s *StateBuilder[StateID, EventID, Context]) {
			s.InvokeWith(func(ctx context.Context, _ Context, h *invokeHandleT) error {
				results <- h.Update(func(c Context) Context { c.Count = 42; return c }, "SAVED")
				<-ctx.Done()
				return nil
			}, "", "")
		}).
		Build()

	seen := make(chan int, 1)
	var actor *Actor[StateID, EventID, Context]
	ready := make(chan struct{})
	handler := func(_ context.Context, e RaisedEvent[StateID, EventID]) {
		<-ready
		if e.Event == "SAVED" {
			seen <- actor.Data().Count
		}
	}
	actor = Start(m, Context{}, m.WithRaiseHandler(handler))
	close(ready)
	defer actor.Stop()

	if got := <-seen; got != 42 {
		t.Fatalf("data seen by handler = %d, want 42 (event must follow committed data)", got)
	}
	if !<-results {
		t.Fatal("h.Update = false, want true")
	}
}

func TestInvokeWithHandleMutateAndStaleCalls(t *testing.T) {
	retained := make(chan *invokeHandleT, 1)
	mutated := make(chan bool, 1)
	m := New[StateID, EventID, Context]("handle-stale").
		Initial("working").
		State("working", func(s *StateBuilder[StateID, EventID, Context]) {
			s.InvokeWith(func(_ context.Context, _ Context, h *invokeHandleT) error {
				mutated <- h.Mutate(func(c Context) Context { c.Count++; return c })
				retained <- h
				return nil
			}, "idle", "")
		}).
		State("idle", func(s *StateBuilder[StateID, EventID, Context]) {
			s.On("PING").Raise("PONG")
		}).
		Build()

	handler, ch := raiseRecorder()
	actor := Start(m, Context{}, m.WithRaiseHandler(handler))
	defer actor.Stop()

	if !<-mutated {
		t.Fatal("h.Mutate = false, want true")
	}
	h := <-retained
	actor.Send("PING")
	if got := awaitRaised(t, ch); got.Event != "PONG" { // proves completion ran
		t.Fatalf("raised = %q, want PONG", got.Event)
	}
	if actor.Data().Count != 1 {
		t.Fatalf("Count = %d, want 1", actor.Data().Count)
	}
	if h.Mutate(func(c Context) Context { c.Count = 99; return c }) {
		t.Error("stale h.Mutate = true, want false")
	}
	if h.Update(func(c Context) Context { c.Count = 99; return c }, "LATE") {
		t.Error("stale h.Update = true, want false")
	}
	if h.Raise("LATE") {
		t.Error("stale h.Raise = true, want false")
	}
	if actor.Data().Count != 1 {
		t.Fatalf("Count = %d after stale calls, want 1", actor.Data().Count)
	}
	select {
	case got := <-ch:
		t.Fatalf("unexpected delivery after completion: %+v", got)
	default:
	}
}

func TestInvokeWithHandleNoRaiseHandler(t *testing.T) {
	type res struct{ raised, updated bool }
	results := make(chan res, 1)
	m := New[StateID, EventID, Context]("handle-nohandler").
		Initial("working").
		State("working", func(s *StateBuilder[StateID, EventID, Context]) {
			s.InvokeWith(func(_ context.Context, _ Context, h *invokeHandleT) error {
				r := res{raised: h.Raise("X")}
				r.updated = h.Update(func(c Context) Context { c.Count = 7; return c }, "Y")
				results <- r
				return nil
			}, "", "")
		}).
		Build()

	actor := Start(m, Context{})
	defer actor.Stop()
	r := <-results
	if r.raised {
		t.Error("h.Raise without handler = true, want false")
	}
	if !r.updated || actor.Data().Count != 7 {
		t.Errorf("h.Update without handler: committed=%v Count=%d, want true and 7", r.updated, actor.Data().Count)
	}
}

func TestInvokeWithSupportsContextRaise(t *testing.T) {
	m := New[StateID, EventID, Context]("handle-ctx-raise").
		Initial("working").
		State("working", func(s *StateBuilder[StateID, EventID, Context]) {
			s.InvokeWith(func(ctx context.Context, _ Context, _ *invokeHandleT) error {
				Raise(ctx, "VIA_CTX")
				<-ctx.Done()
				return nil
			}, "", "")
		}).
		Build()

	handler, ch := raiseRecorder()
	actor := Start(m, Context{}, m.WithRaiseHandler(handler))
	defer actor.Stop()
	if got := awaitRaised(t, ch); got.Event != "VIA_CTX" {
		t.Fatalf("raised = %q, want VIA_CTX", got.Event)
	}
}

func TestInvokeWithNilIsBuildError(t *testing.T) {
	b := New[StateID, EventID, Context]("handle-nil").
		Initial("a").
		State("a", func(s *StateBuilder[StateID, EventID, Context]) {
			s.InvokeWith(nil, "", "")
		})
	if err := b.Validate(); err == nil || !strings.Contains(err.Error(), "nil") {
		t.Fatalf("Validate() = %v, want nil-function error", err)
	}
}

func TestReplaceInvokeWith(t *testing.T) {
	m := New[StateID, EventID, Context]("handle-replace").
		Initial("working").
		State("working", func(s *StateBuilder[StateID, EventID, Context]) {
			s.Invoke(func(context.Context, Context, func(func(Context) Context)) error {
				return errors.New("original must not run")
			}, "done", "")
			s.InvokeLabel("svc")
			s.ReplaceInvokeWith("svc", func(_ context.Context, _ Context, h *invokeHandleT) error {
				h.Raise("REPLACED")
				return nil
			}, "done", "").RaiseOnDone("FINISHED")
		}).
		State("done", func(s *StateBuilder[StateID, EventID, Context]) { s.Type(Final) }).
		Build()

	handler, ch := raiseRecorder()
	actor := Start(m, Context{}, m.WithRaiseHandler(handler))
	defer actor.Stop()
	for _, want := range []EventID{"REPLACED", "FINISHED"} {
		if got := awaitRaised(t, ch); got.Event != want {
			t.Fatalf("raised = %q, want %q", got.Event, want)
		}
	}
	awaitDone(t, actor)
}

// --- D: declarative RaiseOnDone / RaiseOnError ----------------------------

func TestInvokeRaiseOnDone(t *testing.T) {
	m := New[StateID, EventID, Context]("raise-on-done").
		Initial("working").
		State("working", func(s *StateBuilder[StateID, EventID, Context]) {
			s.Invoke(func(context.Context, Context, func(func(Context) Context)) error {
				return nil
			}, "done", "failed").RaiseOnDone("FINISHED").RaiseOnDone("CLEANUP").RaiseOnError("FAILED")
		}).
		State("done", func(s *StateBuilder[StateID, EventID, Context]) { s.Type(Final) }).
		State("failed", func(s *StateBuilder[StateID, EventID, Context]) { s.Type(Final) }).
		Build()

	handler, ch := raiseRecorder()
	actor := Start(m, Context{}, m.WithRaiseHandler(handler))
	defer actor.Stop()
	for _, want := range []EventID{"FINISHED", "CLEANUP"} {
		got := awaitRaised(t, ch)
		if got.Event != want || got.State != "working" || got.Err != nil {
			t.Fatalf("raised = %+v, want Event=%s State=working Err=nil", got, want)
		}
	}
	awaitDone(t, actor)
	if actor.State() != "done" {
		t.Fatalf("state = %s, want done", actor.State())
	}
}

func TestInvokeRaiseOnErrorCarriesError(t *testing.T) {
	boom := errors.New("boom")
	m := New[StateID, EventID, Context]("raise-on-error").
		Initial("working").
		State("working", func(s *StateBuilder[StateID, EventID, Context]) {
			s.InvokeWith(func(context.Context, Context, *invokeHandleT) error {
				return boom
			}, "done", "failed").RaiseOnDone("FINISHED").RaiseOnError("FAILED")
		}).
		State("done", func(s *StateBuilder[StateID, EventID, Context]) { s.Type(Final) }).
		State("failed", func(s *StateBuilder[StateID, EventID, Context]) { s.Type(Final) }).
		Build()

	handler, ch := raiseRecorder()
	actor := Start(m, Context{}, m.WithRaiseHandler(handler))
	defer actor.Stop()
	got := awaitRaised(t, ch)
	if got.Event != "FAILED" || !errors.Is(got.Err, boom) {
		t.Fatalf("raised = %+v, want Event=FAILED Err=boom", got)
	}
	awaitDone(t, actor)
	if actor.State() != "failed" {
		t.Fatalf("state = %s, want failed", actor.State())
	}
}

func TestInvokeRaiseOnDoneWithoutTarget(t *testing.T) {
	m := New[StateID, EventID, Context]("raise-on-done-notarget").
		Initial("working").
		State("working", func(s *StateBuilder[StateID, EventID, Context]) {
			s.Invoke(func(context.Context, Context, func(func(Context) Context)) error {
				return nil
			}, "", "").RaiseOnDone("FINISHED")
		}).
		Build()

	handler, ch := raiseRecorder()
	actor := Start(m, Context{}, m.WithRaiseHandler(handler))
	defer actor.Stop()
	if got := awaitRaised(t, ch); got.Event != "FINISHED" {
		t.Fatalf("raised = %q, want FINISHED", got.Event)
	}
	if actor.State() != "working" {
		t.Fatalf("state = %s, want working", actor.State())
	}
}

func TestInvokeCancelledDoesNotRaiseOnDone(t *testing.T) {
	started := make(chan struct{})
	m := New[StateID, EventID, Context]("raise-on-done-cancel").
		Initial("working").
		State("working", func(s *StateBuilder[StateID, EventID, Context]) {
			s.Invoke(func(ctx context.Context, _ Context, _ func(func(Context) Context)) error {
				close(started)
				<-ctx.Done()
				return nil
			}, "", "").RaiseOnDone("FINISHED").RaiseOnError("FAILED")
			s.On("LEAVE").GoTo("other")
		}).
		State("other", func(*StateBuilder[StateID, EventID, Context]) {}).
		Build()

	handler, ch := raiseRecorder()
	actor := Start(m, Context{}, m.WithRaiseHandler(handler))
	<-started
	actor.Send("LEAVE")
	actor.Stop() // waits for the invoke goroutine, including its completion path
	select {
	case got := <-ch:
		t.Fatalf("cancelled invoke raised %+v", got)
	default:
	}
}

func TestInvokeBuilderDetachedOnDuplicate(t *testing.T) {
	m := New[StateID, EventID, Context]("raise-on-done-dup").
		Initial("a").
		State("a", func(s *StateBuilder[StateID, EventID, Context]) {
			fn := func(context.Context, Context, func(func(Context) Context)) error { return nil }
			s.Invoke(fn, "", "")
			s.Invoke(fn, "", "").RaiseOnDone("X") // rejected; must not touch the first
		})
	if err := m.Validate(); err == nil {
		t.Fatal("expected duplicate invoke error")
	}
	sd := m.machine.States["a"]
	if len(sd.Invoke.DoneRaise) != 0 {
		t.Fatalf("DoneRaise = %v, want empty", sd.Invoke.DoneRaise)
	}
}

func TestInvokeRaiseOnDoneExports(t *testing.T) {
	m := New[StateID, EventID, Context]("raise-on-done-export").
		Initial("working").
		State("working", func(s *StateBuilder[StateID, EventID, Context]) {
			s.Invoke(func(context.Context, Context, func(func(Context) Context)) error {
				return nil
			}, "done", "failed").RaiseOnDone("FINISHED").RaiseOnError("FAILED")
		}).
		State("done", func(*StateBuilder[StateID, EventID, Context]) {}).
		State("failed", func(*StateBuilder[StateID, EventID, Context]) {}).
		Build()

	doc, err := ToSCXML(m)
	if err != nil {
		t.Fatal(err)
	}
	out, err := xml.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`<send event="FINISHED"></send>`, `<send event="FAILED"></send>`} {
		if !strings.Contains(string(out), want) {
			t.Errorf("SCXML missing %s: %s", want, out)
		}
	}
	mmd := ToMermaid(m)
	for _, want := range []string{"raise FINISHED", "raise FAILED"} {
		if !strings.Contains(mmd, want) {
			t.Errorf("Mermaid missing %q:\n%s", want, mmd)
		}
	}
}
