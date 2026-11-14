package gstate

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

// internalRoutingMachine: GO raises internal NEXT (with params) and moves to b;
// NEXT in b records whether it arrived as internal, then finishes.
func internalRoutingMachine() *Machine[StateID, EventID, Context] {
	return New[StateID, EventID, Context]("internal-routing").
		Initial("a").
		State("a", func(s *sb) {
			s.On("GO").Raise("NEXT", Internal, Params{"n": 7}).GoTo("b")
		}).
		State("b", func(s *sb) {
			s.On("NEXT").AssignEvent(func(c Context, e Event[EventID]) Context {
				if e.Internal {
					c.Count = e.Params["n"].(int)
				}
				return c
			}).GoTo("c")
		}).
		State("c", finalState).
		Build()
}

func TestInternalRaiseIsAlsoDeliveredToHandler(t *testing.T) {
	m := internalRoutingMachine()
	handler, ch := raiseRecorder()
	actor := Start(m, Context{}, m.WithRaiseHandler(handler))
	defer actor.Stop()

	actor.Send("GO")
	got := awaitRaised(t, ch)
	if got.Event != "NEXT" || !got.Internal || got.State != "a" || got.Params["n"] != 7 {
		t.Fatalf("raised = %+v, want internal NEXT from a with n=7", got)
	}
	awaitDone(t, actor) // processed locally as well
	if actor.Data().Count != 7 {
		t.Fatalf("Count = %d, want 7", actor.Data().Count)
	}
}

func TestInternalViaHandlerSkipsLocalProcessing(t *testing.T) {
	m := internalRoutingMachine()
	type seen struct {
		event RaisedEvent[StateID, EventID]
		state StateID
	}
	ch := make(chan seen, 1)
	var actor *Actor[StateID, EventID, Context]
	ready := make(chan struct{})
	handler := func(_ context.Context, e RaisedEvent[StateID, EventID]) {
		<-ready
		ch <- seen{e, actor.State()}
	}
	actor = Start(m, Context{}, m.WithRaiseHandler(handler), m.WithInternalViaHandler(true))
	close(ready)
	defer actor.Stop()

	actor.Send("GO")
	var got seen
	select {
	case got = <-ch:
	case <-time.After(time.Second):
		t.Fatal("handler did not receive internal event")
	}
	if !got.event.Internal || got.event.Event != "NEXT" {
		t.Fatalf("raised = %+v, want internal NEXT", got.event)
	}
	if got.state != "b" {
		t.Fatalf("state when handler ran = %s, want b (not processed locally)", got.state)
	}

	// The handler's owner routes it back; it keeps its internal flag and params.
	if err := actor.SendCtxEvent(context.Background(), got.event.AsEvent()); err != nil {
		t.Fatal(err)
	}
	awaitDone(t, actor)
	if actor.Data().Count != 7 {
		t.Fatalf("Count = %d, want 7 (routed event must arrive as internal with params)", actor.Data().Count)
	}
}

func TestInternalViaHandlerWithoutHandlerProcessesLocally(t *testing.T) {
	m := internalRoutingMachine()
	actor := Start(m, Context{}, m.WithInternalViaHandler(true))
	defer actor.Stop()
	actor.Send("GO")
	awaitDone(t, actor)
	if actor.Data().Count != 7 {
		t.Fatalf("Count = %d, want 7", actor.Data().Count)
	}
}

func TestSendEventCopiesParams(t *testing.T) {
	m := New[StateID, EventID, Context]("send-event").
		Initial("a").
		State("a", func(s *sb) {
			s.On("SET").AssignEvent(func(c Context, e Event[EventID]) Context {
				c.Count = e.Params["n"].(int)
				return c
			}).GoTo("b")
		}).
		State("b", finalState).
		Build()
	actor := Start(m, Context{})
	defer actor.Stop()
	params := Params{"n": 4}
	actor.SendEvent(Event[EventID]{Name: "SET", Params: params})
	params["n"] = 99
	awaitDone(t, actor)
	if actor.Data().Count != 4 {
		t.Fatalf("Count = %d, want 4", actor.Data().Count)
	}
}

// TestReplicatedLogKeepsReplicasInSync models raft: every input goes through
// an ordered log applied to all replicas; only the leader's handler appends
// (raft.Apply on a follower fails with ErrNotLeader).
func TestReplicatedLogKeepsReplicasInSync(t *testing.T) {
	m := New[StateID, EventID, Context]("replicated").
		Initial("idle").
		State("idle", func(s *sb) {
			s.On("START").
				Assign(func(c Context) Context { c.Count++; return c }).
				Raise("STEP", Internal, Params{"inc": 10}).
				GoTo("running")
		}).
		State("running", func(s *sb) {
			s.On("STEP").AssignEvent(func(c Context, e Event[EventID]) Context {
				c.Count += e.Params["inc"].(int)
				return c
			}).Raise("FINISH", Internal).GoTo("finishing")
		}).
		State("finishing", func(s *sb) { s.On("FINISH").GoTo("done") }).
		State("done", finalState).
		Build()

	var mu sync.Mutex
	var log []Event[EventID]
	logged := make(chan struct{}, 16)
	appendLog := func(e Event[EventID]) {
		mu.Lock()
		log = append(log, e)
		mu.Unlock()
		logged <- struct{}{}
	}
	handlerFor := func(leader bool) func(context.Context, RaisedEvent[StateID, EventID]) {
		return func(_ context.Context, e RaisedEvent[StateID, EventID]) {
			if leader && e.Internal {
				appendLog(e.AsEvent()) // raft.Apply
			}
		}
	}
	leader := Start(m, Context{}, m.WithRaiseHandler(handlerFor(true)), m.WithInternalViaHandler(true), m.WithActorID("leader"))
	follower := Start(m, Context{}, m.WithRaiseHandler(handlerFor(false)), m.WithInternalViaHandler(true), m.WithActorID("follower"))
	defer leader.Stop()
	defer follower.Stop()
	replicas := []*Actor[StateID, EventID, Context]{leader, follower}

	appendLog(Event[EventID]{Name: "START"}) // client command, also via the log
	// FSM.Apply: deliver every committed entry to every replica, in log order.
	for applied := 0; applied < 3; applied++ {
		select {
		case <-logged:
		case <-time.After(time.Second):
			t.Fatalf("log stalled after %d entries", applied)
		}
		mu.Lock()
		entry := log[applied]
		mu.Unlock()
		for _, r := range replicas {
			if err := r.SendCtxEvent(context.Background(), entry); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, r := range replicas {
		awaitDone(t, r)
		if r.State() != "done" || r.Data().Count != 11 {
			t.Fatalf("%s: state=%s Count=%d, want done and 11", r.ID(), r.State(), r.Data().Count)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(log) != 3 {
		t.Fatalf("log = %v, want START, STEP, FINISH exactly once", log)
	}
}

func TestEventJSONRoundTrip(t *testing.T) {
	in := Event[EventID]{Name: "SET", Params: Params{"n": 1.5}, Internal: true}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"name":"SET","params":{"n":1.5},"internal":true}`; string(data) != want {
		t.Fatalf("json = %s, want %s", data, want)
	}
	var out Event[EventID]
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if out.Name != in.Name || !out.Internal || out.Params["n"] != 1.5 {
		t.Fatalf("round trip = %+v, want %+v", out, in)
	}
}
