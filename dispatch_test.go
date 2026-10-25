package gstate

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func dispatchMachine() *Machine[StateID, EventID, Context] {
	return New[StateID, EventID, Context]("dispatch").
		Initial("waiting").
		State("waiting", func(s *StateBuilder[StateID, EventID, Context]) {
			s.After(10 * time.Millisecond).GoTo("timedOut")
			s.On("skip").GoTo("other")
		}).
		State("other", func(s *StateBuilder[StateID, EventID, Context]) {
			s.On("back").GoTo("waiting")
		}).
		State("timedOut", func(s *StateBuilder[StateID, EventID, Context]) {}).
		Build()
}

func recvTrigger(t *testing.T, ch <-chan InternalTrigger[StateID]) InternalTrigger[StateID] {
	t.Helper()
	select {
	case tr := <-ch:
		return tr
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for trigger")
		return InternalTrigger[StateID]{}
	}
}

func TestInternalDispatchReplicatesDelayedTransition(t *testing.T) {
	m := dispatchMachine()
	triggers := make(chan InternalTrigger[StateID], 1)
	leader := Start(m, Context{}, true, m.WithRunToCompletion(),
		m.WithInternalDispatch(func(tr InternalTrigger[StateID]) { triggers <- tr }))
	defer leader.Stop()
	follower := Start(m, Context{}, false, m.WithRunToCompletion())
	defer follower.Stop()

	tr := recvTrigger(t, triggers)
	want := InternalTrigger[StateID]{Kind: TriggerAfter, Source: "waiting", Target: "timedOut", Epoch: 1}
	if tr != want {
		t.Fatalf("trigger = %+v, want %+v", tr, want)
	}
	if got := leader.State(); got != "waiting" {
		t.Fatalf("leader transitioned locally to %s before FireInternal", got)
	}

	// Round-trip through JSON as a replicated log entry would.
	data, err := json.Marshal(tr)
	if err != nil {
		t.Fatal(err)
	}
	var applied InternalTrigger[StateID]
	if err := json.Unmarshal(data, &applied); err != nil {
		t.Fatal(err)
	}

	for name, a := range map[string]*Actor[StateID, EventID, Context]{"leader": leader, "follower": follower} {
		if !a.FireInternal(applied) {
			t.Fatalf("%s rejected current trigger", name)
		}
		if got := a.State(); got != "timedOut" {
			t.Fatalf("%s state = %s, want timedOut", name, got)
		}
		if a.FireInternal(applied) {
			t.Fatalf("%s accepted trigger twice", name)
		}
	}
}

func TestFireInternalRejectsTriggerFromEarlierEntry(t *testing.T) {
	m := dispatchMachine()
	a := Start(m, Context{}, false, m.WithRunToCompletion())
	defer a.Stop()

	stale := InternalTrigger[StateID]{Kind: TriggerAfter, Source: "waiting", Target: "timedOut", Epoch: 1}
	a.Send("skip")
	if a.FireInternal(stale) {
		t.Fatal("accepted trigger for inactive state")
	}
	a.Send("back")
	if a.FireInternal(stale) {
		t.Fatal("accepted trigger from previous entry after re-entry")
	}
	stale.Epoch = 2
	if !a.FireInternal(stale) || a.State() != "timedOut" {
		t.Fatalf("rejected current trigger, state = %s", a.State())
	}
}

func TestSnapshotPreservesEpochs(t *testing.T) {
	m := dispatchMachine()
	a := Start(m, Context{}, false, m.WithRunToCompletion())
	a.Send("skip")
	a.Send("back")
	data, err := json.Marshal(a.Snapshot())
	a.Stop()
	if err != nil {
		t.Fatal(err)
	}

	var snap Snapshot[StateID, Context]
	if err := json.Unmarshal(data, &snap); err != nil {
		t.Fatal(err)
	}
	h := Hydrate(m, snap, false, m.WithRunToCompletion())
	defer h.Stop()

	tr := InternalTrigger[StateID]{Kind: TriggerAfter, Source: "waiting", Target: "timedOut", Epoch: 1}
	if h.FireInternal(tr) {
		t.Fatal("hydrated actor accepted stale epoch")
	}
	tr.Epoch = 2
	if !h.FireInternal(tr) {
		t.Fatal("hydrated actor rejected current epoch")
	}
}

func TestInternalDispatchInvokeCompletion(t *testing.T) {
	m := New[StateID, EventID, Context]("invoke-dispatch").
		Initial("working").
		State("working", func(s *StateBuilder[StateID, EventID, Context]) {
			s.Invoke(func(ctx context.Context, _ Context, _ func(func(Context) Context)) error {
				return errors.New("boom")
			}, "done", "failed")
		}).
		State("done", func(s *StateBuilder[StateID, EventID, Context]) {}).
		State("failed", func(s *StateBuilder[StateID, EventID, Context]) {}).
		Build()

	triggers := make(chan InternalTrigger[StateID], 1)
	a := Start(m, Context{}, true, m.WithRunToCompletion(),
		m.WithInternalDispatch(func(tr InternalTrigger[StateID]) { triggers <- tr }))
	defer a.Stop()

	tr := recvTrigger(t, triggers)
	want := InternalTrigger[StateID]{Kind: TriggerInvokeError, Source: "working", Target: "failed", Epoch: 1}
	if tr != want {
		t.Fatalf("trigger = %+v, want %+v", tr, want)
	}
	if a.State() != "working" {
		t.Fatalf("transitioned locally to %s", a.State())
	}
	if !a.FireInternal(tr) || a.State() != "failed" {
		t.Fatalf("FireInternal failed, state = %s", a.State())
	}
}

func TestDeactivateStopsServicesAndActivateRestarts(t *testing.T) {
	m := dispatchMachine()
	triggers := make(chan InternalTrigger[StateID], 1)
	a := Start(m, Context{}, true, m.WithRunToCompletion(),
		m.WithInternalDispatch(func(tr InternalTrigger[StateID]) { triggers <- tr }))
	defer a.Stop()

	a.Deactivate()
	select {
	case tr := <-triggers:
		t.Fatalf("got trigger after Deactivate: %+v", tr)
	case <-time.After(50 * time.Millisecond):
	}

	a.Activate()
	if tr := recvTrigger(t, triggers); tr.Source != "waiting" || tr.Epoch != 1 {
		t.Fatalf("unexpected trigger after re-Activate: %+v", tr)
	}
}

func TestWithoutDispatchDelayedTransitionStillLocal(t *testing.T) {
	m := dispatchMachine()
	a := Start(m, Context{}, true)
	defer a.Stop()
	deadline := time.Now().Add(time.Second)
	for a.State() != "timedOut" {
		if time.Now().After(deadline) {
			t.Fatalf("state = %s, want timedOut", a.State())
		}
		time.Sleep(5 * time.Millisecond)
	}
}
