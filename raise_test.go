package gstate

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

type rtcState = string
type rtcEvent = string
type rtcBuilder = StateBuilder[rtcState, rtcEvent, Context]

// enteredSignal returns an observer that delivers the first entered state among
// states on the returned channel.
func enteredSignal(states ...rtcState) (Observer[rtcState, rtcEvent, Context], <-chan rtcState) {
	ch := make(chan rtcState, 1)
	obs := ObserverFuncs[rtcState, rtcEvent, Context]{
		StateEnteredFunc: func(_ context.Context, e *StateEvent[rtcState, rtcEvent, Context]) {
			if slices.Contains(states, e.State) {
				select {
				case ch <- e.State:
				default:
				}
			}
		},
	}
	return obs, ch
}

func waitState(t *testing.T, ch <-chan rtcState) rtcState {
	t.Helper()
	select {
	case s := <-ch:
		return s
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for state entry")
		return ""
	}
}

func TestRaiseRunsToCompletionBeforeNextExternalEvent(t *testing.T) {
	m := New[rtcState, rtcEvent, Context]("rtc").Initial("idle").
		State("idle", func(s *rtcBuilder) { s.On("GO").Raise("STEP").GoTo("s1") }).
		State("s1", func(s *rtcBuilder) {
			s.On("STEP").GoTo("s2")
			s.On("NEXT").GoTo("wrong")
		}).
		State("s2", func(s *rtcBuilder) { s.On("NEXT").GoTo("s3") }).
		State("s3", func(*rtcBuilder) {}).
		State("wrong", func(*rtcBuilder) {}).
		Build()
	obs, ch := enteredSignal("s3", "wrong")
	a := Start(m, Context{}, m.WithObservers(obs))
	defer a.Stop()
	a.Send("GO")
	a.Send("NEXT")
	if got := waitState(t, ch); got != "s3" {
		t.Fatalf("NEXT observed intermediate configuration: entered %q, want s3", got)
	}
}

func TestRaiseOrderExitTransitionEntry(t *testing.T) {
	m := New[rtcState, rtcEvent, Context]("order").Initial("a").
		State("a", func(s *rtcBuilder) {
			s.RaiseOnExit("X1")
			s.On("GO").Raise("X2", "X3").GoTo("b")
		}).
		State("b", func(s *rtcBuilder) {
			s.RaiseOnEntry("X4")
			for _, e := range []rtcEvent{"X1", "X2", "X3", "X4"} {
				s.On(e).Assign(func(c Context) Context { c.Count++; return c })
			}
			s.On("END").GoTo("end")
		}).
		State("end", func(*rtcBuilder) {}).
		Build()
	rec := &RecordingObserver[rtcState, rtcEvent, Context]{}
	obs, ch := enteredSignal("end")
	a := Start(m, Context{}, m.WithObservers(rec, obs))
	defer a.Stop()
	a.Send("GO")
	a.Send("END")
	waitState(t, ch)

	var got []string
	for _, e := range rec.EventsReceived() {
		tag := "ext"
		if e.Internal {
			tag = "int"
		}
		got = append(got, tag+":"+e.Event)
	}
	want := []string{"ext:GO", "int:X1", "int:X2", "int:X3", "int:X4", "ext:END"}
	if !slices.Equal(got, want) {
		t.Fatalf("received=%v, want %v", got, want)
	}
	if c := a.Data().Count; c != 4 {
		t.Fatalf("handled internal events=%d, want 4", c)
	}
}

func TestEventlessTransitionsPrecedeRaisedEvents(t *testing.T) {
	m := New[rtcState, rtcEvent, Context]("prio").Initial("a").
		State("a", func(s *rtcBuilder) { s.On("GO").Raise("INT").GoTo("b") }).
		State("b", func(s *rtcBuilder) {
			s.Always().GoTo("c")
			s.On("INT").GoTo("wrong")
		}).
		State("c", func(s *rtcBuilder) { s.On("INT").GoTo("d") }).
		State("d", func(*rtcBuilder) {}).
		State("wrong", func(*rtcBuilder) {}).
		Build()
	obs, ch := enteredSignal("d", "wrong")
	a := Start(m, Context{}, m.WithObservers(obs))
	defer a.Stop()
	a.Send("GO")
	if got := waitState(t, ch); got != "d" {
		t.Fatalf("entered %q, want d", got)
	}
}

func TestRaiseOnInitialEntrySettlesBeforeStartReturns(t *testing.T) {
	m := New[rtcState, rtcEvent, Context]("init").Initial("a").
		State("a", func(s *rtcBuilder) {
			s.RaiseOnEntry("READY")
			s.On("READY").GoTo("b")
		}).
		State("b", func(*rtcBuilder) {}).
		Build()
	a := Start(m, Context{})
	defer a.Stop()
	if got := a.State(); got != "b" {
		t.Fatalf("state after Start=%q, want b", got)
	}
}

func TestRaiseOnHydrateEntrySettlesBeforeHydrateReturns(t *testing.T) {
	m := New[rtcState, rtcEvent, Context]("hyd").Initial("a").
		State("a", func(s *rtcBuilder) {
			s.Always().Raise("PING").GoTo("b")
		}).
		State("b", func(s *rtcBuilder) { s.On("PING").GoTo("c") }).
		State("c", func(*rtcBuilder) {}).
		Build()
	a := Hydrate(m, Snapshot[rtcState, Context]{Active: []rtcState{"a"}})
	defer a.Stop()
	if got := a.State(); got != "c" {
		t.Fatalf("state after Hydrate=%q, want c", got)
	}
}

func TestUnhandledRaisedEventIsDropped(t *testing.T) {
	m := New[rtcState, rtcEvent, Context]("drop").Initial("a").
		State("a", func(s *rtcBuilder) { s.On("GO").Raise("NOBODY").GoTo("b") }).
		State("b", func(*rtcBuilder) {}).
		Build()
	bar := make(chan *EventNotice[rtcState, rtcEvent, Context], 1)
	obs := ObserverFuncs[rtcState, rtcEvent, Context]{
		EventDroppedFunc: func(_ context.Context, e *EventNotice[rtcState, rtcEvent, Context]) { bar <- e },
	}
	a := Start(m, Context{}, m.WithObservers(obs))
	defer a.Stop()
	a.Send("GO")
	select {
	case e := <-bar:
		if e.Event != "NOBODY" || !e.Internal || e.Reason != "no_transition" {
			t.Fatalf("drop notice=%+v", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no drop notice")
	}
}

func TestRaisedEventsUseTriggeringContext(t *testing.T) {
	type key struct{}
	m := New[rtcState, rtcEvent, Context]("ctx").Initial("a").
		State("a", func(s *rtcBuilder) { s.On("GO").Raise("INT").GoTo("b") }).
		State("b", func(s *rtcBuilder) { s.On("INT").GoTo("c") }).
		State("c", func(*rtcBuilder) {}).
		Build()
	got := make(chan any, 1)
	obs := ObserverFuncs[rtcState, rtcEvent, Context]{
		EventReceivedFunc: func(ctx context.Context, e *EventNotice[rtcState, rtcEvent, Context]) {
			if e.Internal {
				got <- ctx.Value(key{})
			}
		},
	}
	a := Start(m, Context{}, m.WithObservers(obs))
	defer a.Stop()
	if err := a.SendCtx(context.WithValue(context.Background(), key{}, "trace-1"), "GO"); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-got:
		if v != "trace-1" {
			t.Fatalf("ctx value=%v", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("internal event not received")
	}
}

func TestRaiseLoopExhaustsBudget(t *testing.T) {
	m := New[rtcState, rtcEvent, Context]("loop").Initial("a").
		State("a", func(s *rtcBuilder) {
			s.RaiseOnEntry("TICK")
			s.On("TICK").Assign(func(c Context) Context { c.Count++; return c }).Raise("TICK")
		}).
		Build()
	a := Start(m, Context{})
	defer a.Stop()
	if !errors.Is(a.Err(), ErrAlwaysLimit) {
		t.Fatalf("Err()=%v, want ErrAlwaysLimit", a.Err())
	}
	select {
	case <-a.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("actor did not stop")
	}
	if got := a.Data().Count; got != 100 {
		t.Fatalf("steps=%d, want 100", got)
	}
}

func TestRaisedEventsDiscardedWhenMachineHalts(t *testing.T) {
	m := New[rtcState, rtcEvent, Context]("halt").Initial("a").
		State("a", func(s *rtcBuilder) { s.On("GO").Raise("LATE").GoTo("done") }).
		State("done", func(s *rtcBuilder) { s.Type(Final) }).
		Build()
	rec := &RecordingObserver[rtcState, rtcEvent, Context]{}
	a := Start(m, Context{}, m.WithObservers(rec))
	a.Send("GO")
	<-a.Done()
	for _, e := range rec.EventsReceived() {
		if e.Event == "LATE" {
			t.Fatal("raised event processed after top-level final state")
		}
	}
	drops := rec.EventsDropped()
	if len(drops) != 1 || drops[0].Event != "LATE" || drops[0].Reason != "halted" || !drops[0].Internal {
		t.Fatalf("drops=%v", drops)
	}
}

// TestRaiseFromDelayedTransition uses a real After duration because the
// behaviour under test is a timer-fired transition; there is no channel-based
// substitute for the timer itself. Completion is still awaited via a signal.
func TestRaiseFromDelayedTransition(t *testing.T) {
	m := New[rtcState, rtcEvent, Context]("delay").Initial("a").
		State("a", func(s *rtcBuilder) { s.After(time.Millisecond).Raise("INT").GoTo("b") }).
		State("b", func(s *rtcBuilder) { s.On("INT").GoTo("c") }).
		State("c", func(*rtcBuilder) {}).
		Build()
	obs, ch := enteredSignal("c")
	a := Start(m, Context{}, m.WithObservers(obs))
	defer a.Stop()
	waitState(t, ch)
}

func TestRaiseOnlyTransitionCommitsSendWithPayload(t *testing.T) {
	// A transition that only raises still counts as fired, so the payload commits.
	m := New[rtcState, rtcEvent, Context]("payload").Initial("a").
		State("a", func(s *rtcBuilder) {
			s.On("GO").Raise("INT")
			s.On("INT").GoTo("b")
		}).
		State("b", func(*rtcBuilder) {}).
		Build()
	obs, ch := enteredSignal("b")
	a := Start(m, Context{}, m.WithObservers(obs))
	defer a.Stop()
	a.SendWith("GO", func(c Context) Context { c.Count = 7; return c })
	waitState(t, ch)
	if got := a.Data().Count; got != 7 {
		t.Fatalf("Count=%d, want 7", got)
	}
}

func TestSCXMLExportsRaise(t *testing.T) {
	m := New[rtcState, rtcEvent, Context]("scxml").Initial("a").
		State("a", func(s *rtcBuilder) {
			s.RaiseOnEntry("IN")
			s.RaiseOnExit("OUT")
			s.On("GO").Raise("X", "Y").GoTo("b")
		}).
		State("b", func(*rtcBuilder) {}).
		Build()
	raw, err := ToSCXMLString(m)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`<onentry><raise event="IN"></raise></onentry>`,
		`<onexit><raise event="OUT"></raise></onexit>`,
		`<transition event="GO" target="b"><raise event="X"></raise><raise event="Y"></raise></transition>`,
	} {
		if !strings.Contains(strings.Join(strings.Fields(raw), ""), strings.Join(strings.Fields(want), "")) {
			t.Fatalf("missing %s in\n%s", want, raw)
		}
	}
	doc := mustParseSCXML(t, raw)
	a := nodeByID(doc.Children, "a")
	if a == nil || a.OnEntry == nil || len(a.OnEntry.Raises) != 1 || a.OnEntry.Raises[0].Event != "IN" {
		t.Fatalf("onentry raise not round-tripped: %+v", a)
	}
	if a.OnExit == nil || len(a.OnExit.Raises) != 1 || a.OnExit.Raises[0].Event != "OUT" {
		t.Fatalf("onexit raise not round-tripped: %+v", a.OnExit)
	}
	if len(a.Transitions) != 1 || len(a.Transitions[0].Raises) != 2 {
		t.Fatalf("transition raise not round-tripped: %+v", a.Transitions)
	}
}
