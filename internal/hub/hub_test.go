package hub

import (
	"errors"
	"testing"
	"time"
)

func TestStreamPublishAndSubscribeLive(t *testing.T) {
	s := newStream("s1", 4)

	sub, gap := s.Subscribe(0, 0)
	defer sub.Close()
	if gap {
		t.Fatal("expected no gap on a fresh stream")
	}

	ev, err := s.Publish("hello")
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if ev.ID != 1 {
		t.Fatalf("first event id = %d, want 1", ev.ID)
	}

	select {
	case got := <-sub.Events():
		if got.Data != "hello" || got.ID != 1 {
			t.Fatalf("got %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestSubscribeReplaysBufferedHistory(t *testing.T) {
	s := newStream("s1", 4)
	s.Publish("a")
	s.Publish("b")
	s.Publish("c")

	sub, gap := s.Subscribe(0, 0)
	defer sub.Close()
	if gap {
		t.Fatal("expected no gap: nothing was evicted")
	}

	for i, want := range []string{"a", "b", "c"} {
		select {
		case got := <-sub.Events():
			if got.Data != want || got.ID != uint64(i+1) {
				t.Fatalf("event %d = %+v, want data %q id %d", i, got, want, i+1)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for replayed event %d", i)
		}
	}
}

func TestSubscribeResumesAfterLastEventID(t *testing.T) {
	s := newStream("s1", 4)
	s.Publish("a")
	s.Publish("b")
	s.Publish("c")

	sub, gap := s.Subscribe(1, 0)
	defer sub.Close()
	if gap {
		t.Fatal("expected no gap: lastID is still within the buffer")
	}

	for i, want := range []string{"b", "c"} {
		select {
		case got := <-sub.Events():
			if got.Data != want {
				t.Fatalf("event %d = %+v, want data %q", i, got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for replayed event %d", i)
		}
	}
}

func TestSubscribeReportsGapAfterEviction(t *testing.T) {
	s := newStream("s1", 2)
	s.Publish("a")
	s.Publish("b")
	s.Publish("c") // evicts "a"

	// lastID 0 means the client never saw event 1, and it is now gone for good.
	sub, gap := s.Subscribe(0, 0)
	defer sub.Close()
	if !gap {
		t.Fatal("expected a gap: event 1 was evicted from the buffer")
	}

	// Whatever is still buffered should replay regardless of the gap.
	select {
	case got := <-sub.Events():
		if got.Data != "b" {
			t.Fatalf("got %+v, want data %q", got, "b")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for replayed event")
	}
}

func TestPublishDropsLaggingSubscriber(t *testing.T) {
	s := newStream("s1", 8)
	sub, _ := s.Subscribe(0, 1) // buffer of 1: the second publish overflows it

	if _, err := s.Publish("a"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if _, err := s.Publish("b"); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// "a" is already queued; the channel is now full and "b" cannot be
	// delivered, so the subscriber is dropped and its channel closed with
	// ErrLagged once it drains what was already queued.
	select {
	case ev, open := <-sub.Events():
		if !open {
			t.Fatal("channel closed before draining the queued event")
		}
		if ev.Data != "a" {
			t.Fatalf("got %+v, want queued event %q", ev, "a")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queued event")
	}

	select {
	case _, open := <-sub.Events():
		if open {
			t.Fatal("expected channel to be closed after lagging")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for channel close")
	}

	if !errors.Is(sub.Err(), ErrLagged) {
		t.Fatalf("Err() = %v, want ErrLagged", sub.Err())
	}
}

func TestPublishAfterFinishReturnsErrStreamDone(t *testing.T) {
	s := newStream("s1", 4)
	s.Finish()

	if _, err := s.Publish("late"); !errors.Is(err, ErrStreamDone) {
		t.Fatalf("Publish after Finish = %v, want ErrStreamDone", err)
	}
}

func TestFinishClosesLiveSubscribersWithoutError(t *testing.T) {
	s := newStream("s1", 4)
	sub, _ := s.Subscribe(0, 0)

	s.Finish()

	select {
	case _, open := <-sub.Events():
		if open {
			t.Fatal("expected channel to be closed after Finish")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for channel close")
	}
	if err := sub.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
	if !s.Done() {
		t.Fatal("Done() = false after Finish")
	}
}

func TestSubscribeToFinishedStreamStillReplaysAndClosesImmediately(t *testing.T) {
	s := newStream("s1", 4)
	s.Publish("a")
	s.Finish()

	sub, gap := s.Subscribe(0, 0)
	if gap {
		t.Fatal("expected no gap")
	}

	select {
	case ev, open := <-sub.Events():
		if !open || ev.Data != "a" {
			t.Fatalf("got %+v open=%v, want %q open=true", ev, open, "a")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for replayed event")
	}

	select {
	case _, open := <-sub.Events():
		if open {
			t.Fatal("expected channel to be closed for a finished stream")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for channel close")
	}
}

func TestSubscriptionCloseDetachesWithoutError(t *testing.T) {
	s := newStream("s1", 4)
	sub, _ := s.Subscribe(0, 0)
	sub.Close()

	if err := sub.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
	if s.Stats().Subscribers != 0 {
		t.Fatalf("Subscribers = %d, want 0 after Close", s.Stats().Subscribers)
	}

	// Closing twice must not panic or block.
	sub.Close()
}

func TestBufferEvictsOldestBeyondCapacity(t *testing.T) {
	s := newStream("s1", 2)
	s.Publish("a")
	s.Publish("b")
	s.Publish("c")

	stats := s.Stats()
	if stats.Buffered != 2 {
		t.Fatalf("Buffered = %d, want 2", stats.Buffered)
	}
	if stats.Events != 3 {
		t.Fatalf("Events = %d, want 3", stats.Events)
	}
}

func TestHubGetOrCreateReturnsSameStream(t *testing.T) {
	h := New(4)
	s1 := h.GetOrCreate("x")
	s2 := h.GetOrCreate("x")
	if s1 != s2 {
		t.Fatal("GetOrCreate returned different streams for the same id")
	}

	if _, ok := h.Stream("missing"); ok {
		t.Fatal("Stream found an id that was never created")
	}
}

func TestHubRemoveFinishesAndForgetsStream(t *testing.T) {
	h := New(4)
	s := h.GetOrCreate("x")
	sub, _ := s.Subscribe(0, 0)

	if !h.Remove("x") {
		t.Fatal("Remove returned false for a known stream")
	}
	if h.Remove("x") {
		t.Fatal("Remove returned true a second time")
	}

	select {
	case _, open := <-sub.Events():
		if open {
			t.Fatal("expected subscriber channel to close after Remove")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for channel close")
	}

	if _, ok := h.Stream("x"); ok {
		t.Fatal("stream still present after Remove")
	}
}

func TestHubIDsSorted(t *testing.T) {
	h := New(4)
	h.GetOrCreate("b")
	h.GetOrCreate("a")
	h.GetOrCreate("c")

	got := h.IDs()
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("IDs() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("IDs() = %v, want %v", got, want)
		}
	}
}

func TestHubCloseAllFinishesEveryStream(t *testing.T) {
	h := New(4)
	a := h.GetOrCreate("a")
	b := h.GetOrCreate("b")

	h.CloseAll()

	if !a.Done() || !b.Done() {
		t.Fatal("expected every stream to be Done after CloseAll")
	}
}
