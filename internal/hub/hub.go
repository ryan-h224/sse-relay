// Package hub holds the in-memory fan-out core of the relay: named streams that
// accept events from a single producer and deliver them to any number of
// subscribers, with a bounded replay buffer so a client that reconnects can pick
// up where it left off.
//
// Everything here is safe for concurrent use and knows nothing about HTTP.
package hub

import (
	"errors"
	"sort"
	"sync"
	"time"
)

// Default sizes used when a caller passes zero.
const (
	DefaultCapacity         = 512
	DefaultSubscriberBuffer = 64
)

var (
	// ErrStreamDone is returned when publishing to a stream that already ended.
	ErrStreamDone = errors.New("stream already finished")
	// ErrLagged is reported to a subscriber that could not keep up and was
	// dropped. The client should reconnect with Last-Event-ID.
	ErrLagged = errors.New("subscriber fell behind and was dropped")
	// ErrNotFound is returned when a stream id is unknown.
	ErrNotFound = errors.New("stream not found")
)

// Event is one item of a stream. IDs start at 1 and increase by one, which is
// what makes Last-Event-ID resumption exact.
type Event struct {
	ID   uint64
	Data string
}

// Stats is a snapshot of a stream, suitable for a status endpoint.
type Stats struct {
	ID          string    `json:"id"`
	Events      uint64    `json:"events"`
	Buffered    int       `json:"buffered"`
	Subscribers int       `json:"subscribers"`
	Done        bool      `json:"done"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// Subscription is one reader attached to a stream.
type Subscription struct {
	stream *Stream
	ch     chan Event
	once   sync.Once

	mu  sync.Mutex
	err error
}

// Events is the channel of events for this subscription. It is closed when the
// stream ends, when the subscriber is dropped for lagging, or after Close.
func (sub *Subscription) Events() <-chan Event {
	return sub.ch
}

// Err explains why the event channel was closed: nil for a normal end of stream
// or a local Close, ErrLagged when the subscriber could not keep up.
func (sub *Subscription) Err() error {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	return sub.err
}

// Close detaches the subscription. It is safe to call more than once and safe to
// call after the stream already ended.
func (sub *Subscription) Close() {
	sub.stream.detach(sub)
	sub.finish(nil)
}

func (sub *Subscription) finish(err error) {
	sub.once.Do(func() {
		sub.mu.Lock()
		sub.err = err
		sub.mu.Unlock()
		close(sub.ch)
	})
}

// Stream is a single named fan-out stream.
type Stream struct {
	ID string

	mu        sync.RWMutex
	buf       []Event
	capacity  int
	nextID    uint64
	done      bool
	subs      map[*Subscription]struct{}
	createdAt time.Time
	updatedAt time.Time
}

func newStream(id string, capacity int) *Stream {
	if capacity <= 0 {
		capacity = DefaultCapacity
	}
	now := time.Now()
	return &Stream{
		ID:        id,
		capacity:  capacity,
		subs:      make(map[*Subscription]struct{}),
		createdAt: now,
		updatedAt: now,
	}
}

// Publish appends data to the stream and hands it to every current subscriber.
// A subscriber whose buffer is full is dropped rather than blocking the
// producer: it can reconnect and replay from its last id.
func (s *Stream) Publish(data string) (Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.done {
		return Event{}, ErrStreamDone
	}

	s.nextID++
	ev := Event{ID: s.nextID, Data: data}
	s.buf = append(s.buf, ev)
	if len(s.buf) > s.capacity {
		s.buf = s.buf[len(s.buf)-s.capacity:]
	}
	s.updatedAt = time.Now()

	var lagging []*Subscription
	for sub := range s.subs {
		select {
		case sub.ch <- ev:
		default:
			lagging = append(lagging, sub)
		}
	}
	for _, sub := range lagging {
		delete(s.subs, sub)
		sub.finish(ErrLagged)
	}
	return ev, nil
}

// Subscribe attaches a reader. Buffered events newer than lastID are delivered
// immediately, so a client that reconnects with Last-Event-ID sees no gap. The
// boolean result reports that some events were already evicted from the replay
// buffer and are lost for good.
func (s *Stream) Subscribe(lastID uint64, buffer int) (*Subscription, bool) {
	if buffer <= 0 {
		buffer = DefaultSubscriberBuffer
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	gap := len(s.buf) > 0 && s.buf[0].ID > lastID+1
	replay := make([]Event, 0, len(s.buf))
	for _, ev := range s.buf {
		if ev.ID > lastID {
			replay = append(replay, ev)
		}
	}

	size := buffer
	if len(replay) > size {
		size = len(replay)
	}
	sub := &Subscription{stream: s, ch: make(chan Event, size)}
	for _, ev := range replay {
		sub.ch <- ev
	}

	if s.done {
		// Nothing more will ever arrive: hand over the history and close.
		sub.finish(nil)
		return sub, gap
	}
	s.subs[sub] = struct{}{}
	return sub, gap
}

// Finish marks the stream complete. Every attached subscriber drains what is
// already queued and then sees its channel closed.
func (s *Stream) Finish() {
	s.mu.Lock()
	if s.done {
		s.mu.Unlock()
		return
	}
	s.done = true
	s.updatedAt = time.Now()
	subs := make([]*Subscription, 0, len(s.subs))
	for sub := range s.subs {
		subs = append(subs, sub)
	}
	s.subs = make(map[*Subscription]struct{})
	s.mu.Unlock()

	for _, sub := range subs {
		sub.finish(nil)
	}
}

// Done reports whether the producer already finished the stream.
func (s *Stream) Done() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.done
}

// Stats returns a snapshot of the stream.
func (s *Stream) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Stats{
		ID:          s.ID,
		Events:      s.nextID,
		Buffered:    len(s.buf),
		Subscribers: len(s.subs),
		Done:        s.done,
		CreatedAt:   s.createdAt,
		UpdatedAt:   s.updatedAt,
	}
}

func (s *Stream) detach(sub *Subscription) {
	s.mu.Lock()
	delete(s.subs, sub)
	s.mu.Unlock()
}

// Hub is the registry of live streams.
type Hub struct {
	mu       sync.RWMutex
	streams  map[string]*Stream
	capacity int
}

// New returns a hub whose streams keep capacity events for replay.
func New(capacity int) *Hub {
	if capacity <= 0 {
		capacity = DefaultCapacity
	}
	return &Hub{
		streams:  make(map[string]*Stream),
		capacity: capacity,
	}
}

// Stream looks up an existing stream.
func (h *Hub) Stream(id string) (*Stream, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	s, ok := h.streams[id]
	return s, ok
}

// GetOrCreate returns the stream for id, creating it on first use.
func (h *Hub) GetOrCreate(id string) *Stream {
	h.mu.RLock()
	s, ok := h.streams[id]
	h.mu.RUnlock()
	if ok {
		return s
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if s, ok := h.streams[id]; ok {
		return s
	}
	s = newStream(id, h.capacity)
	h.streams[id] = s
	return s
}

// Remove finishes a stream and forgets it. Subscribers are released.
func (h *Hub) Remove(id string) bool {
	h.mu.Lock()
	s, ok := h.streams[id]
	delete(h.streams, id)
	h.mu.Unlock()
	if !ok {
		return false
	}
	s.Finish()
	return true
}

// IDs returns the ids of every live stream, sorted.
func (h *Hub) IDs() []string {
	h.mu.RLock()
	out := make([]string, 0, len(h.streams))
	for id := range h.streams {
		out = append(out, id)
	}
	h.mu.RUnlock()
	sort.Strings(out)
	return out
}

// CloseAll finishes every stream. Used on shutdown so that in-flight SSE
// responses terminate cleanly instead of being cut mid frame.
func (h *Hub) CloseAll() {
	h.mu.RLock()
	streams := make([]*Stream, 0, len(h.streams))
	for _, s := range h.streams {
		streams = append(streams, s)
	}
	h.mu.RUnlock()

	for _, s := range streams {
		s.Finish()
	}
}
