// Package relay puts an HTTP surface on top of the hub: a publish endpoint for
// the producer and a Server-Sent Events endpoint for the browsers.
package relay

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ryan-h224/sse-relay/internal/hub"
)

// maxChunkBytes caps a single published chunk. Token deltas are tiny; anything
// larger is a client bug.
const maxChunkBytes = 1 << 20

// Config tunes the HTTP layer.
type Config struct {
	// Heartbeat is the delay between `: ping` comment frames, which keep
	// proxies from closing an idle connection. Defaults to 15s.
	Heartbeat time.Duration
	// RetryHint is the reconnect delay advertised to clients. Defaults to 2s.
	RetryHint time.Duration
	// Token, when set, is required as a bearer token on every write endpoint.
	Token string
}

// Server is the HTTP handler of the relay.
type Server struct {
	hub *hub.Hub
	cfg Config
	mux *http.ServeMux
}

// NewServer wires the routes on top of h.
func NewServer(h *hub.Hub, cfg Config) *Server {
	if cfg.Heartbeat <= 0 {
		cfg.Heartbeat = 15 * time.Second
	}
	if cfg.RetryHint <= 0 {
		cfg.RetryHint = 2 * time.Second
	}
	s := &Server{hub: h, cfg: cfg, mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /streams/{id}", s.handlePublish)
	s.mux.HandleFunc("POST /streams/{id}/done", s.handleFinish)
	s.mux.HandleFunc("DELETE /streams/{id}", s.handleDelete)
	s.mux.HandleFunc("GET /streams/{id}/events", s.handleEvents)
	s.mux.HandleFunc("GET /streams/{id}", s.handleStats)
	s.mux.HandleFunc("GET /streams", s.handleList)
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

type publishRequest struct {
	Data string `json:"data"`
	Done bool   `json:"done"`
}

// handlePublish accepts one chunk from the upstream producer. The body is either
// the raw chunk or, with an application/json content type, an object with
// `data` and `done` fields.
func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeError(w, http.StatusUnauthorized, "missing or invalid bearer token")
		return
	}
	id := r.PathValue("id")
	if !validStreamID(id) {
		writeError(w, http.StatusBadRequest, "stream id must be 1 to 128 characters of [A-Za-z0-9._-]")
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxChunkBytes))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "chunk is larger than 1MiB")
		return
	}

	req := publishRequest{Data: string(body)}
	if strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		req = publishRequest{}
		if err := json.Unmarshal(body, &req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json body: "+err.Error())
			return
		}
	}
	if req.Data == "" && !req.Done {
		writeError(w, http.StatusBadRequest, "empty chunk")
		return
	}

	stream := s.hub.GetOrCreate(id)
	var ev hub.Event
	if req.Data != "" {
		ev, err = stream.Publish(req.Data)
		if err != nil {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
	}
	if req.Done {
		stream.Finish()
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"id":       id,
		"event_id": ev.ID,
		"done":     req.Done,
	})
}

// handleFinish ends a stream without publishing anything else.
func (s *Server) handleFinish(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeError(w, http.StatusUnauthorized, "missing or invalid bearer token")
		return
	}
	stream, ok := s.hub.Stream(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, hub.ErrNotFound.Error())
		return
	}
	stream.Finish()
	writeJSON(w, http.StatusOK, stream.Stats())
}

// handleDelete drops a stream and its replay buffer.
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeError(w, http.StatusUnauthorized, "missing or invalid bearer token")
		return
	}
	if !s.hub.Remove(r.PathValue("id")) {
		writeError(w, http.StatusNotFound, hub.ErrNotFound.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleEvents is the SSE endpoint. It replays the buffered history, then
// follows the stream live until the client goes away or the producer finishes.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	stream, ok := s.hub.Stream(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, hub.ErrNotFound.Error())
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming is not supported by this server")
		return
	}

	lastID := lastEventID(r)
	sub, gap := stream.Subscribe(lastID, 0)
	defer sub.Close()

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache, no-transform")
	h.Set("Connection", "keep-alive")
	// Tell nginx and friends not to buffer the response.
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	fmt.Fprintf(w, "retry: %d\n\n", s.cfg.RetryHint.Milliseconds())
	if gap {
		fmt.Fprintf(w, "event: gap\ndata: {\"after\":%d}\n\n", lastID)
	}
	flusher.Flush()

	ticker := time.NewTicker(s.cfg.Heartbeat)
	defer ticker.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := io.WriteString(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case ev, open := <-sub.Events():
			if !open {
				if errors.Is(sub.Err(), hub.ErrLagged) {
					io.WriteString(w, "event: lagged\ndata: {}\n\n")
				} else {
					io.WriteString(w, "event: done\ndata: {}\n\n")
				}
				flusher.Flush()
				return
			}
			writeEvent(w, ev)
			flusher.Flush()
		}
	}
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stream, ok := s.hub.Stream(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, hub.ErrNotFound.Error())
		return
	}
	writeJSON(w, http.StatusOK, stream.Stats())
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	ids := s.hub.IDs()
	out := make([]hub.Stats, 0, len(ids))
	for _, id := range ids {
		if stream, ok := s.hub.Stream(id); ok {
			out = append(out, stream.Stats())
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"streams": out})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"streams": len(s.hub.IDs()),
	})
}

func (s *Server) authorized(r *http.Request) bool {
	if s.cfg.Token == "" {
		return true
	}
	got := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer"))
	return subtle.ConstantTimeCompare([]byte(got), []byte(s.cfg.Token)) == 1
}

// writeEvent serialises one event as an SSE frame. Multi-line payloads are split
// across several `data:` lines, which is what the protocol requires.
func writeEvent(w io.Writer, ev hub.Event) {
	fmt.Fprintf(w, "id: %d\n", ev.ID)
	data := strings.ReplaceAll(ev.Data, "\r\n", "\n")
	for _, line := range strings.Split(data, "\n") {
		fmt.Fprintf(w, "data: %s\n", line)
	}
	io.WriteString(w, "\n")
}

// lastEventID reads the resume point from the header the browser sends on
// reconnect, falling back to a query parameter for clients such as curl.
func lastEventID(r *http.Request) uint64 {
	raw := r.Header.Get("Last-Event-ID")
	if raw == "" {
		raw = r.URL.Query().Get("last_event_id")
	}
	if raw == "" {
		return 0
	}
	n, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func validStreamID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
			// allowed
		case c == '.', c == '_', c == '-':
			// allowed
		default:
			return false
		}
	}
	return true
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, map[string]string{"error": message})
}
