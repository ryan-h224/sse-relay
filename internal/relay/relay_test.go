package relay

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ryan-h224/sse-relay/internal/hub"
)

func newTestServer(t *testing.T, capacity int, token string) (*httptest.Server, *http.Client) {
	t.Helper()
	h := hub.New(capacity)
	srv := httptest.NewServer(NewServer(h, Config{Token: token}))
	t.Cleanup(srv.Close)
	return srv, &http.Client{Timeout: 5 * time.Second}
}

func publish(t *testing.T, client *http.Client, url, token, body, contentType string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	return resp
}

func TestPublishRequiresTokenWhenConfigured(t *testing.T) {
	srv, client := newTestServer(t, 4, "secret")
	url := srv.URL + "/streams/s1"

	resp := publish(t, client, url, "", "hello", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}

	resp2 := publish(t, client, url, "secret", "hello", "")
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp2.StatusCode, http.StatusAccepted)
	}
}

func TestPublishPlainBodyAppendsOneEvent(t *testing.T) {
	srv, client := newTestServer(t, 4, "")

	resp := publish(t, client, srv.URL+"/streams/s1", "", "hello", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
	var got map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["event_id"] != float64(1) {
		t.Fatalf("event_id = %v, want 1", got["event_id"])
	}

	stats := fetchStats(t, client, srv.URL+"/streams/s1")
	if stats.Events != 1 || stats.Done {
		t.Fatalf("stats = %+v, want one event and not done", stats)
	}
}

func TestPublishJSONBodyCanFinishInOneCall(t *testing.T) {
	srv, client := newTestServer(t, 4, "")

	resp := publish(t, client, srv.URL+"/streams/s1", "", `{"data":"bye","done":true}`, "application/json")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}

	stats := fetchStats(t, client, srv.URL+"/streams/s1")
	if !stats.Done {
		t.Fatal("expected stream to be done")
	}
}

func TestPublishEmptyChunkIsBadRequest(t *testing.T) {
	srv, client := newTestServer(t, 4, "")

	resp := publish(t, client, srv.URL+"/streams/s1", "", "", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestPublishInvalidJSONIsBadRequest(t *testing.T) {
	srv, client := newTestServer(t, 4, "")

	resp := publish(t, client, srv.URL+"/streams/s1", "", "{not json", "application/json")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestPublishInvalidStreamIDIsBadRequest(t *testing.T) {
	srv, client := newTestServer(t, 4, "")

	resp := publish(t, client, srv.URL+"/streams/bad!id", "", "hello", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestPublishOversizeChunkIsRejected(t *testing.T) {
	srv, client := newTestServer(t, 4, "")

	body := bytes.Repeat([]byte("a"), maxChunkBytes+1)
	resp := publish(t, client, srv.URL+"/streams/s1", "", string(body), "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusRequestEntityTooLarge)
	}
}

func TestHandleFinishEndsAnUnknownStreamIs404(t *testing.T) {
	srv, client := newTestServer(t, 4, "")

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/streams/missing/done", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestHandleFinishEndsAnExistingStream(t *testing.T) {
	srv, client := newTestServer(t, 4, "")
	publish(t, client, srv.URL+"/streams/s1", "", "hello", "").Body.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/streams/s1/done", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	stats := fetchStats(t, client, srv.URL+"/streams/s1")
	if !stats.Done {
		t.Fatal("expected stream to be done")
	}
}

func TestHandleDeleteRemovesTheStream(t *testing.T) {
	srv, client := newTestServer(t, 4, "")
	publish(t, client, srv.URL+"/streams/s1", "", "hello", "").Body.Close()

	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/streams/s1", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	req2, _ := http.NewRequest(http.MethodDelete, srv.URL+"/streams/s1", nil)
	resp2, err := client.Do(req2)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Fatalf("second delete status = %d, want %d", resp2.StatusCode, http.StatusNotFound)
	}
}

func TestHandleEventsReplaysThenSendsDone(t *testing.T) {
	srv, client := newTestServer(t, 8, "")
	publish(t, client, srv.URL+"/streams/s1", "", "Hello", "").Body.Close()
	publish(t, client, srv.URL+"/streams/s1", "", " world", "").Body.Close()
	publish(t, client, srv.URL+"/streams/s1", "", "!", "").Body.Close()
	resp := publish(t, client, srv.URL+"/streams/s1/done", "", "", "")
	resp.Body.Close()

	resp, err := client.Get(srv.URL + "/streams/s1/events")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}
	body := readAll(t, resp)

	want := "retry: 2000\n\nid: 1\ndata: Hello\n\nid: 2\ndata:  world\n\nid: 3\ndata: !\n\nevent: done\ndata: {}\n\n"
	if body != want {
		t.Fatalf("body = %q, want %q", body, want)
	}
}

func TestHandleEventsResumesFromLastEventIDHeader(t *testing.T) {
	srv, client := newTestServer(t, 8, "")
	publish(t, client, srv.URL+"/streams/s1", "", "a", "").Body.Close()
	publish(t, client, srv.URL+"/streams/s1", "", "b", "").Body.Close()
	publish(t, client, srv.URL+"/streams/s1", "", "c", "").Body.Close()
	publish(t, client, srv.URL+"/streams/s1/done", "", "", "").Body.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/streams/s1/events", nil)
	req.Header.Set("Last-Event-ID", "1")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	body := readAll(t, resp)
	want := "retry: 2000\n\nid: 2\ndata: b\n\nid: 3\ndata: c\n\nevent: done\ndata: {}\n\n"
	if body != want {
		t.Fatalf("body = %q, want %q", body, want)
	}
}

func TestHandleEventsReportsGapAfterEviction(t *testing.T) {
	srv, client := newTestServer(t, 1, "")
	publish(t, client, srv.URL+"/streams/s1", "", "a", "").Body.Close()
	publish(t, client, srv.URL+"/streams/s1", "", "b", "").Body.Close() // evicts "a"
	publish(t, client, srv.URL+"/streams/s1/done", "", "", "").Body.Close()

	resp, err := client.Get(srv.URL + "/streams/s1/events?last_event_id=0")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()

	body := readAll(t, resp)
	want := "retry: 2000\n\nevent: gap\ndata: {\"after\":0}\n\nid: 2\ndata: b\n\nevent: done\ndata: {}\n\n"
	if body != want {
		t.Fatalf("body = %q, want %q", body, want)
	}
}

func TestHandleEventsUnknownStreamIs404(t *testing.T) {
	srv, client := newTestServer(t, 4, "")

	resp, err := client.Get(srv.URL + "/streams/missing/events")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestHandleStatsUnknownStreamIs404(t *testing.T) {
	srv, client := newTestServer(t, 4, "")

	resp, err := client.Get(srv.URL + "/streams/missing")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestHandleListReturnsEveryStream(t *testing.T) {
	srv, client := newTestServer(t, 4, "")
	publish(t, client, srv.URL+"/streams/a", "", "x", "").Body.Close()
	publish(t, client, srv.URL+"/streams/b", "", "y", "").Body.Close()

	resp, err := client.Get(srv.URL + "/streams")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()

	var got struct {
		Streams []hub.Stats `json:"streams"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Streams) != 2 {
		t.Fatalf("streams = %d, want 2", len(got.Streams))
	}
}

func TestHandleHealthReportsStreamCount(t *testing.T) {
	srv, client := newTestServer(t, 4, "")
	publish(t, client, srv.URL+"/streams/a", "", "x", "").Body.Close()

	resp, err := client.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()

	var got map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["status"] != "ok" || got["streams"] != float64(1) {
		t.Fatalf("got %+v", got)
	}
}

func TestValidStreamID(t *testing.T) {
	cases := map[string]bool{
		"chat-42":     true,
		"a.b_c-9":     true,
		"":            false,
		"has space":   false,
		"slash/here":  false,
		strings.Repeat("a", 129): false,
		strings.Repeat("a", 128): true,
	}
	for id, want := range cases {
		if got := validStreamID(id); got != want {
			t.Errorf("validStreamID(%q) = %v, want %v", id, got, want)
		}
	}
}

func fetchStats(t *testing.T, client *http.Client, url string) hub.Stats {
	t.Helper()
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	var stats hub.Stats
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return stats
}

func readAll(t *testing.T, resp *http.Response) string {
	t.Helper()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	return buf.String()
}
