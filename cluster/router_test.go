package cluster

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRouter_OwnerDeterministicAndDistributed(t *testing.T) {
	peers := []string{"http://a", "http://b", "http://c"}
	r1, err := NewRouter("http://a", peers)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := NewRouter("http://b", peers)
	if err != nil {
		t.Fatal(err)
	}

	owned := map[string]int{}
	for i := 0; i < 3000; i++ {
		key := fmt.Sprintf("entity-%d", i)
		o1, _ := r1.Owner(key)
		o2, _ := r2.Owner(key)
		if o1 != o2 {
			t.Fatalf("peers disagree on owner of %q: %s vs %s", key, o1, o2)
		}
		owned[o1]++
	}
	for _, p := range peers {
		if owned[p] < 500 {
			t.Errorf("peer %s owns %d of 3000 keys — distribution too skewed", p, owned[p])
		}
	}
}

func TestRouter_SelfDetection(t *testing.T) {
	peers := []string{"http://a", "http://b"}
	r, err := NewRouter("http://a", peers)
	if err != nil {
		t.Fatal(err)
	}
	foundSelf := false
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("k-%d", i)
		peer, self := r.Owner(key)
		if self != (peer == "http://a") {
			t.Fatalf("self flag inconsistent for %q: peer=%s self=%v", key, peer, self)
		}
		if self {
			foundSelf = true
		}
	}
	if !foundSelf {
		t.Error("self never owns any key in 100 — suspicious")
	}
}

func TestNewRouter_Validation(t *testing.T) {
	if _, err := NewRouter("http://a", nil); err == nil {
		t.Error("empty peer list must be rejected")
	}
	if _, err := NewRouter("http://x", []string{"http://a"}); err == nil {
		t.Error("self not in peer list must be rejected")
	}
}

func TestRouter_ForwardSetsMarkerAndEndpoint(t *testing.T) {
	type seen struct {
		path      string
		forwarded string
		body      string
	}
	var got seen
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got = seen{path: r.URL.Path, forwarded: r.Header.Get("X-Fruitfly-Forwarded"), body: string(body)}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	r, err := NewRouter(srv.URL, []string{srv.URL})
	if err != nil {
		t.Fatal(err)
	}

	if err := r.Forward(context.Background(), srv.URL, []byte(`{"a":1}`), false); err != nil {
		t.Fatalf("Forward(single): %v", err)
	}
	if got.path != "/events" || got.forwarded != "1" || got.body != `{"a":1}` {
		t.Errorf("single forward = %+v, want /events with marker", got)
	}

	if err := r.Forward(context.Background(), srv.URL, []byte("{}\n{}"), true); err != nil {
		t.Fatalf("Forward(batch): %v", err)
	}
	if got.path != "/events/batch" || got.forwarded != "1" {
		t.Errorf("batch forward = %+v, want /events/batch with marker", got)
	}
}

func TestRouter_ForwardErrorOnPeerFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	r, err := NewRouter(srv.URL, []string{srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Forward(context.Background(), srv.URL, []byte("{}"), false); err == nil {
		t.Error("Forward must report peer 5xx as an error")
	}
}
