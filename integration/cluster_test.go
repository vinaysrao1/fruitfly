package integration_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vinaysrao1/fruitfly/cluster"
	"github.com/vinaysrao1/fruitfly/ingest"
	"github.com/vinaysrao1/fruitfly/types"
)

// twoNodeCluster wires two in-process "pods": real ingest handlers behind
// httptest servers, each with a router over the shared peer list.
func twoNodeCluster(t *testing.T) (urlA, urlB string, chA, chB chan types.Event) {
	t.Helper()
	chA = make(chan types.Event, 200)
	chB = make(chan types.Event, 200)

	srvA := ingest.NewServer(":0", maxEventBytes, chA)
	srvB := ingest.NewServer(":0", maxEventBytes, chB)

	httpA := httptest.NewServer(srvA.Handler())
	t.Cleanup(httpA.Close)
	httpB := httptest.NewServer(srvB.Handler())
	t.Cleanup(httpB.Close)

	peers := []string{httpA.URL, httpB.URL}
	routerA, err := cluster.NewRouter(httpA.URL, peers)
	if err != nil {
		t.Fatal(err)
	}
	routerB, err := cluster.NewRouter(httpB.URL, peers)
	if err != nil {
		t.Fatal(err)
	}
	srvA.SetRouter(routerA)
	srvB.SetRouter(routerB)

	return httpA.URL, httpB.URL, chA, chB
}

func drainEvents(ch chan types.Event) []types.Event {
	var out []types.Event
	for {
		select {
		case e := <-ch:
			out = append(out, e)
		default:
			return out
		}
	}
}

func clusterEventBody(entity string) string {
	return fmt.Sprintf(`{"event_type":"post","timestamp":"2024-01-15T10:30:00Z","entity_id":%q}`, entity)
}

// TestCluster_SingleEventsRouteToOwner: every event lands exactly once, on
// the pod that owns its routing entity, regardless of which pod received it.
func TestCluster_SingleEventsRouteToOwner(t *testing.T) {
	urlA, urlB, chA, chB := twoNodeCluster(t)
	ownerRouter, _ := cluster.NewRouter(urlA, []string{urlA, urlB})

	const n = 50
	for i := 0; i < n; i++ {
		// Alternate the receiving pod; ownership must not depend on it.
		target := urlA
		if i%2 == 1 {
			target = urlB
		}
		resp, err := http.Post(target+"/events", "application/json",
			strings.NewReader(clusterEventBody(fmt.Sprintf("user-%d", i))))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("event %d: status %d", i, resp.StatusCode)
		}
	}

	gotA := drainEvents(chA)
	gotB := drainEvents(chB)
	if len(gotA)+len(gotB) != n {
		t.Fatalf("processed %d+%d events, want exactly %d (no loss, no duplication)", len(gotA), len(gotB), n)
	}
	if len(gotA) == 0 || len(gotB) == 0 {
		t.Errorf("distribution: A=%d B=%d — both pods should own some entities", len(gotA), len(gotB))
	}
	for _, e := range gotA {
		if owner, _ := ownerRouter.Owner(e.EntityID); owner != urlA {
			t.Errorf("entity %s processed by A but owned by %s", e.EntityID, owner)
		}
	}
	for _, e := range gotB {
		if owner, _ := ownerRouter.Owner(e.EntityID); owner != urlB {
			t.Errorf("entity %s processed by B but owned by %s", e.EntityID, owner)
		}
	}
}

// TestCluster_BatchSplitsByOwner: a mixed-entity NDJSON batch posted to one
// pod is split, with remote entities forwarded as a sub-batch to their owner.
func TestCluster_BatchSplitsByOwner(t *testing.T) {
	urlA, urlB, chA, chB := twoNodeCluster(t)
	ownerRouter, _ := cluster.NewRouter(urlA, []string{urlA, urlB})

	const n = 40
	var lines []string
	for i := 0; i < n; i++ {
		lines = append(lines, clusterEventBody(fmt.Sprintf("batch-user-%d", i)))
	}
	resp, err := http.Post(urlA+"/events/batch", "application/json",
		strings.NewReader(strings.Join(lines, "\n")))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("batch status %d", resp.StatusCode)
	}

	gotA := drainEvents(chA)
	gotB := drainEvents(chB)
	if len(gotA)+len(gotB) != n {
		t.Fatalf("processed %d+%d, want %d", len(gotA), len(gotB), n)
	}
	for _, e := range append(gotA, gotB...) {
		owner, _ := ownerRouter.Owner(e.EntityID)
		processedBy := urlA
		for _, eb := range gotB {
			if eb.EventID == e.EventID {
				processedBy = urlB
				break
			}
		}
		if owner != processedBy {
			t.Errorf("entity %s: owner %s, processed by %s", e.EntityID, owner, processedBy)
		}
	}
}

// TestCluster_ForwardedEventsNeverReForward: a forwarded event is processed
// where it lands even if the receiver believes a peer owns it (one-hop
// bound; loops impossible during ring divergence).
func TestCluster_ForwardedEventsNeverReForward(t *testing.T) {
	urlA, urlB, chA, chB := twoNodeCluster(t)
	ownerRouter, _ := cluster.NewRouter(urlA, []string{urlA, urlB})

	// Find an entity owned by B, then post it to A with the forwarded
	// marker already set: A must process it locally, not bounce it to B.
	entity := ""
	for i := 0; ; i++ {
		candidate := fmt.Sprintf("hop-user-%d", i)
		if owner, _ := ownerRouter.Owner(candidate); owner == urlB {
			entity = candidate
			break
		}
	}

	req, _ := http.NewRequest(http.MethodPost, urlA+"/events",
		strings.NewReader(clusterEventBody(entity)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Fruitfly-Forwarded", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d", resp.StatusCode)
	}

	if got := len(drainEvents(chA)); got != 1 {
		t.Errorf("pod A processed %d events, want 1 (forwarded event processed locally)", got)
	}
	if got := len(drainEvents(chB)); got != 0 {
		t.Errorf("pod B processed %d events, want 0 (no re-forwarding)", got)
	}
}
