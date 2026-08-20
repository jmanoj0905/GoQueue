package queue

import (
	"sync"
	"testing"
)

// hammer the queue with a bunch of goroutines at once, like what
// happens when multiple worker/client requests hit the broker at the
// same time. run this with -race.
func TestEnqueueConcurrent(t *testing.T) {
	q := New()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			q.Enqueue("payload", "normal")
		}()
	}
	wg.Wait()

	if len(q.normalJobs) != 50 {
		t.Fatalf("expected 50 jobs, got %d", len(q.normalJobs))
	}
}
