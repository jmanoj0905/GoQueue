package queue

import (
	"fmt"
	"sync"
	"time"

	"goqueue/internal/job"
)

// Queue is just an in-memory FIFO queue for now. No persistence yet,
// if the broker restarts everything in here is gone. Adding a
// write-ahead log for that later.
//
// broker handlers run one goroutine per request, so this thing gets
// hit from multiple goroutines at once. everything below needs the
// mutex held (found this out the hard way with -race, see
// queue_test.go).
type Queue struct {
	mu       sync.Mutex
	jobs     []*job.Job
	inFlight map[string]*job.Job
	nextID   int
}

func New() *Queue {
	return &Queue{
		jobs:     make([]*job.Job, 0),
		inFlight: make(map[string]*job.Job),
		nextID:   1,
	}
}

// Enqueue adds a new job with the given payload and returns it.
func (q *Queue) Enqueue(payload string) *job.Job {
	q.mu.Lock()
	defer q.mu.Unlock()

	j := &job.Job{
		ID:        fmt.Sprintf("job-%d", q.nextID),
		Payload:   payload,
		CreatedAt: time.Now(),
	}
	q.nextID++
	q.jobs = append(q.jobs, j)
	return j
}

// Dequeue pops the oldest job off the queue and marks it in-flight
// until it gets acked.
func (q *Queue) Dequeue() (*job.Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.jobs) == 0 {
		return nil, false
	}

	j := q.jobs[0]
	q.jobs = q.jobs[1:]
	q.inFlight[j.ID] = j
	return j, true
}

// Ack marks a job as done, removing it from the in-flight map.
func (q *Queue) Ack(id string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	_, ok := q.inFlight[id]
	if !ok {
		return false
	}
	delete(q.inFlight, id)
	return true
}
