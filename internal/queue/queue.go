package queue

import (
	"fmt"
	"sync"
	"time"

	"goqueue/internal/job"
	"goqueue/internal/wal"
)

// Queue is an in-memory FIFO queue backed by a write-ahead log, so a
// broker restart doesn't lose jobs that were already enqueued.
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
	log      *wal.WAL // can be nil, e.g. in tests
}

func New() *Queue {
	return &Queue{
		jobs:     make([]*job.Job, 0),
		inFlight: make(map[string]*job.Job),
		nextID:   1,
	}
}

// NewWithWAL is like New but also loads any existing state from the
// log file and appends new events to it going forward.
func NewWithWAL(logPath string) (*Queue, error) {
	q := New()

	w, err := wal.Open(logPath)
	if err != nil {
		return nil, err
	}
	q.log = w

	events, err := wal.ReadAll(logPath)
	if err != nil {
		return nil, err
	}
	q.replay(events)

	return q, nil
}

// replay rebuilds queue state from a slice of WAL events. jobs that
// were enqueued but never acked go back on the queue - if a job was
// mid-flight to a worker when the broker died, we don't know if it
// finished, so it just gets redelivered. that's the at-least-once
// tradeoff.
func (q *Queue) replay(events []wal.Event) {
	pending := make(map[string]*job.Job)
	order := make([]string, 0)

	for _, e := range events {
		switch e.Type {
		case wal.EventEnqueue:
			pending[e.Job.ID] = e.Job
			order = append(order, e.Job.ID)
		case wal.EventAck:
			delete(pending, e.ID)
		}
	}

	for _, id := range order {
		j, ok := pending[id]
		if ok {
			q.jobs = append(q.jobs, j)
		}
	}

	// keep nextID ahead of whatever we've already handed out, so we
	// don't reuse job IDs after a restart
	for _, id := range order {
		var n int
		fmt.Sscanf(id, "job-%d", &n)
		if n >= q.nextID {
			q.nextID = n + 1
		}
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

	if q.log != nil {
		// if this fails we've got a job we told the caller about but
		// didn't actually persist - not great, but logging it and
		// moving on for now instead of failing the whole request
		err := q.log.Append(wal.Event{Type: wal.EventEnqueue, Job: j})
		if err != nil {
			fmt.Println("wal append failed:", err)
		}
	}

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

	if q.log != nil {
		err := q.log.Append(wal.Event{Type: wal.EventAck, ID: id})
		if err != nil {
			fmt.Println("wal append failed:", err)
		}
	}

	delete(q.inFlight, id)
	return true
}
