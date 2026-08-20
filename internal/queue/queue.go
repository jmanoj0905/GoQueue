package queue

import (
	"fmt"
	"sync"
	"time"

	"goqueue/internal/job"
	"goqueue/internal/wal"
)

// how long a worker has to ack a job before we assume it died and
// give the job to someone else.
const leaseTimeout = 10 * time.Second

// give up on a job after this many attempts and move it to the dlq
// instead of retrying forever.
const maxAttempts = 5

// Queue is an in-memory FIFO queue backed by a write-ahead log, so a
// broker restart doesn't lose jobs that were already enqueued.
//
// broker handlers run one goroutine per request, so this thing gets
// hit from multiple goroutines at once. everything below needs the
// mutex held (found this out the hard way with -race, see
// queue_test.go).
type Queue struct {
	mu            sync.Mutex
	jobs          []*job.Job
	inFlight      map[string]*job.Job
	leaseDeadline map[string]time.Time
	dlq           []*job.Job
	nextID        int
	log           *wal.WAL // can be nil, e.g. in tests
}

func New() *Queue {
	return &Queue{
		jobs:          make([]*job.Job, 0),
		inFlight:      make(map[string]*job.Job),
		leaseDeadline: make(map[string]time.Time),
		nextID:        1,
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
		case wal.EventAck, wal.EventDLQ:
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
	j.Attempts++
	q.inFlight[j.ID] = j
	q.leaseDeadline[j.ID] = time.Now().Add(leaseTimeout)
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
	delete(q.leaseDeadline, id)
	return true
}

// CheckTimeouts looks for in-flight jobs whose worker never acked in
// time and either puts them back on the queue (with a backoff delay)
// or, if they've failed too many times already, moves them to the
// dead-letter queue. Meant to be called on a timer from main.
func (q *Queue) CheckTimeouts() {
	q.mu.Lock()

	now := time.Now()
	var timedOut []*job.Job
	for id, deadline := range q.leaseDeadline {
		if now.After(deadline) {
			timedOut = append(timedOut, q.inFlight[id])
		}
	}

	for _, j := range timedOut {
		delete(q.inFlight, j.ID)
		delete(q.leaseDeadline, j.ID)

		if j.Attempts >= maxAttempts {
			q.dlq = append(q.dlq, j)
			if q.log != nil {
				err := q.log.Append(wal.Event{Type: wal.EventDLQ, ID: j.ID})
				if err != nil {
					fmt.Println("wal append failed:", err)
				}
			}
			continue
		}

		// exponential backoff based on how many times we've tried
		// this job already: 1s, 2s, 4s, 8s...
		backoff := time.Duration(1<<uint(j.Attempts-1)) * time.Second
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}

		jobCopy := j
		time.AfterFunc(backoff, func() {
			q.requeue(jobCopy)
		})
	}

	q.mu.Unlock()
}

// requeue puts a job that failed/timed out back on the queue. Grabs
// its own lock since it gets called later from a timer, not from
// inside CheckTimeouts' lock.
func (q *Queue) requeue(j *job.Job) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.jobs = append(q.jobs, j)
}

// DLQ returns everything that's given up retrying. Note this is
// in-memory only - unlike the main queue, dead-letter jobs don't
// survive a broker restart yet.
func (q *Queue) DLQ() []*job.Job {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.dlq
}
