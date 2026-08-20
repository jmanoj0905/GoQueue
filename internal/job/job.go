package job

import "time"

// Priority levels a job can be enqueued with. Higher priority jobs
// get dequeued first (mostly - see queue.Dequeue for the anti
// starvation bit).
const (
	PriorityHigh   = "high"
	PriorityNormal = "normal"
	PriorityLow    = "low"
)

// Job is one unit of work sitting in the queue.
type Job struct {
	ID        string    `json:"id"`
	Payload   string    `json:"payload"`
	Priority  string    `json:"priority"`
	CreatedAt time.Time `json:"created_at"`
	Attempts  int       `json:"attempts"` // how many times this has been handed to a worker
}
