package job

import "time"

// Job is one unit of work sitting in the queue.
type Job struct {
	ID        string    `json:"id"`
	Payload   string    `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
	Attempts  int       `json:"attempts"` // how many times this has been handed to a worker
}
