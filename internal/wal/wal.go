// Package wal is a super basic write-ahead log. Append-only file,
// one JSON object per line. Idea is: before we tell someone a job
// is queued (or acked), write it here first. If the broker crashes
// and restarts, replay this file to get back to where we were.
package wal

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"

	"goqueue/internal/job"
)

// EventType tells us what kind of thing happened.
type EventType string

const (
	EventEnqueue EventType = "enqueue"
	EventAck     EventType = "ack"
	EventDLQ     EventType = "dlq" // job gave up retrying, moved to dead-letter queue
)

// Event is one line in the log file.
type Event struct {
	Type EventType `json:"type"`
	Job  *job.Job  `json:"job,omitempty"` // set for enqueue events
	ID   string    `json:"id,omitempty"`  // set for ack events
}

type WAL struct {
	mu   sync.Mutex
	file *os.File
}

// Open opens (or creates) the log file for appending.
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f}, nil
}

// Append writes one event to the log and flushes it to disk before
// returning. If this returns without an error, the event is durable.
func (w *WAL) Append(e Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	line, err := json.Marshal(e)
	if err != nil {
		return err
	}
	line = append(line, '\n')

	_, err = w.file.Write(line)
	if err != nil {
		return err
	}

	return w.file.Sync()
}

// ReadAll reads every event currently in the log file, in order.
// Used on startup to rebuild queue state. Opens the file separately
// for reading so it doesn't mess with the append handle.
func ReadAll(path string) ([]Event, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		// no log yet, nothing to replay
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var events []Event
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var e Event
		err := json.Unmarshal(line, &e)
		if err != nil {
			// skip bad lines instead of crashing on startup -
			// might happen if broker died mid-write
			continue
		}
		events = append(events, e)
	}

	return events, scanner.Err()
}

func (w *WAL) Close() error {
	return w.file.Close()
}
