package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"goqueue/internal/queue"
)

var q *queue.Queue

func main() {
	err := os.MkdirAll("data", 0755)
	if err != nil {
		log.Fatal("failed to create data dir:", err)
	}

	q, err = queue.NewWithWAL("data/wal.log")
	if err != nil {
		log.Fatal("failed to start queue:", err)
	}

	// background goroutine that checks for jobs whose worker never
	// acked in time, so they can get retried or dead-lettered
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for range ticker.C {
			q.CheckTimeouts()
		}
	}()

	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/enqueue", enqueueHandler)
	http.HandleFunc("/dequeue", dequeueHandler)
	http.HandleFunc("/ack", ackHandler)
	http.HandleFunc("/dlq", dlqHandler)

	port := ":8080"
	fmt.Println("broker starting on", port)
	err = http.ListenAndServe(port, nil)
	if err != nil {
		log.Fatal(err)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "ok")
}

type enqueueRequest struct {
	Payload string `json:"payload"`
}

func enqueueHandler(w http.ResponseWriter, r *http.Request) {
	var req enqueueRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "bad request body", http.StatusBadRequest)
		return
	}

	j := q.Enqueue(req.Payload)
	json.NewEncoder(w).Encode(j)
}

func dequeueHandler(w http.ResponseWriter, r *http.Request) {
	j, ok := q.Dequeue()
	if !ok {
		http.Error(w, "no jobs available", http.StatusNoContent)
		return
	}

	json.NewEncoder(w).Encode(j)
}

type ackRequest struct {
	ID string `json:"id"`
}

func ackHandler(w http.ResponseWriter, r *http.Request) {
	var req ackRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "bad request body", http.StatusBadRequest)
		return
	}

	ok := q.Ack(req.ID)
	if !ok {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	fmt.Fprintln(w, "acked")
}

func dlqHandler(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(q.DLQ())
}
