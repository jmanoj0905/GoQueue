package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"goqueue/internal/queue"
)

var q *queue.Queue

func main() {
	role := flag.String("role", "primary", "primary or standby")
	addr := flag.String("addr", ":8080", "address for this broker to listen on")
	walPath := flag.String("wal", "data/wal.log", "path to the write-ahead log file")
	primaryURL := flag.String("primary", "http://localhost:8080", "primary broker's address (only used when -role=standby)")
	flag.Parse()

	err := os.MkdirAll("data", 0755)
	if err != nil {
		log.Fatal("failed to create data dir:", err)
	}

	if *role == "standby" {
		runStandby(*addr, *walPath, *primaryURL)
		return
	}

	q, err = queue.NewWithWAL(*walPath)
	if err != nil {
		log.Fatal("failed to start queue:", err)
	}

	startTimeoutChecker()
	serve(*addr)
}

// startTimeoutChecker kicks off the background goroutine that checks
// for jobs whose worker never acked in time, so they can get retried
// or dead-lettered. Needs to run whether we started as primary or got
// promoted from standby.
func startTimeoutChecker() {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for range ticker.C {
			q.CheckTimeouts()
		}
	}()
}

// serve registers all the HTTP handlers and blocks forever serving
// requests. Called by main() directly for a primary, or by
// runStandby() once it's promoted itself.
func serve(addr string) {
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/enqueue", enqueueHandler)
	http.HandleFunc("/dequeue", dequeueHandler)
	http.HandleFunc("/ack", ackHandler)
	http.HandleFunc("/dlq", dlqHandler)
	http.HandleFunc("/metrics", metricsHandler)

	fmt.Println("broker listening on", addr)
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		log.Fatal(err)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "ok")
}

type enqueueRequest struct {
	Payload  string `json:"payload"`
	Priority string `json:"priority"` // "high", "normal", or "low" - defaults to normal
}

func enqueueHandler(w http.ResponseWriter, r *http.Request) {
	var req enqueueRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "bad request body", http.StatusBadRequest)
		return
	}

	j := q.Enqueue(req.Payload, req.Priority)
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

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(q.Stats())
}
