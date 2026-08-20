package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const brokerURL = "http://localhost:8080"

type Job struct {
	ID        string `json:"id"`
	Payload   string `json:"payload"`
	CreatedAt string `json:"created_at"`
}

func main() {
	fmt.Println("worker starting, polling broker at", brokerURL)

	for {
		j, ok := pollForJob()
		if !ok {
			time.Sleep(1 * time.Second)
			continue
		}

		doWork(j)

		// payloads with "faildemo" in them are for testing retries -
		// pretend the worker crashed and never ack, so the broker
		// times it out and retries it. everything else acks normally.
		if strings.Contains(j.Payload, "faildemo") {
			fmt.Printf("(faildemo) not acking %s on purpose\n", j.ID)
			continue
		}

		ack(j.ID)
	}
}

func pollForJob() (*Job, bool) {
	resp, err := http.Get(brokerURL + "/dequeue")
	if err != nil {
		fmt.Println("error reaching broker:", err)
		return nil, false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, false
	}

	body, _ := io.ReadAll(resp.Body)

	var j Job
	err = json.Unmarshal(body, &j)
	if err != nil {
		fmt.Println("bad job body:", err)
		return nil, false
	}

	return &j, true
}

func doWork(j *Job) {
	fmt.Printf("working on %s: %s\n", j.ID, j.Payload)
	// pretending to do actual work here
	time.Sleep(2 * time.Second)
	fmt.Printf("done with %s\n", j.ID)
}

func ack(id string) {
	body, _ := json.Marshal(map[string]string{"id": id})
	resp, err := http.Post(brokerURL+"/ack", "application/json", bytes.NewReader(body))
	if err != nil {
		fmt.Println("error acking job:", err)
		return
	}
	defer resp.Body.Close()
}
