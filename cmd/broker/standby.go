package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"goqueue/internal/queue"
)

// how many heartbeats in a row we can miss before deciding the
// primary is actually dead and taking over.
const missedHeartbeatLimit = 3

// runStandby watches the primary broker's /health endpoint. If it
// stops responding for a few heartbeats in a row, this promotes
// itself: loads whatever's in the WAL file and starts serving
// requests as the new primary.
//
// note: this only works because both brokers point at the same WAL
// file path - there's no actual network replication happening, just
// both processes reading/writing the same file on disk. fine for a
// learning project running on one machine, a real setup would need
// to ship WAL entries over the network instead. also no real leader
// election here - if the primary is just slow (not dead), the
// standby could still promote itself and now there'd be two brokers
// both willing to serve writes. good enough for this project though.
func runStandby(addr, walPath, primaryURL string) {
	fmt.Println("starting as standby, watching primary at", primaryURL)

	client := http.Client{Timeout: 1 * time.Second}
	missed := 0

	for {
		time.Sleep(1 * time.Second)

		resp, err := client.Get(primaryURL + "/health")
		if err != nil || resp.StatusCode != http.StatusOK {
			missed++
			fmt.Printf("missed heartbeat from primary (%d/%d)\n", missed, missedHeartbeatLimit)
		} else {
			missed = 0
			resp.Body.Close()
		}

		if missed >= missedHeartbeatLimit {
			break
		}
	}

	fmt.Println("primary looks dead, promoting myself to primary")

	var err error
	q, err = queue.NewWithWAL(walPath)
	if err != nil {
		log.Fatal("failed to load queue from wal on promotion:", err)
	}

	startTimeoutChecker()
	serve(addr)
}
