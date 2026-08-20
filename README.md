# GoQueue

A small distributed task queue, written in Go, mostly as a way to actually
learn how these things work instead of just using one.

Still very early / work in progress.

## What is this

Broker holds a job queue, workers pull jobs off it and ack when done. Jobs
are written to a log on disk before being queued so a broker restart
doesn't lose anything. Failed/timed-out jobs get retried with backoff, and
give up into a dead-letter queue after too many failures. There's a
standby broker that can take over if the primary goes down.

More detail (and the reasoning behind the decisions) is in my design notes,
kept locally, not part of this repo.

## Status

Just started. Building it in stages - single in-memory broker first, then
durability, then retries, then the standby/failover part last.

## Running it

Nothing to run yet.
