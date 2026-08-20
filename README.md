# GoQueue

A small distributed task queue, written in Go, mostly as a way to actually
learn how these things work instead of just using one.

## What is this

Broker holds a job queue, workers pull jobs off it and ack when done. Jobs
are written to a log on disk before being queued so a broker restart
doesn't lose anything. Failed/timed-out jobs get retried with backoff, and
give up into a dead-letter queue after too many failures. Jobs can be
enqueued with a priority (high/normal/low). There's a standby broker that
can take over if the primary goes down.

More detail (and the reasoning behind the decisions) is in my design notes,
kept locally, not part of this repo.

## Status

Core features are all in: durable queue, retries + DLQ, priorities,
metrics, primary/standby failover. Still rough around the edges in
places - see "known limitations" below.

## Running it

Start a broker:

```
go run ./cmd/broker
```

Enqueue a job:

```
curl localhost:8080/enqueue -d '{"payload":"hello","priority":"high"}'
```

Start a worker (in another terminal) to pull and process jobs:

```
go run ./cmd/worker
```

Check what's going on:

```
curl localhost:8080/metrics
curl localhost:8080/dlq
```

### Trying the retry/DLQ path

Enqueue a payload containing `faildemo` - the worker will pick it up but
deliberately not ack it, so you can watch it get retried (with backoff)
and eventually land in the dead-letter queue:

```
curl localhost:8080/enqueue -d '{"payload":"faildemo test"}'
```

### Trying primary/standby failover

Run a primary and a standby pointed at the same WAL file:

```
go run ./cmd/broker -role=primary -addr=:8080 -wal=data/wal.log
go run ./cmd/broker -role=standby -addr=:8081 -primary=http://localhost:8080 -wal=data/wal.log
```

Kill the primary and watch the standby's logs - after a few missed
heartbeats it promotes itself and starts serving on :8081 with whatever
was in the WAL.

## Known limitations

Documenting these on purpose instead of hiding them:

- At-least-once delivery, not exactly-once. A job can run twice if a
  worker crashes right after finishing but before acking.
- Standby failover relies on both brokers reading the same WAL file on
  the same disk - there's no actual network replication. A "real"
  version would ship WAL entries over the network.
- No proper leader election. If the primary is just slow (not actually
  dead), the standby could still promote itself, and then you'd have two
  brokers both willing to accept writes. Went with "assume dead after N
  missed heartbeats" on purpose to keep this simple instead of
  implementing raft.
- Dead-letter queue is in-memory only right now - doesn't survive a
  broker restart (the main queue does, via the WAL, but DLQ entries
  aren't replayed back into it).
