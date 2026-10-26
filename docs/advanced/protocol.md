# The Pico protocol

Pico treats each topic as an HTTP resource. Its name is the URL path:
`/orders` names one topic. That is why any HTTP client can read or write
a topic, and why this client needs no session or registration first.

A topic has one or more partitions, numbered from 0. Each partition is an
ordered list of records, and order holds inside one partition only. A reader
that needs order reads one partition.

This client implements Pico only, not the separate Durable Streams protocol.
For every wire header and status code, use PicoMQ's upstream
[HTTP API](https://picomq.com/docs/api) or
[OpenAPI document](https://picomq.com/openapi.yaml).

## HTTP operations

Each operation is an ordinary HTTP request:

| Operation | HTTP request |
|---|---|
| Create | `PUT /orders` |
| Append | `POST /orders?partition=0` |
| Read | `GET /orders?format=binary&seq=0&partition=0` |
| Inspect | `HEAD /orders?partition=0` |
| Trim | `POST` with `Pico-Trim-Seq` |
| Seal | `POST` with `Pico-Closed: true` |
| Delete | `DELETE /orders` |
| List | `GET /?prefix=/orders/` |

The examples select partition 0. Reads, appends, and trims act on one
partition, so on a multi-partition topic they must name it, or the server
returns `400`. Metadata requests may omit it and get aggregate topic info.
[Partition addressing](../topics.md#partition-addressing) lists what
each call needs.

Pico headers begin with `Pico-`. Authentication uses the standard
`Authorization: Bearer ...` header.

## Records, positions, and checkpoints

If you read the upstream HTTP docs, one word changes meaning. The HTTP API
calls a position `seq` (`?seq=`, `Pico-Next-Seq`) and a producer sequence
`Pico-Producer-Seq`. Here a position is a `Position`, and `seq` means only
the producer sequence, so the two numbers never share a name. Other PicoMQ
clients pass positions as strings; here a position is an `int`.

A record has a byte body, an optional byte key, byte-valued record headers,
and a server timestamp.
Record headers are separate from the HTTP headers used by the protocol. A
record's numeric position is stable and increases in append order, starting at
zero. Each partition numbers its records separately.

Reads return each record's position and the position where the next read should
begin. This client reads without a consumer group, so PicoMQ keeps no
checkpoint for it, and positions are plain integers, so a checkpoint is one number that an application can store
anywhere, and resuming after a restart is the application's job; see
[why you save the next position](behavior.md#why-you-save-the-next-position).

Trimming removes old records without renumbering those that remain, so a
position keeps its meaning after a trim and `BEGINNING` still means
position zero, even when that record is gone.

On PicoMQ 0.1.1, a read below the start position may still return cached
records, so a read that succeeds does not prove that nothing was trimmed.
To know what remains, ask `info().start_position`; see
[topic settings](../topics.md#change-topic-settings).

## Writes and producer identity

An append can contain one record or an ordered batch. Records in a batch occupy
consecutive positions and commit together. PicoMQ acknowledges the append only
after storing the records, which become readable at the same point.

A direct append has no producer id or sequence. If its response is lost after
sending starts, the caller may not know whether PicoMQ stored the batch.
Repeating it can create a duplicate.

A producer adds a stable `id` and a sequence number for each batch. PicoMQ
accepts each sequence once, so a retry of the same batch is recognized
instead of stored again.

The epoch says which instance of that `id` is current. The application
chooses it, or the client takes it from the clock, and a newer epoch fences
older producers with the same `id`. It is not the epoch that the server uses
internally for node ownership of a topic. See
[producer delivery and fencing](producer-semantics.md) for what follows from
this, and the [producer guide](../producers.md) for creating one.

## Read modes and validation

Pico exposes the same ordered records through three read modes:

| Read mode | Python call |
|---|---|
| Catch-up: returns available records and stops. | `records()`, `read()` |
| Long poll: waits briefly when none are available. | `records(live=LiveMode.LONG_POLL)`, `read(live=LiveMode.LONG_POLL)` |
| Server-Sent Events (SSE): delivers records as they arrive. | `records(live=LiveMode.SSE)`, `subscribe()` |

Every binary response includes the next position. A long-poll response also
includes an internal cursor that only lets the next long poll continue, so
applications save the position, not the cursor.

### What the client verifies

The client advances only when the records, next position, and cursor agree.
Contradictory data raises `InvalidResponseError` without advancing, and an
incomplete or invalid SSE event is discarded before it can change the
checkpoint. So a bad response can never move your checkpoint past records
you did not receive.

??? note "Header order over SSE"

    SSE JSON keeps one value per header name in each of `headers` and
    `headers_b64`. The decoder keeps each object's order and places text
    entries before binary entries, so SSE cannot preserve header order across
    the two objects. Binary framing can preserve repeated names and their
    order; the [stored-header limitation](behavior.md#record-values) of the
    pinned server applies before either read format is produced.

A reader finishes after reaching the end of a sealed topic.

## Routing and authentication

One PicoMQ node owns a partition at a time. Another node returns HTTP `307` with
the owner's URL. The client preserves the request method and body, validates
the target, and decides whether the target may receive its bearer token,
because a redirect followed blindly would send the token to whatever host
the response named.

The Python client accepts only absolute HTTP or HTTPS targets. It rejects
HTTPS-to-HTTP downgrades and invalid locations. The
[connection guide](../connection.md#restrict-redirect-targets) defines how
`allowed_redirect_origins` controls which targets may receive the token.

A partition can move to another node without changing record positions or
producer state.
