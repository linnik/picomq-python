# How the client behaves

Two words here differ from upstream PicoMQ. A record's offset is its
**position**, and `client.stream(name)` returns a **stream**: the handle you
read and write one topic through. Topics, partitions, records, and producers
mean what [Topics and streams](https://picomq.com/docs/design/streams) says
they mean.

## Why queued is not stored

Suppose you write a record and your app crashes a moment later. Is the
record safe? It depends on how you wrote it.

A direct `append()` is one request, and it returns only after the server has
stored the whole batch. Once it returns, the record is safe and readers can
see it:

```python
ack = orders.append("order-41", "order-42")  # returns once both are stored
```

It can also end with a rejection or an unknown outcome; the
[error guide](../errors.md#handle-direct-write-errors) says what to do
in each case.

A producer does two things a direct append does not. It gives every write an
identity, so a retry of the same batch is stored once. It groups records into
batches, so many small writes cost one request.

To do both, it must take your record first and send it later. That is why
`send()` returns at once:

```python
with orders.producer("checkout") as producer:
    pending = producer.send("order-43")  # queued in the producer's buffer, not stored
```

If the app crashes at this point, the record is lost with the buffer.

Later, each record gets exactly one outcome. There are three because the
network allows three answers:

```python
Delivery  # the server said yes
DeliveryRejected  # the server said no
DeliveryUnknown  # the answer never arrived
```

The client does not guess, so the third case is reported as unknown rather
than as one of the other two.

So queued is not stored, and the outcome is what tells you:

=== "Async"

    ```python
    outcome = await pending.outcome()  # a value: one of the three above
    ```

=== "Blocking"

    ```python
    outcome = pending.outcome()  # a value: one of the three above
    ```

??? note "The same wait with exceptions"

    Use the pending delivery returned by `send()`:

    === "Async"

        ```python
        delivery = await pending  # a Delivery, or raises the rejected or unknown error
        ```

    === "Blocking"

        ```python
        delivery = pending.result()  # a Delivery, or raises the rejected or unknown error
        ```

`await pending` and `send_durable()` raise like `result()`. The
[producer guide](../producers.md) shows when each call returns, and
[producer delivery](producer-semantics.md) explains what each outcome means,
including a duplicate `Delivery`.

## Why you save the next position

Many queues remember, for each reader, where it stopped. PicoMQ can do that
for consumer groups, but this client reads without one, so the server keeps
no position for you. Instead, every record carries `next_position`, the
number of the record after it, and that number is the whole checkpoint: one
integer that you store wherever you like.

```python
with orders.records(from_=checkpoint) as records:
    for record in records:
        handle(record)
        checkpoint = record.next_position  # one integer; store it anywhere
```

Because the checkpoint is yours, resuming after a crash is yours to get
right.

??? note "Resume across a crash, as a picture"

    ```mermaid
    sequenceDiagram
        participant App
        participant Store as Your checkpoint store
        participant Server as PicoMQ
        App->>Server: records(from_=20)
        Server-->>App: record 20 ... record 26, next_position 27
        App->>App: process 20 ... 26
        App->>Store: save 27
        Note over App: crash and restart
        App->>Store: load
        Store-->>App: 27
        App->>Server: records(from_=27)
        Server-->>App: record 27 ...
    ```

Save the checkpoint after processing a record, not before:

```python
for record in records:
    handle(record)  # a crash before the next line replays this record after restart
    save(record.next_position)
```

With the order reversed, a crash between the save and the handling skips the
record for good. A replay can be handled and a skip cannot, so handlers
should tolerate seeing a record twice.

Trimming removes old records but does not renumber the ones that remain, so
a checkpoint saved long ago can point below the topic's start;
[start after trimmed records](../reading.md#start-after-trimmed-records)
shows the recovery.

An SSE subscription advances its checkpoint only after it has decoded and
validated a complete event. A reconnect resumes from the checkpoint, so
moving it past records you never received would skip them.

## Why the client waits on exit

Two rules decide when work starts and when it may stop, and both exist so
that no work is lost without your knowing.

Work starts late: `records()`, `subscribe()`, and `list()` make no request
until you iterate:

```python
records = orders.records()  # no request yet
with records:  # releases the response if the loop stops early
    for record in records:  # the request starts here
        ...
```

A stream object is a value: create it, pass it to the code that reads, and
any error surfaces there, where you can handle it. Each returned iterator has
one consumer, because it reads one response in order.

Work stops late too. A producer starts its worker when you enter its block,
and a normal exit from the block stops the worker only after it has
finished:

```python
with orders.producer("checkout") as producer:
    producer.send("order-41")
    producer.send("order-42")
# leaving the block waits for both; a failure that no wait reported raises here
```

Normal exit waits for every queued record, because an app that exits
before its writes finish would lose them without knowing. A failure is
reported once: the exit raises it only when no `outcome()`, `result()`,
`await`, `flush()`, or refused `send()` already did.

During an outage this wait can be long. The producer keeps retrying the
batch in flight until its delivery timeout runs out, two minutes by
default, plus a little for its last attempt. Closing the client does not
wait that long: it stops after `close_timeout`.

If your code raises inside the block, cleanup is different: it preserves
your error, because your error is the one you need to see. The producer stops
taking records but keeps sending for up to `close_timeout`. Records still in
flight then end up unknown, and records not yet sent are rejected.

On exit, the client stops its iterators and producers before it closes the
HTTP transport, including a transport you supplied. Closing the transport
first would turn every write still in flight into an unknown outcome.

## State words

Four words describe something that has stopped. They are easy to mix up,
because each is set in a different place:

| Word | Who sets it, and what it means |
|---|---|
| `sealed` | The server. The topic rejects future appends. |
| `closed` | You. A local client, producer, or iterator released its resources. |
| `poisoned` | The client, after a producer error. The producer stops queuing; `send()` raises. |
| `stopped` | The client. A producer worker has no work left. |

```python
client.seal("/orders")  # on the server: the topic stops accepting writes
client.close()  # in your app: connections are released, the topic is untouched
```

Other PicoMQ clients call the sealed state closed. Here `close()` is always
local and `seal()` is always the server, so closing a client, producer, or
iterator never seals a topic. A poisoned producer does not continue under
the same identity.

## Why async objects stay in one runtime

An async producer sends its batches from a background worker, and that
worker belongs to the runtime, asyncio or Trio, that entered the producer.
So enter and exit a client or producer in the same task and runtime. Sibling tasks in that runtime may use it. An
entered object must not move to another event loop or Trio run, because two
runtimes would then drive one worker.

## Bound a wait without cancelling delivery

Sometimes your code cannot wait long, for example while it answers an HTTP
request of its own. A timeout limits your wait, not the write. In blocking
code, `Pending.result(timeout)`, `Pending.outcome(timeout)`, and
`send_durable(..., timeout=)` raise the built-in `TimeoutError` when the
time runs out. It is not a `PicoMQError`, because nothing happened to the
record: it stays queued or in flight, and it still gets its outcome.

In async code, limit the wait with an AnyIO cancel scope or
`asyncio.wait_for()`. The cancellation stays the runtime's own exception,
not a `TransportError`. In these examples, `pending` comes from `send()`
inside an entered producer block:

=== "Async"

    ```python
    import asyncio

    try:
        outcome = await asyncio.wait_for(pending.outcome(), timeout=1.0)
    except TimeoutError:
        outcome = await pending.outcome()  # keep waiting for the same write
    ```

=== "Blocking"

    ```python
    try:
        outcome = pending.outcome(timeout=1.0)
    except TimeoutError:
        outcome = pending.outcome()  # keep waiting for the same write
    ```

With exceptions instead of outcome values, make the second wait with
`await pending` or `pending.result()`.

Be careful about letting the `TimeoutError` leave the producer block. The
block then exits with an error, so cleanup waits only up to
`close_timeout`. After that, the batch in flight ends unknown and queued
records are rejected. A one-second wait can thus turn a batch that would
have been stored a minute later, well within its
[delivery timeout](producer-semantics.md#how-long-a-producer-keeps-trying),
into an unknown outcome.

## Record values

PicoMQ stores bytes. A `str` body or key is encoded as UTF-8, and reads
return bytes; decode them when you know the body is text:

```python
orders.append("order-1")
record.body  # b"order-1"
record.body.decode()  # "order-1", when you know it is text
```

`AppendRecord` and `Record` keep an immutable snapshot of their headers.
`Record.headers` is an ordered tuple of `(name, bytes)` pairs, and string
values you supply are encoded as UTF-8:

```python
record.headers  # (("content-type", b"application/json"), ("message-id", b"o-43"))
dict(record.headers)  # {"content-type": b"application/json", "message-id": b"o-43"}
```

Repeated names keep only their last value in the dict. PicoMQ 0.1.1 also
keeps only the last value for each repeated header name when reading stored
records; the client sends every pair in order but cannot recover pairs the
server discarded. Use distinct header names when each value must survive a
round trip.

A key is optional text or bytes, supplied with `key=`, for example
`AppendRecord("order", key="o-43")`. An absent key (`None`) differs from an
empty key (`b""`).
