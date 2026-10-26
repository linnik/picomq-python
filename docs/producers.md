# Publish records

Writing a record looks simple until the network fails halfway. Say you
append an order and the connection drops before the answer comes back. Was
the order stored? Send it again and you may store it twice; don't, and you
may lose it. The three ways to write differ in how they handle that moment:

| Method | What it does |
|---|---|
| `stream.append(record)` | One direct request. Returns an `AppendAck`, or raises. |
| `producer.send_durable(record)` | Queues the record and waits for the server. Returns a `Delivery`, or raises. |
| `producer.send(record)` | Queues the record and returns a `Pending` at once. You wait later. |

They also differ in when they return. `append()` and `send_durable()`
return once the server has stored the write. `send()` returns at once,
before anything is stored, and `outcome()` tells you later.

??? note "When each call returns, as a picture"

    ```mermaid
    sequenceDiagram
        participant App
        participant Producer
        participant Server as PicoMQ
        App->>Server: append(record)
        Server-->>App: AppendAck
        Note over App,Server: append() returns once the batch is stored
        App->>Producer: send_durable(record)
        Producer->>Server: batch
        Server-->>Producer: stored
        Producer-->>App: Delivery
        Note over App,Server: send_durable() returns once the record is stored
        App->>Producer: send(record)
        Producer-->>App: Pending
        Note over App,Producer: send() returns at once, with nothing stored yet
        Producer->>Server: batch
        Server-->>Producer: stored
        App->>Producer: outcome()
        Producer-->>App: Delivery
        Note over App,Server: outcome() returns once the record is stored
    ```

Which one should you use? `append()` is enough for a one-off write, when
you can afford to check the topic after a lost answer. When a retry must not
store a record twice, use a producer. Start with `send_durable()`: it waits
for each record, so the code reads top to bottom. Switch to `send()` when you
write many records and want them batched while your code keeps going; you
then read each record's outcome with `outcome()`.

## Append directly

`append()` sends one request. Pass one record, or several to store them as
one batch: they get consecutive positions, in order, and are stored
together. Each record can be text or bytes.

=== "Async"

    ```python
    from picomq import AsyncClient

    async with AsyncClient("http://127.0.0.1:4437") as client:
        orders = client.stream("/orders")
        ack = await orders.append("order-41", "order-42")
        print("next position:", ack.next_position)
    ```

=== "Blocking"

    ```python
    from picomq import Client

    with Client("http://127.0.0.1:4437") as client:
        orders = client.stream("/orders")
        ack = orders.append("order-41", "order-42")
        print("next position:", ack.next_position)
    ```

When `append()` returns an `AppendAck`, the whole batch is stored. If the
response is lost, the client cannot tell whether the batch arrived, so it
raises `WriteUnknownError` instead of guessing. The [error guide](errors.md)
shows how to recover.

### Add headers to a record

Headers carry metadata such as a content type or a message id. PicoMQ stores
them with the record for your readers and does not interpret or filter on
them.

```python
from picomq import AppendRecord

record = AppendRecord(
    body='{"order": 43}',
    key="o-43",  # optional; text or bytes
    headers={  # names are text; values are text (stored as UTF-8) or bytes
        "content-type": "application/json",
        "message-id": "o-43",
    },
)
```

Pass it in place of text or bytes, and read the headers back as ordered
byte pairs:

```python
ack = orders.append(record)  # or producer.send(record)

for stored in orders.records():
    stored.headers  # (("content-type", b"application/json"), ("message-id", b"o-43"))
    stored.key  # b"o-43"
```

See [record values](advanced/behavior.md#record-values) for how headers and keys
are stored.

## Send and wait with `send_durable()`

`send_durable()` sends one record through a producer and returns once the
server has stored it:

=== "Async"

    `AsyncStream.producer()` itself is not awaited. Enter the object it returns:

    ```python
    from picomq import AsyncClient

    async with AsyncClient("http://127.0.0.1:4437") as client:
        orders = client.stream("/orders")
        async with orders.producer("checkout") as producer:
            delivery = await producer.send_durable("order-43")

            if delivery.duplicate:
                print("duplicate: an earlier attempt may have stored it")
            else:
                print("stored at position", delivery.position)
    ```

=== "Blocking"

    ```python
    from picomq import Client

    with Client("http://127.0.0.1:4437") as client:
        orders = client.stream("/orders")
        with orders.producer("checkout") as producer:
            delivery = producer.send_durable("order-43")

            if delivery.duplicate:
                print("duplicate: an earlier attempt may have stored it")
            else:
                print("stored at position", delivery.position)
    ```

Why a producer? It solves the lost-answer problem with an identity. You
give it an `id` that names the writer, and it numbers every batch it sends.
When an answer is lost, the producer sends the same batch again under the
same number. PicoMQ remembers the numbers it has stored, so it answers
"already stored" instead of storing the batch twice, as long as it still
remembers the producer. Because a resend is safe, the producer keeps trying,
so a server restart or a short outage does not lose the batch. It gives up
after `delivery_timeout`, two minutes by default;
[how long a producer keeps trying](advanced/producer-semantics.md#how-long-a-producer-keeps-trying)
explains the clock.

Entering the producer block starts its background worker, and leaving the
block waits for queued records to finish. `send_durable()` raises when the
record is rejected, or when its outcome is still unknown after the last
attempt.

When `delivery.duplicate` is true, the server answered "already stored": it
recognized the batch's identity from an earlier attempt. Treat the record as
stored. The answer has no position, because it does not say where the
earlier attempt put the record; see
[producer delivery](advanced/producer-semantics.md).

A producer trades a little delay for fewer requests: it waits up to `linger`
seconds for more records, then sends them together. Its settings are keyword
arguments, shown here with their defaults:

```python
orders.producer(
    "checkout",
    epoch=None,  # unset: a new epoch from the clock, so a restart fences the old one
    linger=0.005,  # seconds to wait for more records before sending a batch
    delivery_timeout=120.0,  # seconds a batch keeps trying after its first send
    max_batch_records=500,
    max_batch_bytes=1_048_576,
    max_buffered_bytes=33_554_432,  # send() waits for room beyond this
)
```

### Which timeout limits what

Four timeouts touch a producer, and each limits something different. Only
`delivery_timeout` gives up on a write:

| Timeout | What it limits |
|---|---|
| `delivery_timeout` | How long one batch keeps trying |
| `connect_timeout`, `read_timeout` | One attempt, cut to the time left |
| `timeout=` on a wait | Your wait only; the write goes on |
| `close_timeout` | Cleanup after an error, and closing the client |

So during an outage, `send_durable()` without `timeout=` can wait up to two
minutes: it waits for the delivery timeout, not for one request. When your
code cannot wait that long, give the wait a time limit and catch
`TimeoutError` inside the producer block:

=== "Async"

    ```python
    import asyncio

    pending = await producer.send("order-44")
    try:
        delivery = await asyncio.wait_for(pending, timeout=5.0)
    except TimeoutError:
        print("not confirmed yet; order-44 is still being sent")
    ```

=== "Blocking"

    ```python
    pending = producer.send("order-44")
    try:
        delivery = pending.result(timeout=5.0)
    except TimeoutError:
        print("not confirmed yet; order-44 is still being sent")
    ```

The time limit ends only your wait. The record stays queued, the producer
keeps sending it, and leaving the block still waits for it.
[Bounded waits](advanced/behavior.md#bound-a-wait-without-cancelling-delivery)
shows how to wait again, and why the `TimeoutError` should not leave the
block. You set the client timeouts on the client; see
[Set the endpoint, token, and timeouts](connection.md#set-the-endpoint-token-and-timeouts).

### Choose the producer id

The server remembers two things for each `id` on each partition: which
instance of the writer is current (its epoch), and which batch number it
expects next (its sequence). Two running producers that share an `id` on one
partition would compete for those numbers, so run one producer per `id` and
partition at a time:

| Situation | Id |
|---|---|
| Two worker replicas write to one partition | A different `id` for each replica. |
| One worker writes to two partitions | One producer per partition. The `id` can be the same. |
| A worker restarts | The same `id`. The new epoch fences the old instance. |

```python
# Replica 1 and replica 2 of the same worker, writing to one partition
with client.stream("/orders", partition=0).producer("checkout-1") as producer:
    ...
with client.stream("/orders", partition=0).producer("checkout-2") as producer:
    ...

# One worker covering two partitions
with client.stream("/orders", partition=0).producer("checkout-1") as europe:
    with client.stream("/orders", partition=1).producer("checkout-1") as asia:
        ...
```

A second producer with an `id` that is already live on the same partition
fences the first as soon as it writes: the first producer's next batch is
rejected with `StaleEpochError`. See
[producer fencing](advanced/producer-semantics.md#how-fencing-changes-producer-ownership).

??? note "Differences from the other PicoMQ clients"

    `send_durable` returns a `Delivery`; the other clients return only the
    position. The other clients group producer settings in a
    `ProducerConfig`; here they are keyword arguments with the same names.
    TypeScript's `lingerMs` is in milliseconds; `linger` here is in seconds.
    The other clients use a 12-try retry policy; this client uses a delivery
    timeout like Kafka.

## Queue, then wait with `send()`

`send()` puts a record in the producer's buffer and returns a `Pending` at
once, so many records can go out in one batch. Read each record's outcome
later with `outcome()`:

=== "Async"

    ```python
    from picomq import (
        AsyncClient,
        Delivery,
        DeliveryRejected,
        DeliveryUnknown,
        ProducerClosedError,
        ProducerPoisonedError,
    )

    async with AsyncClient("http://127.0.0.1:4437") as client:
        orders = client.stream("/orders")
        async with orders.producer("checkout") as producer:
            bodies = ("one", "two", "three")
            # `asyncio.gather()` is unnecessary for throughput here
            pending = []
            for body in bodies:
                try:
                    pending.append(await producer.send(body))
                except (ProducerClosedError, ProducerPoisonedError):
                    break  # the producer stopped; the rest were never queued
            for body, item in zip(bodies, pending):
                match await item.outcome():
                    case Delivery() as delivery:
                        print(body, "stored at", delivery.position)
                    case DeliveryRejected(error=ProducerClosedError() | ProducerPoisonedError()):
                        print(body, "was never sent")
                    case DeliveryRejected(error=error):
                        print(body, "rejected:", error)
                    case DeliveryUnknown(error=error):
                        print(body, "may be stored; seq", error.seq)
            for body in bodies[len(pending) :]:
                print(body, "was never sent")
    ```

=== "Blocking"

    ```python
    from picomq import (
        Client,
        Delivery,
        DeliveryRejected,
        DeliveryUnknown,
        ProducerClosedError,
        ProducerPoisonedError,
    )

    with Client("http://127.0.0.1:4437") as client:
        orders = client.stream("/orders")
        with orders.producer("checkout") as producer:
            bodies = ("one", "two", "three")
            pending = []
            for body in bodies:
                try:
                    pending.append(producer.send(body))
                except (ProducerClosedError, ProducerPoisonedError):
                    break  # the producer stopped; the rest were never queued
            for body, item in zip(bodies, pending):
                match item.outcome():
                    case Delivery() as delivery:
                        print(body, "stored at", delivery.position)
                    case DeliveryRejected(error=ProducerClosedError() | ProducerPoisonedError()):
                        print(body, "was never sent")
                    case DeliveryRejected(error=error):
                        print(body, "rejected:", error)
                    case DeliveryUnknown(error=error):
                        print(body, "may be stored; seq", error.seq)
            for body in bodies[len(pending) :]:
                print(body, "was never sent")
    ```

When everything succeeds on a new topic, the output is:

```text
one stored at 0
two stored at 1
three stored at 2
```

With `send_durable()`, every record waits for a round trip before the next
one goes out; `send()` does not wait. `outcome()` returns a value: a
`Delivery` when the server stored the record, a `DeliveryRejected` when the
client knows it did not, or a `DeliveryUnknown` when the record may be
stored. Because there are exactly three outcome types, one `match` covers
them all.

One failure affects the records behind it. After a batch fails, the
producer is **poisoned**: the records still queued are rejected, and
`send()` raises `ProducerPoisonedError`. That can happen while you are
still queuing, because the producer sends the first batches while your loop
adds later records. So the example stops queuing when `send()` raises, and
its loop over the outcomes expects a mix.

`zip()` stops at the shorter list, so the second loop handles only the
records that were queued. `bodies[len(pending):]` are the ones that were
never queued.

`outcome()` never raises for a rejected or unknown delivery, and it returns
at once when the record is already settled. The blocking form raises
`TimeoutError` only when its timeout expires. Leaving the
producer block waits for every queued record and raises a failure only when
no `outcome()`, `result()`, `await`, `flush()`, or refused `send()` reported it.
[Handle producer failures](errors.md#handle-producer-failures) shows a loop
with one next step per failure.

??? note "How the match patterns read"

    ```python
    case Delivery() as delivery:  # any Delivery; name it delivery
    case DeliveryRejected(error=error):  # any rejection; name its error attribute error
    case DeliveryRejected(error=StaleEpochError() as error):  # only a rejection whose
    #   error is a StaleEpochError; name that error
    ```

    Arms are tried in order, so put the narrower pattern first.

??? note "The same example with exceptions"

    `await pending` and `pending.result()` return a `Delivery` or raise the
    error that the value would carry:

    === "Async"

        ```python
        from picomq import (
            AsyncClient,
            PicoMQError,
            ProducerClosedError,
            ProducerPoisonedError,
            WriteUnknownError,
        )

        async with AsyncClient("http://127.0.0.1:4437") as client:
            orders = client.stream("/orders")
            async with orders.producer("checkout") as producer:
                bodies = ("one", "two", "three")
                # `asyncio.gather()` is unnecessary for throughput here
                pending = []
                for body in bodies:
                    try:
                        pending.append(await producer.send(body))
                    except (ProducerClosedError, ProducerPoisonedError):
                        break  # the producer stopped; the rest were never queued
                for body, item in zip(bodies, pending):
                    try:
                        delivery = await item
                    except (ProducerClosedError, ProducerPoisonedError):
                        print(body, "was never sent")
                    except WriteUnknownError as error:
                        print(body, "may be stored; seq", error.seq)
                    except PicoMQError as error:
                        print(body, "rejected:", error)
                    else:
                        print(body, "stored at", delivery.position)
                for body in bodies[len(pending) :]:
                    print(body, "was never sent")
        ```

    === "Blocking"

        ```python
        from picomq import (
            Client,
            PicoMQError,
            ProducerClosedError,
            ProducerPoisonedError,
            WriteUnknownError,
        )

        with Client("http://127.0.0.1:4437") as client:
            orders = client.stream("/orders")
            with orders.producer("checkout") as producer:
                bodies = ("one", "two", "three")
                pending = []
                for body in bodies:
                    try:
                        pending.append(producer.send(body))
                    except (ProducerClosedError, ProducerPoisonedError):
                        break  # the producer stopped; the rest were never queued
                for body, item in zip(bodies, pending):
                    try:
                        delivery = item.result()
                    except (ProducerClosedError, ProducerPoisonedError):
                        print(body, "was never sent")
                    except WriteUnknownError as error:
                        print(body, "may be stored; seq", error.seq)
                    except PicoMQError as error:
                        print(body, "rejected:", error)
                    else:
                        print(body, "stored at", delivery.position)
                for body in bodies[len(pending) :]:
                    print(body, "was never sent")
        ```

## Append with an explicit identity

Sometimes the batch numbers already exist outside your app. An importer
replaying an archive knows which batch is which, and a rerun of the import
should store nothing new. `append_as` sends one batch under the `id`,
`epoch`, and `seq` you supply. It starts no background worker and makes no
retries for you. It returns a value or raises:

| Result | Meaning |
|---|---|
| `AppendAck` | Stored. Reports the batch's positions and the server timestamp. |
| `AppendDuplicate` | An earlier attempt stored it. Reports only the current end of the topic. |
| `StaleEpochError` | A newer epoch owns this `id`. |
| `ConflictError` | `seq` is not the next sequence the server expects. |
| `WriteUnknownError` | Lost response or 5xx. Carries `producer_id`, `epoch`, and `seq`. |

A duplicate's `stream_next_position` is the end of the topic, not this batch's
checkpoint. The original positions and timestamp cannot be recovered.

=== "Async"

    ```python
    from picomq import AppendDuplicate

    orders = async_client.stream(name)
    result = await orders.append_as(["one"], id="writer", epoch=0, seq=0)
    if isinstance(result, AppendDuplicate):
        print("end of the topic:", result.stream_next_position)
    else:
        print("batch checkpoint:", result.next_position)
    ```

=== "Blocking"

    ```python
    from picomq import AppendDuplicate

    orders = client.stream(name)
    result = orders.append_as(["one"], id="writer", epoch=0, seq=0)
    if isinstance(result, AppendDuplicate):
        print("end of the topic:", result.stream_next_position)
    else:
        print("batch checkpoint:", result.next_position)
    ```

Do not mix an explicit epoch and the default epoch under one `id`, and keep
the same identity when resolving an unknown outcome. See
[explicit epochs](advanced/producer-semantics.md#explicit-epochs) and
[duplicates without a worker](advanced/producer-semantics.md#duplicates-without-a-worker).
