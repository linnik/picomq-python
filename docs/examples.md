# Python API tour

Install `picomq` and start a server with the upstream
[PicoMQ quick start](https://picomq.com/docs/quick-start).

## One order, many readers

Order 1042 is placed, paid, packed, shipped, and delivered, and several apps
follow it: "where is my order?", emails, the warehouse. In PicoMQ its history
is a **topic**: a numbered list of **records** that writers append to and
readers read in order. Each reader resumes from its own **checkpoint**. A
writer that may retry uses a **producer**, so a retried record is stored
once.

## 1. Create a topic and write records

A customer has just placed order 1042 and paid for it. Create the order's
topic, `/examples/orders/1042`, and store those two changes as its first
two records in one request. Each record is a JSON object that says what
happened.

=== "Async"

    ```python
    import asyncio
    import json

    from picomq import AsyncClient


    async def main() -> None:
        async with AsyncClient("http://127.0.0.1:4437") as client:
            name = "/examples/orders/1042"
            await client.create(name, content_type="application/json")
            order = client.stream(name)
            ack = await order.append(
                json.dumps({"type": "order.placed", "item": "keyboard"}),
                json.dumps({"type": "order.paid", "payment_id": "PAY-8201"}),
            )
            print("Order history ends at:", ack.next_position)


    asyncio.run(main())
    ```

=== "Blocking"

    ```python
    import json

    from picomq import Client

    with Client("http://127.0.0.1:4437") as client:
        name = "/examples/orders/1042"
        client.create(name, content_type="application/json")
        order = client.stream(name)
        ack = order.append(
            json.dumps({"type": "order.placed", "item": "keyboard"}),
            json.dumps({"type": "order.paid", "payment_id": "PAY-8201"}),
        )
        print("Order history ends at:", ack.next_position)
    ```

On a new topic, this prints `Order history ends at: 2`.
`client.stream(name)` returns a stream: the handle you read and write the
topic through. `append()` returns only after the server confirms the write. The async
snippets below run inside an async function such as `main()`.

To attach metadata that readers receive with a record, see
[Add headers to a record](producers.md#add-headers-to-a-record).
For tokens, timeouts, and custom networking, see
[Configure connections](connection.md).

## 2. Read records and save your place

The "where is my order?" page replays the order's history to show its
current state. After processing each event, it keeps `Record.next_position`
as the checkpoint for the next read.

=== "Async"

    ```python
    import json

    from picomq import BEGINNING, AsyncClient

    checkpoint = BEGINNING

    async with AsyncClient("http://127.0.0.1:4437") as client:
        order = client.stream("/examples/orders/1042")
        async with order.records(from_=checkpoint) as records:
            async for record in records:
                event = json.loads(record.body)
                print(event["type"])
                checkpoint = record.next_position
    ```

=== "Blocking"

    ```python
    import json

    from picomq import BEGINNING, Client

    checkpoint = BEGINNING

    with Client("http://127.0.0.1:4437") as client:
        order = client.stream("/examples/orders/1042")
        with order.records(from_=checkpoint) as records:
            for record in records:
                event = json.loads(record.body)
                print(event["type"])
                checkpoint = record.next_position
    ```

After step 1, this prints:

```text
order.placed
order.paid
```

**Persist the checkpoint** after processing so a restart resumes where it
stopped; this example only keeps it in memory. The
[resume example](reading.md#resume-from-a-checkpoint) shows closing a
reader and opening another from the saved checkpoint.

## 3. Wait for new records

The email app sends an email when something new happens to the order, so its
first run starts at `NOW`, which skips existing history. `live=LiveMode.SSE`
keeps one connection open while the server pushes each new record. Like the
page in step 2, it keeps `record.next_position` as its checkpoint: after a
restart it starts from the saved checkpoint instead of `NOW` and catches up
on events it missed. Leave it running through steps 4 and 5.

=== "Async"

    ```python
    import json

    from picomq import AsyncClient, LiveMode, NOW

    checkpoint = NOW

    async with AsyncClient("http://127.0.0.1:4437") as client:
        order = client.stream("/examples/orders/1042")
        async with order.records(from_=checkpoint, live=LiveMode.SSE) as records:
            async for record in records:
                event = json.loads(record.body)
                print(event["type"])
                checkpoint = record.next_position
    ```

=== "Blocking"

    ```python
    import json

    from picomq import Client, LiveMode, NOW

    checkpoint = NOW

    with Client("http://127.0.0.1:4437") as client:
        order = client.stream("/examples/orders/1042")
        with order.records(from_=checkpoint, live=LiveMode.SSE) as records:
            for record in records:
                event = json.loads(record.body)
                print(event["type"])
                checkpoint = record.next_position
    ```

The client reconnects an interrupted connection from its last complete
event. `live=LiveMode.LONG_POLL` waits with one request at a time instead.
When the application also needs topic state, such as the topic being
sealed, use [`subscribe()`](reading.md#subscribe-to-events); the
[FastAPI notifications example](https://github.com/linnik/picomq-python/tree/main/examples/fastapi-notifications)
relays its events to browsers.

## 4. Publish through a producer

The warehouse app records that the order is packed. It may retry, so it
writes through a producer: the producer id lets PicoMQ recognize a retried
batch and store it once. `send_durable()` returns after the server has
stored the record.

=== "Async"

    ```python
    import json

    from picomq import AsyncClient

    async with AsyncClient("http://127.0.0.1:4437") as client:
        order = client.stream("/examples/orders/1042")
        # One id per worker replica. A restarted worker reuses its id.
        async with order.producer("fulfillment-worker-1") as producer:
            delivery = await producer.send_durable(json.dumps({"type": "order.packed"}))
            print("Packing event stored at position", delivery.position)
    ```

=== "Blocking"

    ```python
    import json

    from picomq import Client

    with Client("http://127.0.0.1:4437") as client:
        order = client.stream("/examples/orders/1042")
        # One id per worker replica. A restarted worker reuses its id.
        with order.producer("fulfillment-worker-1") as producer:
            delivery = producer.send_durable(json.dumps({"type": "order.packed"}))
            print("Packing event stored at position", delivery.position)
    ```

After steps 1 and 2, this prints `Packing event stored at position 2`.

Run one producer with this id at a time. A restarted worker reuses the id and
gets a new epoch from the clock, which fences the earlier instance. The
[producer guide](producers.md) explains batching, identity, and failure
handling.

## 5. Queue several records and read each outcome

Queue the shipping updates with `send()`, which returns at once, then read
each update's outcome with `outcome()`. Queuing does not confirm storage;
the outcome does.

Why the `try` around `send()`? The producer starts sending batches while
your loop is still queuing. If one of them fails, the producer stops, and
the next `send()` raises instead of queuing. The loop then stops, and the
updates it never queued are printed as not sent.

=== "Async"

    ```python
    import json

    from picomq import (
        AsyncClient,
        Delivery,
        DeliveryRejected,
        DeliveryUnknown,
        ProducerClosedError,
        ProducerPoisonedError,
    )

    updates = [
        {"type": "order.shipped", "tracking_id": "PKG-8201"},
        {"type": "order.delivered", "received_by": "customer"},
    ]

    async with AsyncClient("http://127.0.0.1:4437") as client:
        order = client.stream("/examples/orders/1042")
        async with order.producer("fulfillment-worker-1") as producer:
            pending = []
            for event in updates:
                try:
                    pending.append(await producer.send(json.dumps(event)))
                except (ProducerClosedError, ProducerPoisonedError):
                    break  # the producer stopped; the rest were never queued
            stored = 0
            for item in pending:
                match await item.outcome():
                    case Delivery():
                        stored += 1
                    case DeliveryRejected(error=error):
                        print("Rejected:", error)
                    case DeliveryUnknown(error=error):
                        print("May be stored; seq", error.seq)
            for event in updates[len(pending) :]:
                print("Not sent:", event["type"])
            print("Shipping updates stored:", stored)
    ```

=== "Blocking"

    ```python
    import json

    from picomq import (
        Client,
        Delivery,
        DeliveryRejected,
        DeliveryUnknown,
        ProducerClosedError,
        ProducerPoisonedError,
    )

    updates = [
        {"type": "order.shipped", "tracking_id": "PKG-8201"},
        {"type": "order.delivered", "received_by": "customer"},
    ]

    with Client("http://127.0.0.1:4437") as client:
        order = client.stream("/examples/orders/1042")
        with order.producer("fulfillment-worker-1") as producer:
            pending = []
            for event in updates:
                try:
                    pending.append(producer.send(json.dumps(event)))
                except (ProducerClosedError, ProducerPoisonedError):
                    break  # the producer stopped; the rest were never queued
            stored = 0
            for item in pending:
                match item.outcome():
                    case Delivery():
                        stored += 1
                    case DeliveryRejected(error=error):
                        print("Rejected:", error)
                    case DeliveryUnknown(error=error):
                        print("May be stored; seq", error.seq)
            for event in updates[len(pending) :]:
                print("Not sent:", event["type"])
            print("Shipping updates stored:", stored)
    ```

This prints `Shipping updates stored: 2`. By now the email app from step 3 has
printed `order.packed`, `order.shipped`, and `order.delivered`.

??? note "The same example with exceptions"

    This version uses the same updates and producer, with exception handling:

    === "Async"

        ```python
        import json

        from picomq import (
            AsyncClient,
            PicoMQError,
            ProducerClosedError,
            ProducerPoisonedError,
            WriteUnknownError,
        )

        updates = [
            {"type": "order.shipped", "tracking_id": "PKG-8201"},
            {"type": "order.delivered", "received_by": "customer"},
        ]

        async with AsyncClient("http://127.0.0.1:4437") as client:
            order = client.stream("/examples/orders/1042")
            async with order.producer("fulfillment-worker-1") as producer:
                pending = []
                for event in updates:
                    try:
                        pending.append(await producer.send(json.dumps(event)))
                    except (ProducerClosedError, ProducerPoisonedError):
                        break  # the producer stopped; the rest were never queued
                stored = 0
                for item in pending:
                    try:
                        await item
                    except WriteUnknownError as error:
                        print("May be stored; seq", error.seq)
                    except PicoMQError as error:
                        print("Rejected:", error)
                    else:
                        stored += 1
                for event in updates[len(pending) :]:
                    print("Not sent:", event["type"])
                print("Shipping updates stored:", stored)
        ```

    === "Blocking"

        ```python
        import json

        from picomq import (
            Client,
            PicoMQError,
            ProducerClosedError,
            ProducerPoisonedError,
            WriteUnknownError,
        )

        updates = [
            {"type": "order.shipped", "tracking_id": "PKG-8201"},
            {"type": "order.delivered", "received_by": "customer"},
        ]

        with Client("http://127.0.0.1:4437") as client:
            order = client.stream("/examples/orders/1042")
            with order.producer("fulfillment-worker-1") as producer:
                pending = []
                for event in updates:
                    try:
                        pending.append(producer.send(json.dumps(event)))
                    except (ProducerClosedError, ProducerPoisonedError):
                        break  # the producer stopped; the rest were never queued
                stored = 0
                for item in pending:
                    try:
                        item.result()
                    except WriteUnknownError as error:
                        print("May be stored; seq", error.seq)
                    except PicoMQError as error:
                        print("Rejected:", error)
                    else:
                        stored += 1
                for event in updates[len(pending) :]:
                    print("Not sent:", event["type"])
                print("Shipping updates stored:", stored)
        ```

    Catch `WriteUnknownError` before its base class, `PicoMQError`, so an
    unknown write is not treated as a rejection.

## 6. Handle an unknown outcome

Support adds a note with a plain `append()`. A lost response or a server
error (a 5xx status) hides the outcome: the note may or may not be stored.
Save the topic's end before the write and give the note an ID. After
`WriteUnknownError`, read from the saved position and look for that ID.

=== "Async"

    ```python
    import json

    from picomq import AsyncClient, WriteUnknownError

    note = {"type": "order.note_added", "event_id": "note-1042-1"}

    async with AsyncClient("http://127.0.0.1:4437") as client:
        order = client.stream("/examples/orders/1042")
        info = await order.info()
        before = info.next_position  # save this before the write
        try:
            await order.append(json.dumps(note))
        except WriteUnknownError:
            stored = False
            async with order.records(from_=before) as records:
                async for record in records:
                    if json.loads(record.body).get("event_id") == note["event_id"]:
                        stored = True
            if not stored:
                await order.append(json.dumps(note))
    ```

=== "Blocking"

    ```python
    import json

    from picomq import Client, WriteUnknownError

    note = {"type": "order.note_added", "event_id": "note-1042-1"}

    with Client("http://127.0.0.1:4437") as client:
        order = client.stream("/examples/orders/1042")
        info = order.info()
        before = info.next_position  # save this before the write
        try:
            order.append(json.dumps(note))
        except WriteUnknownError:
            stored = False
            with order.records(from_=before) as records:
                for record in records:
                    if json.loads(record.body).get("event_id") == note["event_id"]:
                        stored = True
            if not stored:
                order.append(json.dumps(note))
    ```

The check reads what the server has stored so far. A write that is still
in flight during the check can land afterward, so readers should tolerate a
repeated event ID. See
[Handle direct-write errors](errors.md#handle-direct-write-errors)
for the other recovery choices. A producer, as in step 4, retries the same
batch itself without storing it twice.

## 7. Inspect, seal, and delete the topic

The order is delivered. Inspect it, list the example orders, then seal and
delete the topic. `seal()` and `close()` are different operations:

| Call | What it does |
|---|---|
| `seal()` | On the server. The topic stops accepting writes and stays readable. |
| `close()` | In your app. The client releases its connections; the topic is untouched. |

=== "Async"

    ```python
    from picomq import AsyncClient

    async with AsyncClient("http://127.0.0.1:4437") as client:
        order = client.stream("/examples/orders/1042")
        info = await order.info()
        if info is not None:
            print("Order:", info.name, "History ends at:", info.next_position)

        async for topic in client.list(prefix="/examples/orders/"):
            print(topic.name)

        await client.seal("/examples/orders/1042")  # readers still work
        await client.delete("/examples/orders/1042")  # removes the topic
    # Leaving the block closes the client. A close never seals a topic.
    ```

=== "Blocking"

    ```python
    from picomq import Client

    with Client("http://127.0.0.1:4437") as client:
        order = client.stream("/examples/orders/1042")
        info = order.info()
        if info is not None:
            print("Order:", info.name, "History ends at:", info.next_position)

        for topic in client.list(prefix="/examples/orders/"):
            print(topic.name)

        client.seal("/examples/orders/1042")  # readers still work
        client.delete("/examples/orders/1042")  # removes the topic
    # Leaving the block closes the client. A close never seals a topic.
    ```

In an application, keep the history as long as readers need it. To remove
only an older portion, see [trimming](topics.md#trim-seal-or-delete).

## 8. Import with an explicit identity

An import job copies an archived order into a second topic, and a rerun
must not store it twice. `append_as` sends one batch under an identity you
manage: an `id`, an `epoch`, and a sequence number.
Sending the same identity and batch again is a retry, which PicoMQ
recognizes while it remembers the producer.

=== "Async"

    ```python
    import json

    from picomq import AppendDuplicate, AsyncClient

    events = [
        {"type": "order.placed", "item": "keyboard"},
        {"type": "order.shipped", "tracking_id": "PKG-8201"},
    ]

    async with AsyncClient("http://127.0.0.1:4437") as client:
        name = "/examples/order-imports/1042"
        await client.create(name, content_type="application/json")
        order = client.stream(name)
        result = await order.append_as(
            [json.dumps(event) for event in events], id="archive-import", epoch=0, seq=0
        )
        if isinstance(result, AppendDuplicate):
            print("Already imported")
        else:
            print("Imported; history ends at", result.next_position)
    ```

=== "Blocking"

    ```python
    import json

    from picomq import AppendDuplicate, Client

    events = [
        {"type": "order.placed", "item": "keyboard"},
        {"type": "order.shipped", "tracking_id": "PKG-8201"},
    ]

    with Client("http://127.0.0.1:4437") as client:
        name = "/examples/order-imports/1042"
        client.create(name, content_type="application/json")
        order = client.stream(name)
        result = order.append_as(
            [json.dumps(event) for event in events], id="archive-import", epoch=0, seq=0
        )
        if isinstance(result, AppendDuplicate):
            print("Already imported")
        else:
            print("Imported; history ends at", result.next_position)
    ```

The first run prints `Imported; history ends at 2`. A rerun prints
`Already imported` and stores nothing. See
[explicit append identity](producers.md#append-with-an-explicit-identity)
for what a duplicate reports and how to recover an unknown outcome.

## 9. Read one page at a time

A report job reads the imported order one record at a time. `read()`
fetches one page. `ReadLimits` bounds the page; pass
`page.next_position` as `from_` to continue, and stop when the page reaches
the end of the topic.

=== "Async"

    ```python
    import json

    from picomq import BEGINNING, AsyncClient, ReadLimits

    async with AsyncClient("http://127.0.0.1:4437") as client:
        order = client.stream("/examples/order-imports/1042")
        position = BEGINNING
        while True:
            page = await order.read(from_=position, limits=ReadLimits(count=1))
            for record in page.records:
                print(json.loads(record.body)["type"])
            if page.up_to_date or not page.records:
                break
            position = page.next_position
    ```

=== "Blocking"

    ```python
    import json

    from picomq import BEGINNING, Client, ReadLimits

    with Client("http://127.0.0.1:4437") as client:
        order = client.stream("/examples/order-imports/1042")
        position = BEGINNING
        while True:
            page = order.read(from_=position, limits=ReadLimits(count=1))
            for record in page.records:
                print(json.loads(record.body)["type"])
            if page.up_to_date or not page.records:
                break
            position = page.next_position
    ```

Output:

```text
order.placed
order.shipped
```

See [page reads](reading.md#read-a-page) for limits and live pages.

## 10. Change topic settings and clean up

Keep the imported history for one day, then remove the example topic.

=== "Async"

    ```python
    from picomq import AsyncClient, ConfigUpdate

    async with AsyncClient("http://127.0.0.1:4437") as client:
        name = "/examples/order-imports/1042"
        config = await client.update_config(name, ConfigUpdate(retention_ms=86_400_000))
        print("Retention in ms:", config.retention_ms)
        await client.delete(name)
    ```

=== "Blocking"

    ```python
    from picomq import Client, ConfigUpdate

    with Client("http://127.0.0.1:4437") as client:
        name = "/examples/order-imports/1042"
        config = client.update_config(name, ConfigUpdate(retention_ms=86_400_000))
        print("Retention in ms:", config.retention_ms)
        client.delete(name)
    ```

This prints `Retention in ms: 86400000`. See
[topic settings](topics.md#change-topic-settings) for the other
settings.
