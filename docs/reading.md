# Read topics

How you read depends on what your app does when it reaches the newest
record. A report job reads what is there and stops. A notification service
waits for the next record and handles it as it arrives. Pick the row that
matches your app:

| You want to | Call |
|---|---|
| Replay the records that exist, then stop | `records()` |
| Follow a topic over one open connection | `records(live=LiveMode.SSE)` |
| Follow a topic with one request at a time | `records(live=LiveMode.LONG_POLL)` |
| Get topic state as well as records | `subscribe()` |
| Control the page size, or write your own loop | `read()` |

With `LiveMode.SSE` the server pushes each record as it is stored. With
`LiveMode.LONG_POLL` each request waits on the server until a record
arrives. `subscribe()` yields data and control events over one connection.
`read()` fetches one page, and you continue from the page's next position.

Records in a partition are numbered from zero; a record's number is its
**position**. `from_` says where a read starts:

```python
orders.records()  # from the start: from_=BEGINNING
orders.records(from_=NOW, live=LiveMode.SSE)  # only records appended from now on
orders.records(from_=checkpoint)  # resume from a saved next_position
```

Each record you read carries `next_position`, the position right after it.
Save that number, and the next read can start exactly where this one
stopped. The saved number is the reader's **checkpoint**.

A **sealed** topic accepts no more appends, so a reader that has read all of
it has nothing left to wait for.

## Resume from a checkpoint

A reader that restarts should continue where it stopped, not from the
beginning. After [step 1 of the API tour](examples.md#1-create-a-topic-and-write-records)
has created the order, this example processes the first event, closes that
reader, and opens a new one at the saved checkpoint:

=== "Async"

    ```python
    import asyncio
    import json

    from picomq import AsyncClient


    async def main() -> None:
        async with AsyncClient("http://127.0.0.1:4437") as client:
            order = client.stream("/examples/orders/1042")
            async with order.records() as records:
                record = await anext(records)
                print("Processed:", json.loads(record.body)["type"])
                checkpoint = record.next_position

            # The first reader is closed. Continue with a new one.
            async with order.records(from_=checkpoint) as records:
                async for record in records:
                    print("Resumed:", json.loads(record.body)["type"])
                    checkpoint = record.next_position


    asyncio.run(main())
    ```

=== "Blocking"

    ```python
    import json

    from picomq import Client

    with Client("http://127.0.0.1:4437") as client:
        order = client.stream("/examples/orders/1042")
        with order.records() as records:
            record = next(records)
            print("Processed:", json.loads(record.body)["type"])
            checkpoint = record.next_position

        # The first reader is closed. Continue with a new one.
        with order.records(from_=checkpoint) as records:
            for record in records:
                print("Resumed:", json.loads(record.body)["type"])
                checkpoint = record.next_position
    ```

With those two events, the output is:

```text
Processed: order.placed
Resumed: order.paid
```

Any events appended since step 1 also appear in the resumed read.

This example keeps the checkpoint in memory; a real reader persists it after
processing each record. Save it after processing, not before;
[why you save the next position](advanced/behavior.md#why-you-save-the-next-position)
shows what goes wrong the other way round.

## Follow new records

A plain `records()` loop ends at the newest record. To keep waiting for new
ones, pass `live=LiveMode.SSE`: one connection stays open, and the server
pushes each record as soon as it is stored. The loop and the checkpoint rule
stay the same:

=== "Async"

    ```python
    import json

    from picomq import AsyncClient, LiveMode

    async with AsyncClient("http://127.0.0.1:4437") as client:
        order = client.stream("/examples/orders/1042")
        async with order.records(from_=checkpoint, live=LiveMode.SSE) as records:
            async for record in records:
                print(json.loads(record.body)["type"])
                checkpoint = record.next_position
    ```

=== "Blocking"

    ```python
    import json

    from picomq import Client, LiveMode

    with Client("http://127.0.0.1:4437") as client:
        order = client.stream("/examples/orders/1042")
        with order.records(from_=checkpoint, live=LiveMode.SSE) as records:
            for record in records:
                print(json.loads(record.body)["type"])
                checkpoint = record.next_position
    ```

The loop runs until you leave the block or the topic is sealed. If the
connection drops, the client reconnects from the last complete event; see
[Configure SSE reconnects and limits](#configure-sse-reconnects-and-limits).
`live=LiveMode.LONG_POLL` follows the topic with one request at a time
instead of an open connection.

## Subscribe to events

A records loop tells you about records and nothing else. Sometimes the
application also needs the state of the topic. Has the reader caught up with
the newest record, so a page can stop showing a spinner? Has the topic been
sealed, so no more records will come? `subscribe()` answers both. Over one
open connection, it yields two kinds of events:

```python
SseDataEvent(records, next_position)
#   records: a complete batch; save next_position after processing all of it
SseControlEvent(next_position, up_to_date, sealed)
#   up_to_date: no record is available at next_position right now
#   sealed: the server accepts no more appends, and every record has been read
```

A control event describes the topic at that moment, so the same state may
arrive again after a reconnect. A sealed control event ends the iteration;
closing the iterator ends it too, whether or not the topic is sealed. When
you need only records, `records(live=LiveMode.SSE)` is the simpler loop.

=== "Async"

    ```python
    from picomq import AsyncClient, NOW, SseControlEvent, SseDataEvent

    async with AsyncClient("http://127.0.0.1:4437") as client:
        orders = client.stream("/orders")
        async with orders.subscribe(from_=NOW) as events:
            async for event in events:
                if isinstance(event, SseDataEvent):
                    for record in event.records:
                        print(record.body.decode())
                    # Save event.next_position after processing the whole batch.
                elif isinstance(event, SseControlEvent):
                    print("caught up:", event.up_to_date)
                    print("sealed:", event.sealed)
    ```

=== "Blocking"

    ```python
    from picomq import Client, NOW, SseControlEvent, SseDataEvent

    with Client("http://127.0.0.1:4437") as client:
        orders = client.stream("/orders")
        with orders.subscribe(from_=NOW) as events:
            for event in events:
                if isinstance(event, SseDataEvent):
                    for record in event.records:
                        print(record.body.decode())
                    # Save event.next_position after processing the whole batch.
                elif isinstance(event, SseControlEvent):
                    print("caught up:", event.up_to_date)
                    print("sealed:", event.sealed)
    ```

## Configure SSE reconnects and limits

Long-lived connections break: a server restarts, a proxy times out. When an
SSE connection ends, the client reconnects on its own and continues from the
last complete event, so your loop sees one continuous read. How hard it
tries is set once on the client, with `ReconnectOptions`; every SSE read,
through `records()` or `subscribe()`, uses that setting.

=== "Async"

    ```python
    from picomq import AsyncClient, ReconnectOptions, SseDataEvent

    reconnect = ReconnectOptions(max_attempts=5)

    async with AsyncClient(
        "http://127.0.0.1:4437",
        reconnect=reconnect,
        max_sse_event_bytes=1024 * 1024,  # largest SSE event accepted
    ) as client:
        orders = client.stream("/orders")
        async with orders.subscribe() as events:
            async for event in events:
                if isinstance(event, SseDataEvent):
                    for record in event.records:
                        print(record.body.decode())
    ```

=== "Blocking"

    ```python
    from picomq import Client, ReconnectOptions, SseDataEvent

    reconnect = ReconnectOptions(max_attempts=5)

    with Client(
        "http://127.0.0.1:4437",
        reconnect=reconnect,
        max_sse_event_bytes=1024 * 1024,  # largest SSE event accepted
    ) as client:
        orders = client.stream("/orders")
        with orders.subscribe() as events:
            for event in events:
                if isinstance(event, SseDataEvent):
                    for record in event.records:
                        print(record.body.decode())
    ```

| `max_attempts` | Behavior |
|---|---|
| `5` | Up to five reconnect attempts. |
| `0` | No automatic reconnects. |
| `None` | Keep trying without a limit. |

The API reference (published once the client code is public) lists the delay, randomized delay (called
jitter), and event-size settings. An event the client cannot decode ends the
subscription with `InvalidResponseError`, without a reconnect, and an
incomplete event cannot advance the checkpoint.

When the attempts run out, the loop raises the last error. Catch it and open
a new subscription from your saved checkpoint. The client's own reconnects
resume only from the last complete event; a read that started from `NOW` has
no numeric checkpoint until such an event establishes one.

## Read a page

To choose how many records each request brings back, for example to process
a backlog in chunks of 100 or to keep memory bounded, use `read()`. It
fetches one page and stops:

=== "Async"

    ```python
    from picomq import LiveMode, ReadLimits

    orders = async_client.stream(name)
    page = await orders.read(limits=ReadLimits(count=100))
    next_page = await orders.read(from_=page.next_position, live=LiveMode.LONG_POLL)
    ```

=== "Blocking"

    ```python
    from picomq import LiveMode, ReadLimits

    orders = client.stream(name)
    page = orders.read(limits=ReadLimits(count=100))
    next_page = orders.read(from_=page.next_position, live=LiveMode.LONG_POLL)
    ```

```python
page.records  # up to 100 records
page.next_position  # pass as from_ to continue
page.up_to_date  # True when this page reached the end of the topic
page.sealed  # True when the topic is sealed and fully read
```

`ReadLimits` bounds the page by record count or bytes. The first record is
returned even when it exceeds the byte limit, so a page is never empty just
because one record is large. `live=LiveMode.LONG_POLL` waits for new records
instead of returning an empty page; a page read has no SSE form, and limits
do not apply to SSE.
[Step 9 of the API tour](examples.md#9-read-one-page-at-a-time) reads a
whole topic page by page.

??? note "Differences from the other PicoMQ clients"

    The other PicoMQ clients follow a topic in `records` only by long poll;
    this client adds `LiveMode.SSE`. Go names the modes `LiveOff` and
    `LiveLongPoll`.

## Read one partition

Order holds inside one partition only, so a read covers one partition. On a
topic with several, make one object per partition:
`client.stream(name, partition=1).records()` or
`client.stream(name, partition=1).subscribe()`. Only `info()` works without a
partition. See [partition addressing](topics.md#partition-addressing).

## Start after trimmed records

Trimming removes old records but does not renumber the rest, and
`BEGINNING` always means position zero. So after a trim, a read from the
beginning asks for records that are gone, and can raise `PositionGoneError`.
Start from the earliest record that remains instead:

```python
info = order.info()
with order.records(
    from_=info.start_position
) as records:  # None on a multi-partition topic: use client.stream(name, partition=N)
    ...
```

A trimmed read does not always fail on PicoMQ 0.1.1; see
[records and positions](advanced/protocol.md#records-positions-and-checkpoints).
