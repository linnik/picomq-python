# Producer delivery and fencing

When `send()` returns, your record is not on the server yet. It waits in the
producer's buffer to join a batch. What `send()` gives you is a pending
delivery, which will later tell you what happened to that record.

What can happen? The producer sends the batch and waits for an answer. The
server can say yes, the server can say no, or the answer can get lost on the
way back. So every record ends in one of three outcomes:

| Outcome | Meaning |
|---|---|
| `Delivery` | The server stored the record. |
| `DeliveryRejected` | This operation did not store the record. |
| `DeliveryUnknown` | The record may or may not be stored. |

The third one surprises people. The client could guess, but a wrong guess
either loses your record or stores it twice, so it tells you it does not
know.

`pending.outcome()` gives you the outcome as a value. Waiting with
`await pending` or `pending.result()` raises the rejected or unknown error
instead. Either way, a successful `send()` means only "queued".

A duplicate `Delivery` means that an earlier attempt of the same batch may
have stored it. PicoMQ recognized the batch by its id, epoch, and sequence
and answered "already stored" instead of storing it again, but that answer
does not say where the earlier attempt put the records, so the delivery has
no position and no timestamp. Treat it as stored.

The [producer guide](../producers.md#send-and-wait-with-send_durable)
shows how to create one.

Sometimes you need a barrier: "everything I have sent so far is settled".
That is `flush()`. When it starts, it notes the last record queued so far,
then waits until every record up to that one has its outcome. Records queued
after the call are not part of the wait, so other code can keep sending
while it waits.

## How long a producer keeps trying

A short outage should not cost you records. A server restart, an ownership
move, or a busy proxy can fail a batch for a few seconds. Sending the same
batch again is safe, because PicoMQ recognizes it by its id, epoch, and
sequence. So the producer keeps trying for up to `delivery_timeout`, 120
seconds by default.

The clock starts when the batch is first sent. Time in the queue and
`linger` do not count, so records waiting behind a slow but healthy batch
never run out of time.

Between attempts the producer waits: about 0.1 seconds after the first
failure, then twice as long each time, up to 1 second. Each wait varies by
up to 20%, so that many producers do not retry in step. A `Retry-After`
from the server can make a wait longer. When the next wait would reach the
deadline, the producer stops there, and the batch ends with the result of
its last attempt. Redirects are not retries: they have their own limit of
five hops, and a hop made after the deadline uses the client's normal
timeouts.

Each attempt's timeouts are also cut to the time left, so a server that
hangs cannot hold a batch far past its deadline. The deadline is still not
exact. HTTPX applies a timeout to each step of a request, such as
connecting or reading, so the last attempt can end a little after it.

Choose `delivery_timeout` well above the time the server takes to answer
one batch. If it is shorter, healthy batches run out of time, end unknown,
and poison the producer.

```python
orders.producer("checkout")  # each batch keeps trying for up to 120 s: rides out a server restart
orders.producer("checkout", delivery_timeout=10.0)  # gives up sooner, e.g. inside a request handler
```

To limit only how long your own code waits, see
[bounded waits](behavior.md#bound-a-wait-without-cancelling-delivery).

## Why an outcome becomes unknown

The client can judge a batch only by what comes back. Network failures,
429, 5xx, and `sequence_gap` answers are retried until the deadline, and
all the attempts together decide the outcome:

| What happened | Outcome |
|---|---|
| No attempt reached the server | Rejected |
| Every attempt got 429 | Rejected |
| An attempt may have stored the batch | Unknown at the deadline |
| Any other 4xx | Rejected at once |
| Rejected in the local queue | Rejected; never sent |

Why do a 5xx and a 4xx differ? A 5xx proves nothing: PicoMQ can fail after
it stored the batch, and a proxy can answer 502 or 504 after it forwarded
the request. A 4xx, including a 429, means the attempt was refused and
nothing was stored. That is true for a proxy's own 4xx too, whatever its
page looks like. (`sequence_gap` is a 409 that the producer retries, so it
is not in the "any other 4xx" row.)

One possible store changes the rest of the batch. Once any attempt may have
stored it, a later 4xx cannot prove that the batch is absent, so the
outcome stays unknown. The last error is in the cause chain.

Some answers prove nothing either way, so they also end as unknown, even
with a 429 status: an error that looks like PicoMQ's but has invalid
fields, and an error body larger than 64 KiB. A success answer that the
client cannot parse ends as unknown too.

Once an outcome is unknown, the producer becomes `poisoned` and stops
queuing. The server may or may not have taken that sequence number, so the
producer cannot know which number to use next. The pending delivery has its
final outcome, and the client makes no more attempts.

Do not replay the record under a new identity: if the first write
succeeded, the replay stores it twice. The error names the batch to look
for:

=== "Async"

    ```python
    from picomq import DeliveryRejected, DeliveryUnknown

    match await pending.outcome():
        case DeliveryUnknown(error=error):
            print("unknown:", error.producer_id, error.epoch, error.seq)
        case DeliveryRejected(error=error):
            raise error
    ```

=== "Blocking"

    ```python
    from picomq import DeliveryRejected, DeliveryUnknown

    match pending.outcome():
        case DeliveryUnknown(error=error):
            print("unknown:", error.producer_id, error.epoch, error.seq)
        case DeliveryRejected(error=error):
            raise error
    ```

??? note "The same unknown outcome with exceptions"

    When waiting on a pending delivery, catch `WriteUnknownError` to read
    the same batch identity. Other errors propagate:

    === "Async"

        ```python
        from picomq import WriteUnknownError

        try:
            await pending
        except WriteUnknownError as error:
            print("unknown:", error.producer_id, error.epoch, error.seq)
        ```

    === "Blocking"

        ```python
        from picomq import WriteUnknownError

        try:
            pending.result()
        except WriteUnknownError as error:
            print("unknown:", error.producer_id, error.epoch, error.seq)
        ```

## How fencing changes producer ownership

Picture a worker that hangs, is restarted, and then wakes up: two instances
of the same writer, both sending. If both could write, their batch numbers
would collide. PicoMQ settles this with an **epoch**. The `id` names the
logical writer, and the epoch says which instance of it is current. A newer
epoch **fences** every older producer with the same `id`: the server rejects
their writes from then on:

```text
orders-worker, epoch 7 -> fenced
orders-worker, epoch 8 -> current
```

The older producer receives `StaleEpochError` on its next batch, and is
then `poisoned`: its queued records are rejected with
`ProducerPoisonedError`, and `send()` raises it. Its cause holds the
fencing error. When PicoMQ reports the current epoch, the error carries it:

=== "Async"

    ```python
    from picomq import DeliveryRejected, DeliveryUnknown
    from picomq.protocol import StaleEpochError

    match await pending.outcome():
        case DeliveryRejected(error=StaleEpochError() as error):
            print("current epoch:", error.current_epoch)
        case DeliveryRejected(error=error) | DeliveryUnknown(error=error):
            raise error
    ```

=== "Blocking"

    ```python
    from picomq import DeliveryRejected, DeliveryUnknown
    from picomq.protocol import StaleEpochError

    match pending.outcome():
        case DeliveryRejected(error=StaleEpochError() as error):
            print("current epoch:", error.current_epoch)
        case DeliveryRejected(error=error) | DeliveryUnknown(error=error):
            raise error
    ```

??? note "The same fencing error with exceptions"

    Catch `StaleEpochError` when waiting on a pending delivery to read the
    same current epoch. Other errors propagate:

    === "Async"

        ```python
        from picomq.protocol import StaleEpochError

        try:
            await pending
        except StaleEpochError as error:
            print("current epoch:", error.current_epoch)
        ```

    === "Blocking"

        ```python
        from picomq.protocol import StaleEpochError

        try:
            pending.result()
        except StaleEpochError as error:
            print("current epoch:", error.current_epoch)
        ```

The [state words](behavior.md#state-words) table defines `poisoned`,
`closed`, `stopped`, and `sealed`.

A higher epoch starts a new producer instance. It does not settle earlier
unknown writes. Decide how to handle an unknown batch before starting a
replacement with the same `id`.

## The default epoch

Where does the epoch come from? Usually from the clock: leave `epoch` unset.
Each new producer then takes its epoch from the clock,
in microseconds, so a restart gets a higher epoch than before as long as the
clock moves forward. The newest instance fences older ones on the same
partition for as long as the server remembers them: at most 24 hours after
their last append.

```python
orders.producer("checkout")  # epoch from the clock: a restart fences the old instance
orders.producer("checkout", epoch=8)  # you manage epochs yourself; see below
```

See [Choose the producer id](../producers.md#choose-the-producer-id)
for replicas and partitions.

A clock that is behind the stored epoch also gives `StaleEpochError`, for
example after a clock correction or on another host.
[Handle producer failures](../errors.md#handle-producer-failures) says
how to recover.

??? note "Differences from the other PicoMQ clients"

    The other PicoMQ clients default to epoch 0. This client takes the
    default from the clock, so a rerun does not reuse the last epoch.

## Explicit epochs

Pass an explicit epoch only when you manage epochs yourself. A default
epoch is the current time in microseconds, so a small explicit epoch such as
8 is fenced once any default-epoch producer has written under the same
`id`, whichever of the two ran first. Do not mix the two under one `id`.

A new producer starts its sequence at 0. If it reuses an earlier explicit
epoch, PicoMQ takes its first batches for batches it already stored and
writes nothing. What the client reports depends on the first attempt:

| First attempt of the batch | Outcome |
|---|---|
| Gets the duplicate answer | Rejected with `ProducerEpochReusedError`; the producer is `poisoned` |
| Loses its response or gets a 5xx | A later duplicate counts as a confirmed `Delivery` |

The second case exists because the client cannot tell a reused epoch from a
real retry. With explicit epochs, a higher epoch is the only reliable
protection.

??? note "Differences from the other PicoMQ clients"

    The other clients derive a duplicate batch's positions from the end of
    the topic, which can be wrong after another writer appends. This client reports
    no position for the duplicate batch.

## When the server forgets a producer

The server cannot remember every producer forever, so it forgets idle ones.
In PicoMQ 0.1.1, that happens after 24 hours without appends, or when a
partition tracks more than 1024 producers and this one is the least recently
used.

After that, the server accepts any epoch again. A clock that was behind works
again, and a reused explicit epoch stores its records normally. So the
protection against duplicates has a horizon: a retry of a batch after the
server forgot its producer is a new write and stores the records again. To
resend a known batch on purpose, use `append_as` with its original epoch and
sequence.

## Duplicates without a worker

Direct `append_as` calls use the same server sequencing without a background
worker. They retry up to three attempts within the call and then return
the server's answer or raise the final error. An applied batch returns `AppendAck`. A duplicate returns
`AppendDuplicate.stream_next_position` with only the current end of the topic, not
this batch's checkpoint.

When the outcome is unknown for one of the reasons above, `append_as` raises
`WriteUnknownError` with `producer_id`, `epoch`, and `seq`, and the response
error is in its cause chain. It never invents an acknowledgement or reports a
rejection that the server did not prove. Resolve the outcome under that same
identity; a retry as an ordinary append can store the batch twice.

## Proxies and delivery certainty

Do not configure a proxy to resend a POST after sending it upstream, or to
rewrite response statuses. Either can hide a stored attempt behind a later
4xx and make a stored batch appear rejected. Let the producer perform its
own retries under the same identity.
