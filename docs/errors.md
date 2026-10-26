# Handle errors and unknown outcomes

When a write fails, the first question is not "what went wrong?" but "did
it happen?". Every error the client raises answers that in one of three
ways: the request never went out, the server rejected it, or nobody knows.
What you do next follows from the answer.

A failed direct write, such as `append()`, raises one of three errors:

| Error | Meaning |
|---|---|
| `TransportError` | The client never sent the request. |
| `ServerResponseError` | The server rejected the write. |
| `WriteUnknownError` | The server may have applied the write. |

After a `TransportError`, you can retry, because nothing reached the server.
After a `ServerResponseError`, fix the
cause, or wait after a 429, then retry. An `InvalidResponseError` with a 4xx
status also means that the server rejected that direct write.

A proxy or load balancer in front of PicoMQ can answer instead of the
server, often with an HTML page. The client then raises a plain
`ServerResponseError` whose `code` is `http_<status>`, such as `http_429`.
Handle it by its `status`, as above. The client does not raise a more
specific error such as `NotFoundError` or `StaleEpochError`, because the
proxy, not PicoMQ, gave the answer. A proxy's 5xx on a write still raises
`WriteUnknownError`, described below.

`InvalidResponseError` means a response that claims to be PicoMQ's but is
broken. An error page larger than 64 KiB counts as broken too.

`WriteUnknownError` is the hard case. The response was lost, the response was
a server error (a 5xx status), or the client could not parse a success
response, and in each case the write may have happened. Its `cause` holds
the original error. Never retry blindly: if the first attempt was stored, a
repeated `append()` stores the records twice. For other writes, check the
current state first. Use `info()` or `config()` for a topic, and
`get_schema()` for a schema.

`UnauthenticatedError` means the server rejected the token;
`PermissionDeniedError` means the token cannot perform this operation. Fix
the token or its permissions, then retry.

## Handle direct-write errors

One `try` statement can tell the cases apart. The example reads the bearer
token from the `PICO_TOKEN` environment variable:

=== "Async"

    ```python
    import os

    from picomq import AsyncClient, TransportError, WriteUnknownError
    from picomq.protocol import PermissionDeniedError, ServerResponseError, UnauthenticatedError

    token = os.environ["PICO_TOKEN"]

    async with AsyncClient("http://127.0.0.1:4437", token=token) as client:
        try:
            orders = client.stream("/orders")
            ack = await orders.append("order-42")
            print("next position:", ack.next_position)
        except (UnauthenticatedError, PermissionDeniedError):
            print("Check the token and its permissions")
        except ServerResponseError as error:
            print("Rejected:", error.code)  # http_<status>, such as http_429, when a proxy answered
        except WriteUnknownError:
            print("order-42 may or may not be stored")
        except TransportError:
            print("The request was not sent")
    ```

=== "Blocking"

    ```python
    import os

    from picomq import Client, TransportError, WriteUnknownError
    from picomq.protocol import PermissionDeniedError, ServerResponseError, UnauthenticatedError

    token = os.environ["PICO_TOKEN"]

    with Client("http://127.0.0.1:4437", token=token) as client:
        try:
            orders = client.stream("/orders")
            ack = orders.append("order-42")
            print("next position:", ack.next_position)
        except (UnauthenticatedError, PermissionDeniedError):
            print("Check the token and its permissions")
        except ServerResponseError as error:
            print("Rejected:", error.code)  # http_<status>, such as http_429, when a proxy answered
        except WriteUnknownError:
            print("order-42 may or may not be stored")
        except TransportError:
            print("The request was not sent")
    ```

You cannot settle an unknown outcome after the fact unless you prepared for
it before the write. Choose one of these plans before you send a direct
append:

1. Use a producer when duplicate writes matter. PicoMQ identifies each batch
   by producer id, epoch, and sequence, so the producer can retry the same
   batch without storing it twice. An exhausted retry can still end with
   an unknown outcome.
2. Give each record a unique message-ID header, such as a `message-id`
   [record header](producers.md#add-headers-to-a-record), save the topic's
   `next_position` before the append, and after an unknown outcome read from
   that position and look for the header:

    ```python
    orders = client.stream("/orders", partition=0)
    before = orders.info().next_position  # save this first
    record = AppendRecord(b'{"order": 43}', headers={"message-id": "o-43"})
    try:
        orders.append(record)
    except WriteUnknownError:
        with orders.records(from_=before) as stored:
            found = any(dict(r.headers).get("message-id") == b"o-43" for r in stored)
        if not found:
            orders.append(record)
    ```

    Read the partition the append targets: `info()` without a partition has
    no positions on a multi-partition topic. On a single-partition topic,
    `client.stream(name)` works as it is.

3. Accept at-least-once delivery: retry the direct append and make readers
   ignore message-ID headers they have already processed.

## Handle producer failures

A producer fails differently. Its errors arrive mostly on each record's
outcome rather than from `send()`, and one failure affects the records
queued behind it:

| Error | Meaning |
|---|---|
| `StaleEpochError` | A newer epoch owns this producer id. |
| `ProducerPoisonedError` | This producer instance cannot send more records. |
| `ProducerClosedError` | The record was rejected from the local buffer before it was sent. |
| `ProducerEpochReusedError` | An earlier producer instance already used this `id` and epoch. |
| `WriteUnknownError` | A producer batch may have been stored. |

Each failure calls for one next step, and the comments in this function name
them. It sends a list of records and returns the ones that are safe to send
again through a new producer:

=== "Async"

    ```python
    from picomq import (
        AsyncClient,
        Delivery,
        DeliveryRejected,
        DeliveryUnknown,
        ProducerClosedError,
        ProducerEpochReusedError,
        ProducerPoisonedError,
    )
    from picomq.protocol import StaleEpochError


    async def publish(client: AsyncClient, bodies: list[str]) -> list[str]:
        orders = client.stream("/orders", partition=0)
        resend = []
        async with orders.producer("checkout-1") as producer:
            pending = []
            for body in bodies:
                try:
                    pending.append(await producer.send(body))
                except (ProducerClosedError, ProducerPoisonedError):
                    break  # the producer stopped; the rest were never queued
            for body, item in zip(bodies, pending):
                match await item.outcome():
                    case Delivery():
                        pass
                    case DeliveryRejected(error=StaleEpochError() as error):
                        # Another instance owns checkout-1, or this clock is behind.
                        # Stop this worker: a new producer here would fence the owner.
                        raise error
                    case DeliveryRejected(error=ProducerEpochReusedError()):
                        resend.append(body)  # nothing was stored
                    case DeliveryRejected(error=ProducerPoisonedError() | ProducerClosedError()):
                        resend.append(body)  # never sent
                    case DeliveryRejected(error=error):
                        raise error
                    case DeliveryUnknown(error=error):
                        # May be stored. Check the topic before sending it again.
                        print("unknown:", error.producer_id, error.epoch, error.seq)
            resend.extend(bodies[len(pending) :])  # never queued
        return resend


    async with AsyncClient("http://127.0.0.1:4437") as client:
        left = await publish(client, ["order-41", "order-42"])
        if left:
            left = await publish(client, left)  # a new producer takes a new epoch
    ```

=== "Blocking"

    ```python
    from picomq import (
        Client,
        Delivery,
        DeliveryRejected,
        DeliveryUnknown,
        ProducerClosedError,
        ProducerEpochReusedError,
        ProducerPoisonedError,
    )
    from picomq.protocol import StaleEpochError


    def publish(client: Client, bodies: list[str]) -> list[str]:
        orders = client.stream("/orders", partition=0)
        resend = []
        with orders.producer("checkout-1") as producer:
            pending = []
            for body in bodies:
                try:
                    pending.append(producer.send(body))
                except (ProducerClosedError, ProducerPoisonedError):
                    break  # the producer stopped; the rest were never queued
            for body, item in zip(bodies, pending):
                match item.outcome():
                    case Delivery():
                        pass
                    case DeliveryRejected(error=StaleEpochError() as error):
                        # Another instance owns checkout-1, or this clock is behind.
                        # Stop this worker: a new producer here would fence the owner.
                        raise error
                    case DeliveryRejected(error=ProducerEpochReusedError()):
                        resend.append(body)  # nothing was stored
                    case DeliveryRejected(error=ProducerPoisonedError() | ProducerClosedError()):
                        resend.append(body)  # never sent
                    case DeliveryRejected(error=error):
                        raise error
                    case DeliveryUnknown(error=error):
                        # May be stored. Check the topic before sending it again.
                        print("unknown:", error.producer_id, error.epoch, error.seq)
            resend.extend(bodies[len(pending) :])  # never queued
        return resend


    with Client("http://127.0.0.1:4437") as client:
        left = publish(client, ["order-41", "order-42"])
        if left:
            left = publish(client, left)  # a new producer takes a new epoch
    ```

??? note "The same example with exceptions"

    This version uses the same `publish()` function and recovery steps:

    === "Async"

        ```python
        from picomq import (
            AsyncClient,
            ProducerClosedError,
            ProducerEpochReusedError,
            ProducerPoisonedError,
            WriteUnknownError,
        )
        from picomq.protocol import StaleEpochError


        async def publish(client: AsyncClient, bodies: list[str]) -> list[str]:
            orders = client.stream("/orders", partition=0)
            resend = []
            async with orders.producer("checkout-1") as producer:
                pending = []
                for body in bodies:
                    try:
                        pending.append(await producer.send(body))
                    except (ProducerClosedError, ProducerPoisonedError):
                        break  # the producer stopped; the rest were never queued
                for body, item in zip(bodies, pending):
                    try:
                        await item
                    except StaleEpochError:
                        # Another instance owns checkout-1, or this clock is behind.
                        # Stop this worker: a new producer here would fence the owner.
                        raise
                    except ProducerEpochReusedError:
                        resend.append(body)  # nothing was stored
                    except (ProducerPoisonedError, ProducerClosedError):
                        resend.append(body)  # never sent
                    except WriteUnknownError as error:
                        # May be stored. Check the topic before sending it again.
                        print("unknown:", error.producer_id, error.epoch, error.seq)
                resend.extend(bodies[len(pending) :])  # never queued
            return resend


        async with AsyncClient("http://127.0.0.1:4437") as client:
            left = await publish(client, ["order-41", "order-42"])
            if left:
                left = await publish(client, left)  # a new producer takes a new epoch
        ```

    === "Blocking"

        ```python
        from picomq import (
            Client,
            ProducerClosedError,
            ProducerEpochReusedError,
            ProducerPoisonedError,
            WriteUnknownError,
        )
        from picomq.protocol import StaleEpochError


        def publish(client: Client, bodies: list[str]) -> list[str]:
            orders = client.stream("/orders", partition=0)
            resend = []
            with orders.producer("checkout-1") as producer:
                pending = []
                for body in bodies:
                    try:
                        pending.append(producer.send(body))
                    except (ProducerClosedError, ProducerPoisonedError):
                        break  # the producer stopped; the rest were never queued
                for body, item in zip(bodies, pending):
                    try:
                        item.result()
                    except StaleEpochError:
                        # Another instance owns checkout-1, or this clock is behind.
                        # Stop this worker: a new producer here would fence the owner.
                        raise
                    except ProducerEpochReusedError:
                        resend.append(body)  # nothing was stored
                    except (ProducerPoisonedError, ProducerClosedError):
                        resend.append(body)  # never sent
                    except WriteUnknownError as error:
                        # May be stored. Check the topic before sending it again.
                        print("unknown:", error.producer_id, error.epoch, error.seq)
                resend.extend(bodies[len(pending) :])  # never queued
            return resend


        with Client("http://127.0.0.1:4437") as client:
            left = publish(client, ["order-41", "order-42"])
            if left:
                left = publish(client, left)  # a new producer takes a new epoch
        ```

    A successful wait needs no handler. Other errors propagate; add a
    handler when your application has a recovery step for them.

After the first failed batch the producer is poisoned, so the records behind
it are rejected with `ProducerPoisonedError` and all go on the resend list.
When the loop handles every failure, leaving the block raises nothing. Once
the producer is poisoned, `send()` itself raises `ProducerPoisonedError`.
That can happen while the loop is still queuing, so the loop stops there,
and the bodies it did not queue also go on the resend list. Catching that
error counts as seeing the failure, so leaving the block does not raise it
a second time.

`StaleEpochError` can also mean that this host's clock is behind the epoch
the server stored. To take the id over on purpose, fix the clock, use a new
`id`, or pass an epoch above the error's `current_epoch`. See
[producer fencing](advanced/producer-semantics.md#how-fencing-changes-producer-ownership).

`ProducerEpochReusedError` happens only with an explicit `epoch`; a producer
without one takes a fresh epoch from the clock. After `WriteUnknownError`,
read the topic from the position saved before the batch, as in
[step 6 of the API tour](examples.md#6-handle-an-unknown-outcome), before
any producer sends the batch again.
