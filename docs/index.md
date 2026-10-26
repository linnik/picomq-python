# PicoMQ Python client

`picomq` is the Python SDK for PicoMQ's Pico protocol. It has a blocking
`Client` and an async `AsyncClient` (asyncio or Trio) with the same calls,
and adds a batching producer that stores a retried record once.

## Quick start

```bash
pip install picomq
```

The server runs separately. Follow the upstream
[quick start](https://picomq.com/docs/quick-start), then run:

```bash
pico serve
```

The example stores two changes of a shop order and reads them back:

=== "Async"

    ```python
    import asyncio
    import json

    from picomq import AsyncClient


    async def main() -> None:
        async with AsyncClient("http://127.0.0.1:4437") as client:
            name = "/orders/1042"
            await client.create(name, content_type="application/json")
            order = client.stream(name)
            await order.append(
                json.dumps({"type": "order.placed", "item": "keyboard"}),
                json.dumps({"type": "order.shipped", "tracking_id": "PKG-8201"}),
            )

            async with order.records() as records:
                async for record in records:
                    event = json.loads(record.body)
                    print(event["type"])


    asyncio.run(main())
    ```

=== "Blocking"

    ```python
    import json

    from picomq import Client

    with Client("http://127.0.0.1:4437") as client:
        name = "/orders/1042"
        client.create(name, content_type="application/json")
        order = client.stream(name)
        order.append(
            json.dumps({"type": "order.placed", "item": "keyboard"}),
            json.dumps({"type": "order.shipped", "tracking_id": "PKG-8201"}),
        )

        with order.records() as records:
            for record in records:
                event = json.loads(record.body)
                print(event["type"])
    ```

On a new topic, the output is:

```text
order.placed
order.shipped
```

To pick between the two clients, see
[Choose a client and async runtime](connection.md#choose-a-client-and-async-runtime).

## Next steps

- [Follow the complete Python API tour](examples.md)
- [Build live notifications with FastAPI](https://github.com/linnik/picomq-python/tree/main/examples/fastapi-notifications)

Or go straight to a task:

- [Configure connections](connection.md): set the server address,
  authentication, timeouts, or networking behavior.
- [Manage topics](topics.md): create, inspect, and list topics, change
  their settings, remove old records, stop appends, or delete topics.
- [Publish records](producers.md): retry without duplicates, wait for
  the server's outcome, send several records, and shut down cleanly.
- [Read topics](reading.md): read existing records, resume from a saved
  position, or follow new records.
- [Handle errors](errors.md): recover when a write may or may not have
  reached the server.

Server installation, deployment, administration, authentication, and complete
wire semantics remain in the upstream [PicoMQ documentation](https://picomq.com/docs/).
