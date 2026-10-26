# Configure connections

## Choose a client and async runtime

Use `Client` in blocking code, such as a script or a Flask view, and
`AsyncClient` in asyncio or Trio code. Both have the same calls. This example
asks a server at `http://127.0.0.1:4437` whether an order topic exists:

=== "Async"

    ```python
    from picomq import AsyncClient


    async def main() -> None:
        async with AsyncClient("http://127.0.0.1:4437") as client:
            info = await client.stream("/orders/1042").info()
            print("topic exists:", info is not None)
    ```

    For a standalone asyncio script, add:

    ```python
    import asyncio

    asyncio.run(main())
    ```

    For Trio, install it with `pip install trio` and add this instead:

    ```python
    import trio

    trio.run(main)
    ```

    The `main()` function is the same for both runtimes. An application
    such as a FastAPI app already runs an event loop, so it needs neither
    line: call the client from your own async functions. Calling
    `asyncio.run()` inside a request handler fails, because a loop is
    already running there.

=== "Blocking"

    ```python
    from picomq import Client

    with Client("http://127.0.0.1:4437") as client:
        info = client.stream("/orders/1042").info()
        print("topic exists:", info is not None)
    ```

    `Client` blocks the calling thread until each call finishes. Inside an
    `async def` handler, that would stall every other task on the event
    loop, so use `AsyncClient` there.

## Set the endpoint, token, and timeouts

These examples read the bearer token from the `PICO_TOKEN` environment
variable.

=== "Async"

    ```python
    import os

    from picomq import AsyncClient

    async with AsyncClient(
        "http://pico.internal:4437",
        token=os.environ["PICO_TOKEN"],
        connect_timeout=10.0,  # connection setup
        read_timeout=65.0,  # one wait for data; longer than the server's long poll
        close_timeout=10.0,  # client cleanup
        http2=True,
    ) as client:
        orders = client.stream("/orders")
        info = await orders.info()
        print("topic exists:", info is not None)
    ```

=== "Blocking"

    ```python
    import os

    from picomq import Client

    with Client(
        "http://pico.internal:4437",
        token=os.environ["PICO_TOKEN"],
        connect_timeout=10.0,  # connection setup
        read_timeout=65.0,  # one wait for data; longer than the server's long poll
        close_timeout=10.0,  # client cleanup
        http2=True,
    ) as client:
        orders = client.stream("/orders")
        info = orders.info()
        print("topic exists:", info is not None)
    ```

Why is `read_timeout` so long? A long-poll read keeps its request open on
purpose while it waits for new records. If `read_timeout` were shorter than
the server's long-poll wait, every quiet wait would time out.

With an `http://` endpoint, `http2=True` uses cleartext HTTP/2 and does not
fall back to HTTP/1, so the server must accept HTTP/2 without TLS.

To create tokens and configure the server to check them, see the upstream
[authentication guide](https://picomq.com/docs/operations/auth).

## Restrict redirect targets

In a cluster, the node you connect to may not own the partition you asked
for. It answers HTTP `307` with the owner's address, and the client repeats
the same request there, with your bearer token. `allowed_redirect_origins`
lists the nodes you trust with it:

=== "Async"

    ```python
    import os

    from picomq import AsyncClient

    trusted_owners = {
        "https://pico-a.internal:443",
        "https://pico-b.internal:443",
    }

    async with AsyncClient(
        "https://pico.internal:443",
        token=os.environ["PICO_TOKEN"],
        allowed_redirect_origins=trusted_owners,
    ) as client:
        orders = client.stream("/orders")
        info = await orders.info()
        print("topic exists:", info is not None)
    ```

=== "Blocking"

    ```python
    import os

    from picomq import Client

    trusted_owners = {
        "https://pico-a.internal:443",
        "https://pico-b.internal:443",
    }

    with Client(
        "https://pico.internal:443",
        token=os.environ["PICO_TOKEN"],
        allowed_redirect_origins=trusted_owners,
    ) as client:
        orders = client.stream("/orders")
        info = orders.info()
        print("topic exists:", info is not None)
    ```

With this setting, a redirect to a node outside the list fails instead of
handing your token to an address you never chose.

A node is trusted by its **origin**: its protocol, host, and port, such as
`https://pico.example:443`. The setting takes three forms:

| Value | Trusted origins |
|---|---|
| `None` | Any otherwise valid PicoMQ owner origin. |
| An explicit collection | The endpoint plus the listed origins. |
| An empty collection | The endpoint only. |

Whatever the setting, the client refuses an invalid target and a redirect
from HTTPS to plain HTTP. On a plain HTTP connection, anyone who can watch
the traffic can read the token.

## Supply an HTTPX transport

Supply your own HTTPX transport when you need a proxy, custom TLS, request
logging, or a fake server in tests.

=== "Async"

    ```python
    import httpx

    from picomq import AsyncClient

    transport = httpx.AsyncHTTPTransport(retries=0)

    async with AsyncClient(
        "http://pico.internal:4437",
        transport=transport,
    ) as client:
        orders = client.stream("/orders")
        info = await orders.info()
        print("topic exists:", info is not None)
    ```

=== "Blocking"

    ```python
    import httpx

    from picomq import Client

    transport = httpx.HTTPTransport(retries=0)

    with Client(
        "http://pico.internal:4437",
        transport=transport,
    ) as client:
        orders = client.stream("/orders")
        info = orders.info()
        print("topic exists:", info is not None)
    ```

The client owns the transport once you pass it in: leaving the client block
closes it, so do not reuse it afterward.
