# FastAPI notifications

This small app stores each user's notifications in a separate PicoMQ stream,
which is a named, ordered log. A FastAPI GET endpoint sends existing
notifications and then waits for new ones as Server-Sent Events. When the
browser reconnects, its `Last-Event-ID` header tells the app where to resume.

Start the [PicoMQ Rust server](https://picomq.com/docs/quick-start) in a
separate terminal with its local defaults:

```bash
pico serve
```

Then start this FastAPI server from this directory:

```bash
uv run uvicorn app:app
```

Open the stream in one terminal:

```bash
curl -N http://127.0.0.1:8000/users/42/notifications
```

Publish a notification in another:

```bash
curl -X POST http://127.0.0.1:8000/users/42/notifications \
  -H 'content-type: application/json' \
  -d '{"kind":"welcome","message":"Hello"}'
```

Set `PICO_ENDPOINT` and `PICO_TOKEN` when the server is not using the local
defaults.
