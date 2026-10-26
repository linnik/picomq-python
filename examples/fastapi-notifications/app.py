"""Expose one user's notifications through FastAPI and Server-Sent Events.

The example follows one request from write to live delivery:

1. FastAPI opens one :class:`picomq.AsyncClient` for the application process.
2. A POST request creates the user's stream when needed and appends a JSON
   notification.
3. A GET request reads the stream and keeps waiting for later records.
4. The route turns each record into an SSE event. Its ``id`` is the next PicoMQ
   position, so the browser can send it back as ``Last-Event-ID`` after a
   reconnect.

Authentication and application storage are intentionally outside this example.
The integer path parameter stands in for the authenticated user, and the
browser's last event id stands in for a saved read checkpoint.
"""

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from picomq import BEGINNING, AsyncClient, Position, SseDataEvent


class Notification(BaseModel):
    """The small JSON payload written to a user's notification stream."""

    kind: str
    message: str


def stream_name(user_id: int) -> str:
    """Map a user to one stable, directly addressable PicoMQ stream."""

    return f"/users/{user_id}/notifications"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Own the PicoMQ client for the lifetime of the FastAPI application.

    FastAPI enters this context before serving requests and exits it during
    shutdown. Keeping the client here reuses its HTTP transport and makes
    shutdown close any live PicoMQ reads cleanly.
    """

    async with AsyncClient(
        os.getenv("PICO_ENDPOINT", "http://127.0.0.1:4437"),
        token=os.getenv("PICO_TOKEN"),
    ) as client:
        app.state.pico = client
        yield


app = FastAPI(lifespan=lifespan)


def get_client(request: Request) -> AsyncClient:
    """Return the client created by :func:`lifespan` for dependency injection."""

    return request.app.state.pico


def resume_position(last_event_id: str | None) -> Position:
    """Turn an SSE reconnect id into the PicoMQ position where reading starts.

    A missing id means a first connection, so the example reads from the
    beginning. Invalid ids are rejected before a long-lived response starts.
    """

    if not last_event_id:
        return BEGINNING

    try:
        position = int(last_event_id)
    except ValueError as error:
        raise HTTPException(
            status_code=400,
            detail="Last-Event-ID must be an integer",
        ) from error

    if position < 0:
        raise HTTPException(status_code=400, detail="Last-Event-ID must not be negative")
    return Position(position)


@app.post("/users/{user_id}/notifications")
async def publish_notification(
    user_id: int,
    notification: Notification,
    client: Annotated[AsyncClient, Depends(get_client)],
) -> dict[str, int]:
    """Append one notification and return the positions assigned by PicoMQ.

    Stream creation is idempotent, which lets the first request create the
    user's stream without a separate provisioning step. The response exposes
    both the first position in the append and the next position after it.
    """

    stream = stream_name(user_id)
    await client.create(stream, content_type="application/json")
    ack = await client.stream(stream).append(notification.model_dump_json())
    return {"position": ack.start_position, "next_position": ack.next_position}


async def notification_events(
    request: Request,
    *,
    client: AsyncClient,
    stream: str,
    from_: Position,
) -> AsyncIterator[str]:
    """Convert subscription records into browser SSE events."""
    source = client.stream(stream)
    async with source.subscribe(from_=from_) as events:
        async for event in events:
            if await request.is_disconnected():
                break
            if not isinstance(event, SseDataEvent):
                continue
            for record in event.records:
                yield f"id: {record.next_position}\ndata: {record.body.decode()}\n\n"


@app.get("/users/{user_id}/notifications")
async def stream_notifications(
    user_id: int,
    *,
    request: Request,
    client: Annotated[AsyncClient, Depends(get_client)],
    last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
) -> StreamingResponse:
    """Stream a user's existing and future notifications as SSE events.

    The first connection starts at position zero. Browsers automatically send
    ``Last-Event-ID`` when reconnecting; using that value resumes after the
    last event the browser received.
    """

    stream = stream_name(user_id)
    await client.create(stream, content_type="application/json")
    return StreamingResponse(
        notification_events(
            request,
            client=client,
            stream=stream,
            from_=resume_position(last_event_id),
        ),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
