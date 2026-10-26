# Manage topics

## Create with topic settings

A topic's settings decide how many partitions it has, how long it keeps
records, and what its records must look like. Pass them as keywords when you
create the topic:

```python
created = client.create(
    "/orders",
    content_type="application/json",
    partitions=4,  # 1 to 10000
    ttl_seconds=7 * 86_400,  # or expires_at=<aware datetime>, not both
    retention_ms=86_400_000,  # positive integers
    retention_bytes=1_000_000,
    schema="order-event",  # a registry name; schema_validate needs one
    schema_validate=True,
    sealed=False,  # True creates a topic that rejects appends
)
```

Why does `create` return a flag instead of failing when the topic exists? So
that every instance of your app can call it at startup. The first call
creates the topic and returns `True`. A later call with the same settings
returns `False` and changes nothing. A call with different settings raises
`ConflictError` rather than quietly changing the topic; to change settings
on purpose, see [Change topic settings](#change-topic-settings).

An aware `expires_at` is converted to UTC.

??? note "Differences from the other PicoMQ clients"

    The other PicoMQ clients offer `create` on both the client and the
    stream object. Here it is on the client only, and `client.stream(name)`
    is a value that does no I/O.

## Topic names

A topic name looks like a path, such as `/orders/2026`, because it is one:
the name becomes the URL path of the topic's HTTP requests. That is also why
a few names are refused. Every operation rejects `.` and `..` segments, and a
first segment of `_streams`, `_schemas`, or `_groups`, with `ValueError`
before any I/O.

??? note "Why those names are rejected"

    HTTP would collapse `.` and `..` onto another topic. The server mounts
    `_streams`, `_schemas`, and `_groups` as control routes, so such a name
    would act on a configuration, schema, or consumer group instead.

## Inspect and list

To check whether a topic exists and how far it has grown, call `info()`: it
returns the topic's details, or `None` when there is no such topic. To find
topics, loop over `list()`. Pass a `prefix` to see only the names that start
with it; the client fetches the names one page at a time as you loop.

=== "Async"

    `AsyncClient.list()` is not awaited; `async for` requests each page.

    ```python
    from picomq import AsyncClient

    async with AsyncClient("http://127.0.0.1:4437") as client:
        orders = client.stream("/orders")
        info = await orders.info()
        if info is not None:
            print(info.start_position, info.next_position, info.sealed)

        async for topic in client.list(prefix="/orders/"):
            print(topic.name)
    ```

=== "Blocking"

    ```python
    from picomq import Client

    with Client("http://127.0.0.1:4437") as client:
        orders = client.stream("/orders")
        info = orders.info()
        if info is not None:
            print(info.start_position, info.next_position, info.sealed)

        for topic in client.list(prefix="/orders/"):
            print(topic.name)
    ```

## Change topic settings

Settings can change after creation. `config(name)` returns the current
settings as a `TopicConfig`, or `None` when the topic is missing, and
`update_config(name, ConfigUpdate(...))` changes them. This keeps only the
last day of records:

=== "Async"

    ```python
    from picomq import ConfigUpdate

    config = await client.update_config("/orders", ConfigUpdate(retention_ms=86_400_000))
    print(config.retention_ms)
    ```

=== "Blocking"

    ```python
    from picomq import ConfigUpdate

    config = client.update_config("/orders", ConfigUpdate(retention_ms=86_400_000))
    print(config.retention_ms)
    ```

Both print `86400000`: one day in milliseconds. `ConfigUpdate` changes only
the fields you name, so you never repeat the settings you want to keep:

```python
from picomq import UNSET, ConfigUpdate

ConfigUpdate(retention_ms=86_400_000)  # set one field; the rest stay UNSET
ConfigUpdate(retention_ms=None)  # clear a nullable setting
ConfigUpdate(schema_validate=False)  # turn validation off
ConfigUpdate(partitions=8)  # partitions only grow
ConfigUpdate()  # invalid: an empty update
```

The server allows at most 10000 partitions added per update.

Retention does not delete old records the moment you set it. The server
removes only committed data, and its cache may still return records below the
new start for a while. To see when it has taken effect, watch the partition's
start position, the first record it still keeps:

```python
info = client.stream("/orders", partition=0).info()
info.start_position  # rises as retention removes old records
```

## Partition addressing

Records keep their order only inside one partition, so most calls act on
exactly one. On a single-partition topic you never notice. On a topic with
several, say which partition when you make the stream object:
`orders = client.stream("/orders", partition=N)`. Partition numbers run from
0 through 4294967295.

On a topic with several partitions, a stream object without a `partition`
behaves like this:

| Call | Without `partition` |
|---|---|
| `info()` | Works. Reports aggregate metadata with no positions. |
| `append`, `trim`, `records`, `subscribe`, `producer` | Need a partition. |
| `client.seal(name)` | Rejected. |
| `client.delete(name)` | Removes the whole topic. |

When the partition does not exist, `info()` returns `None`, as for a missing
topic, and the other calls raise `NotFoundError`.

## Trim, seal, or delete

None of these operations can be undone:

| Operation | Effect |
|---|---|
| `stream.trim(before)` | Remove records below `before`. |
| `client.seal(name)` | Reject future appends. |
| `client.delete(name)` | Remove the topic and its records. |

`trim()` takes a position: a record's number in its partition, starting at
zero. Trimming does not renumber the records that remain, so a reader's saved
checkpoint still points at the same record afterward, unless that record was
trimmed.

=== "Async"

    ```python
    from picomq import AsyncClient, Position

    async with AsyncClient("http://127.0.0.1:4437") as client:
        orders = client.stream("/orders")
        new_start = await orders.trim(Position(100))  # first position still available
        final_next = await client.seal("/orders")  # position after the last record
        existed = await client.delete("/orders")  # True if the topic existed

        print(new_start, final_next, existed)
    ```

=== "Blocking"

    ```python
    from picomq import Client, Position

    with Client("http://127.0.0.1:4437") as client:
        orders = client.stream("/orders")
        new_start = orders.trim(Position(100))  # first position still available
        final_next = client.seal("/orders")  # position after the last record
        existed = client.delete("/orders")  # True if the topic existed

        print(new_start, final_next, existed)
    ```

Read the upstream [Topics and streams](https://picomq.com/docs/design/streams) page before
removing production data.

## Schemas

By default a topic accepts any bytes. To enforce a record shape, store a
**schema** in the server's registry and bind it to the topic; the server then
rejects records that do not match:

```python
schema = b'{"type": "object"}'
client.put_schema("order-event", schema, content_type="application/json")
stored = client.get_schema("order-event")  # Schema(body, content_type), or None
client.delete_schema("order-event")  # True if it existed

# Bind it at create time, or later through config:
client.create("/orders", schema="order-event", schema_validate=True)
client.update_config("/orders", ConfigUpdate(schema="order-event", schema_validate=True))
```

Schema names omit the leading slash and follow the same `.` and `..` rule as
topic names. A record the schema rejects raises `ServerResponseError` with
code `schema_violation`. Clearing the binding disables validation.

The server needs a configured `--schema-registry`; see the
[upstream server documentation](https://picomq.com/docs/).
