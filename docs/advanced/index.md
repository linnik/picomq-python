# Advanced

Sooner or later an app meets a case the guides do not show. Each question
below has an answer that follows from how the client and the server work:

- Why is a record not stored when `send()` returns? Why must you keep your
  own checkpoint? Why does leaving a producer block wait?
  See [how the client behaves](behavior.md).
- How does a producer keep a retry from being stored twice, and when can it
  not tell? What is an epoch for?
  See [producer delivery and fencing](producer-semantics.md).
- Which HTTP request does each call make, and what does the client check
  before it trusts a response? See [the Pico protocol](protocol.md).
