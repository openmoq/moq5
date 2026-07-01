# local-publish-subscribe

Two MoQ sessions exchange one object in-process, demonstrating the
C++ binding end-to-end with no network stack.

## Build and run

```sh
# From libmoq build tree:
cmake -S examples/cpp/local-publish-subscribe \
      -B /tmp/local-pub-sub \
      -DCMAKE_PREFIX_PATH=/path/to/libmoq/build
cmake --build /tmp/local-pub-sub
/tmp/local-pub-sub/local_publish_subscribe
```

## Expected output

```
server: subscribe request for demo/video
received demo/video object 0: hello moq
```

## What this demonstrates

- **C++ session wrapper** — `moq::session::create` with designated
  initializers, typed `moq::perspective` enum, `result<T>` error
  handling.
- **Typed actions and events** — `poll_action()` returns
  `polled_action` with `visit()`; `poll_event()` returns
  `polled_event` with typed `subscribe_request`, `object_received`
  variants.
- **Zero-copy buffer** — `moq::buffer::create` wraps an rcbuf;
  payload flows from publisher to subscriber without intermediate
  copies.
- **Explicit sans-I/O pump** — the `pump()` helper shows exactly
  how a QUIC adapter would bridge session output to transport input.
  No hidden threads, no event loop, no sockets.
