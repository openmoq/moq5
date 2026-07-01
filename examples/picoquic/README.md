# picoquic Examples

A publisher and subscriber exchange objects over QUIC using picoquic
and the moq_publisher_t / moq_subscriber_t facades. Draft-16
interop target, ALPN `moqt-16`.

## Build

```sh
cmake -B build/pq \
  -DMOQ_BUILD_ADAPTER_PICOQUIC=ON \
  -DMOQ_PICOQUIC_SOURCE_DIR=/path/to/picoquic
cmake --build build/pq
```

If picotls is not adjacent (`../picotls/build`), add
`-DMOQ_PICOTLS_PREFIX=/path/to/picotls/build`.

## Run

Generate test certificates:
```sh
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
  -keyout key.pem -out cert.pem -days 365 -nodes -subj '/CN=localhost'
```

Publisher (terminal 1):
```sh
./build/pq/examples/picoquic/moq_publisher_example cert.pem key.pem 4433
```

Subscriber (terminal 2):
```sh
./build/pq/examples/picoquic/moq_subscriber_example localhost 4433
```

## Expected output

Publisher:
```
Listening on port 4433...
  connection ready, track added
  subscriber joined
  published: g=0 "count=0" [props=6]
  published: g=1 "count=1" [props=6]
  ...
  subscriber update: priority=200
```

Subscriber:
```
Connecting to localhost:4433...
  subscribed - track active
  object: g=0 o=0 "count=0" [props=6]
  object: g=1 o=0 "count=1" [props=6]
  ...
  sent priority update to 200
```

## Key patterns

- **Facade-first**: all subscribe/publish/update logic uses
  `moq_publisher_t` and `moq_subscriber_t`. Raw session and
  transport calls are limited to initial setup.
- **Zero-copy payload**: `moq_rcbuf_wrap` takes ownership of a
  heap-allocated payload without copying. The release callback
  frees it when the last reference is dropped.
- **Per-object properties**: each published object carries a small
  property (type=1, 4-byte counter). The subscriber prints the
  property length.
- **Request update**: the subscriber sends a priority update after
  5 objects. The publisher observes it via `on_subscriber_updated`.
- **Adapter bridge**: `moq_pq_conn_t` bridges the session to
  picoquic with zero-copy SEND_DATA (no flatten buffer).

## Smoke test

```sh
scripts/smoke_picoquic_examples.sh [build-dir] [port]
# defaults: build-dir=build/pq, port=4433
```

Runs both examples, verifies objects received with properties,
priority update sent and received, then cleans up.
