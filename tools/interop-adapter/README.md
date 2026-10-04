# Interop-runner adapter for the libmoq publisher

`run.sh` lets `moq-contribution-interop-runner` (sibling checkout) start
`examples/service/media_send` as a contribution publisher and score what it puts
on the wire against draft 18 or draft 21. The checked-in drafts, not this
publisher and not the runner, decide what is compliant.

## Use

Build the publisher, then start the runner with this adapter as its driver:

```sh
cmake --build build/dev --target moq_example_media_send
export MOQ5_MEDIA_SEND_BIN=$PWD/build/dev/examples/service/moq_example_media_send
../moq-contribution-interop-runner/build/moq-interop-runner \
  --bind 127.0.0.1 --port 8080 --database work/runs.sqlite3 \
  --publisher-bind 127.0.0.1 --publisher-advertise 127.0.0.1 \
  --publisher-port-start 4443 --publisher-port-end 4452 \
  --tls-cert work/cert.pem --tls-key work/key.pem \
  --driver-executable $PWD/tools/interop-adapter/run.sh \
  --driver-fixture /path/to/any/readable/file --driver-log-root work/drv-logs
```

Then create driven runs with `POST /api/v1/runs` as described in the runner's
`docs/publisher-harness-guide.md`. Use `"transport": "native-quic"`.

`test-adapter.sh` checks the request-to-command-line mapping and the refusals
without any network (a capture stub replaces the publisher); it is registered with
ctest as `interop_adapter_contract` when `jq` is installed.

## What it maps

| Request field | media_send argument |
|---|---|
| `endpoint` | URL (`moqt://` native QUIC, `https://` WebTransport) |
| `namespace_hex` (fields, joined with `/`) | namespace |
| `track_name_hex` | track name |
| `draft` (18 or 21) | `--draft N` (an exact offer, never a fallback) |
| `tls_ca` | `--ca FILE` |
| `scenario_timeout_ms` | `timeout --signal=INT` (ceiling, in seconds) |

A malformed or unsupported request exits 64 before the publisher starts.
Reaching the scenario timeout is the normal end of a context and exits 0.

## Limits worth knowing

- **The fixture is ignored.** `media_send` publishes placeholder access units in
  GOPs of 30, about 10 seconds at 30 fps. A scenario that needs a specific object
  layout (for example "two small groups") will not get it from this publisher.
  Score such rows as publisher gaps, then decide whether to add a mode.
- **No per-scenario modes yet.** The moqxr adapter passes scenario-specific
  options so its publisher originates or withholds messages. This adapter does
  not; the roughly 100 draft-21 probes in which the runner acts as the subscriber
  score whatever `media_send` does by default.
- **WebTransport does not connect to the runner from the `dev` build.** The runner
  admits only the current WebTransport profile (`webtransport-h3`, HTTP/3
  WebTransport drafts 15/16). The picoquic WebTransport backend speaks the legacy
  dialect, so the handshake ends in a transport failure (terminal reason 5) before
  any MoQT bytes. Only the `wtquic-msquic` backend offers the current profile, and
  no existing build tree enables it. Native QUIC is unaffected.
- **Draft 21 does not connect until the d21 profile lands** (`connect failed: -14`,
  unsupported). The run then ends as "publisher exited before connecting", which
  is the intended baseline.
