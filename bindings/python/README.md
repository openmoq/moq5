# moq5 Python endpoint foundation

This first foundation provides a synchronous, caller-driven connection API over
LibMoQ's public service tier. It includes checked configuration, verified
connection setup, wait/state/terminal observations, negotiated wire version,
wake/interrupt, and explicit lifetime management, plus a synchronous media
receiver (track events, owned media objects, a drain observation and an
example receive loop) and a synchronous media sender: configuration and owned
attachment, declaring and removing tracks, writing media objects, ending a
track, an asynchronous completion request, statistics, subscriber-demand
queries, a bounded wait for the write level, and an example publish loop. It
is not a full Python media binding: asyncio, real-media acceptance, CMAF
fragments and a decoder are not part of it, and neither is a graceful finish
or a drain.
Python is not finished by this landing. This Python foundation lands first;
the Android bindings will be integrated above its commit.

## Build and support scope

Normal, GIL-enabled CPython 3.12 is the first tested floor. The private extension
targets `cp312-abi3`; that ABI target alone does not qualify every later Python
runtime or LibMoQ SDK. No guarantees are made for PyPy, subinterpreters,
free-threaded CPython, or Windows in this foundation.

This is an external-SDK source build. Install a compatible LibMoQ service SDK
and its selected backend dependencies first. CMake must be able to resolve
`find_package(libmoq CONFIG REQUIRED COMPONENTS service)` and `moq::service`
from that installation, for example through `CMAKE_PREFIX_PATH`. Build the
Python package with the packaging files in this directory using CPython 3.12
or later. The installed native libraries must also be available to the runtime
loader. Backends are not bundled, and this landing makes no PyPI publication,
self-contained wheel, or general platform support claim.

From the repository root, with the service SDK already installed:

```sh
python3.12 -m venv bindings/python/.venv
bindings/python/.venv/bin/python -m pip install ./bindings/python \
    -C cmake.define.CMAKE_PREFIX_PATH=/path/to/installed-sdk
```

Select the SDK and backend deliberately; this command does not download native
transport dependencies. A shared SDK also needs the normal platform loader
configuration, independently of CMake's package search path.

`moq5.build_info()` returns a fresh dictionary containing `compiled_version`,
`runtime_version`, `python_abi`, and `test_backend`. The version strings identify
the compiled and runtime LibMoQ VERSION values; the other fields identify the
Python ABI target and use of the test fixture. This is not full build provenance
or capability metadata and does not enumerate backends or native dependencies.
These VERSION values are separate from a connection's negotiated MoQ wire
version. A test-backend build is not real transport acceptance.

LibMoQ is licensed under Apache-2.0; see [LICENSE.txt](LICENSE.txt).

## Foundation installation checks

The Python workflow defines Linux/macOS checks with static and shared service
SDKs built without transport adapters, SDK and binding warnings as errors, and
Debug native-bridge ownership/configuration tests using a fake C service
provider. Those tests do not qualify live handshakes or TLS. The workflow also
installs the wheel, sdist, and a plain source archive into separate temporary
venvs and runs the installed smoke against the real SDK outside the source
tree. This workflow is not a hosted-CI acceptance claim until those jobs run.
It does not publish artifacts or qualify media transport.

For an already installed artifact, run the smoke from outside the checkout:

```sh
/path/to/fresh-venv/bin/python -I -W error /path/to/installed_smoke.py \
    --artifact /path/to/moq5-0.1.0.dev0-cp312-abi3-linux_x86_64.whl \
    --expected-package-version 0.1.0.dev0 --expected-sdk-version 0.1.0
```

Use the exact local archive passed to pip for `--artifact`; the smoke checks
its recorded SHA-256, distribution version, installed module paths and hashes,
ABI tag, typed marker, and absence of the private fixture. SDK VERSION strings
must match the explicit expectation. Linux needs `readelf`; macOS needs `otool`
for the installed module's loader-path check. A shared SDK may need a
test-process `LD_LIBRARY_PATH` (Linux) or `DYLD_LIBRARY_PATH` (macOS) pointing to
its installed library directory; the workflow does not bake that path into
the artifacts.

The default smoke expects an SDK without adapters and checks real C `INVAL`
for an invalid URL and `UNSUPPORTED` for valid QUIC and WebTransport URLs. When
using an external SDK with adapters, pass `--skip-backendless-checks` to omit
only the valid-URL attempts. This does not claim transport acceptance. Terminal
raw-bit and type checks exercise Python values; a backend-free refusal cannot
produce a live endpoint terminal snapshot.

## Connect from the caller thread

The installed-package example observes establishment with a bounded overall
deadline and then closes:

```sh
python bindings/python/examples/connect.py moqt://relay.example:4433 \
    --ca-file /path/to/trusted-ca.pem
```

`https://relay.example/moq` selects WebTransport with the default protocol
selection. The example accepts `--timeout-us` for its overall observation
budget; this is separate from the transport-only handshake configuration.
It reports terminal reasons and raw detail codes without treating failures as
clean EOF.

```python
from moq5 import Endpoint, EndpointConfig, WaitResult

config = EndpointConfig(url="moqt://relay.example:4433", ca_file="trusted-ca.pem")
with Endpoint.connect(config) as endpoint:
    result = endpoint.wait(timeout_us=1_000_000)
    print(endpoint.state, endpoint.negotiated_version)
    if result is WaitResult.CLOSED:
        print(endpoint.terminal)
```

`Endpoint.connect(config)` invokes native connect immediately and returns after
creation, while the transport handshake and MoQ establishment continue
asynchronously. It does not wait for readiness. Direct `Endpoint()` construction
is refused. Applications import only `moq5`; native handles stay private.

## Values and outcomes

`EndpointConfig` is a frozen, keyword-only dataclass. `url` is required; the
defaults are `Protocol.AUTO`, `Backend.AUTO`, an empty version offer,
`WtProfile.BACKEND_DEFAULT`, and `handshake_timeout_us=0`. Optional `sni`,
`ca_file`, and `wt_path` default to `None`; `None` and empty strings both select
the native default. Text must be a built-in `str`, must encode as UTF-8, and
must not contain NUL. URL syntax remains the C service's authority.

`versions` accepts a tuple or list of integers and stores a checked immutable
tuple in preference order. An empty tuple means AUTO. Each offered version
must be in `1..2**32-1`; `Version.DRAFT_16` and `Version.DRAFT_18` are named
values, not a promise of compiled support. Unknown nonzero offers reach native
validation without being silently downgraded. Protocol, backend, and WT profile
fields accept their enum members or known integer values. Their numbers, all
named wire versions, states, terminal reasons, and wait results come from C
constants exposed by the compiled bridge.

Native offer-count and support limits still govern validation: the Python
layer checks each offer's range and type, and the native service refuses an
offer list it cannot carry (`INVAL`) or a version it does not support, before
any transport is selected.

`handshake_timeout_us` accepts `0..2**63-1`, where zero leaves the backend's
default. A nonzero value bounds only the QUIC/TLS handshake, not WebTransport
CONNECT or MoQ SETUP, and an unsupported backend rejects it. `wait(timeout_us)`
takes `0..2**63-1` integer microseconds; zero polls. Both durations use the
finite `INT64_MAX` ceiling to avoid overflow in native deadline additions;
there is no infinite-wait sentinel. The example's `--timeout-us` uses the same
range for its observation budget. Numeric fields reject bools, floats,
coercible non-integers, negative durations, and overflow. Wrong types raise
`TypeError`; out-of-range values and unknown configuration enums raise
`ValueError` before native effects.

| Native wait result | Python result | Meaning |
| --- | --- | --- |
| `MOQ_OK` (0) | `WaitResult.WOKEN` | Coalesced activity; re-read state |
| `MOQ_DONE` (1) | `WaitResult.TIMED_OUT` | This wait's timeout elapsed |
| `MOQ_ERR_INTERRUPTED` (-13) | `WaitResult.INTERRUPTED` | Sticky interrupt is set |
| `MOQ_ERR_CLOSED` (-4) | `WaitResult.CLOSED` | Connection is terminal; inspect reason |

A public `wait` is one monotonic deadline served by native wait requests of
at most 250 ms each, and every valid call makes at least one native
observation: an already-expired budget is served by one zero poll, so a
closed, foreign-thread or fork-inherited handle is always refused and an
already-present WOKEN/INTERRUPTED/CLOSED or native failure is never hidden
behind a timeout. A native wait is not interruptible by a signal; Python
signal handlers run on the main thread only between native calls, so a
pending `KeyboardInterrupt` there is honoured within one slice, without
anyone waking or interrupting the endpoint. The 250 ms figure caps each native
wait request; it is not a wall-clock or arbitrary-thread delivery guarantee.
A signal pending when a native wait returns takes precedence over that wait's
result. A timed-out slice neither shortens nor restarts the budget, and the
final slice is the exact remainder, rounded up.

Compare enum members explicitly: `WOKEN` has integer value zero and is false
in a truth test. A timed-out wait does not itself make the connection terminal.
Unexpected native failures raise `MoqError` with the exact signed `.code` and
`.operation` preserved, and retain the native exception as their `__cause__`.
Errors never become EOF or a successful wait.

`endpoint.state` returns an `EndpointState` for known values, otherwise the
unchanged integer. `endpoint.terminal` returns an immutable
`Terminal(reason, detail_code)` snapshot; known reasons are `TerminalReason`
members and unknown reasons remain integers. Only `TerminalReason.CLEAN`
establishes a clean close; `NONE` and unknown reasons are not evidence of
success. `detail_code` retains every unsigned 64-bit raw bit, including
`2**64-1`; transport failures can have detail zero. Some backends encode signed
native errors in these bits. `endpoint.negotiated_version` is an integer, zero
before establishment, and preserves unknown future version numbers.

TLS chain and server-name verification stay enabled. There is no insecure
verification option or Python `ssl.SSLContext` mapping. Native backend defaults
remain authoritative: a picoquic mbedTLS-only SDK requires an explicit PEM
`ca_file`, while backends without CA-file or distinct-SNI support reject those
configurations. Explicit protocol selection overrides a recognized URL scheme;
it does not make an otherwise invalid URL valid. Unsupported backend, version,
profile, or TLS choices fail explicitly through the native service.

## Receiver, track events and descriptions

`Receiver.attach(endpoint, ReceiverConfig(...))` attaches a media receiver to
a live endpoint on the calling thread and retains that endpoint until
`Receiver.close()`: the endpoint's own `close()` is refused by the service
(`MoqError`, `WRONG_STATE`) while a receiver is attached. `ReceiverConfig`
requires an `OverflowPolicy`; namespace parts and the catalog track name are
binary and copied byte-exact. `wait()`, `stats()`, `terminal`, the track
commands and `poll_track()` require the owner thread; only `closed` may be
read elsewhere. To cancel a blocked receiver wait from another thread use
`receiver.endpoint.set_interrupted(True)` or `wake()`.

`poll_track()` returns one `TrackEvent`, or `PollOutcome.EMPTY` (nothing
now) or `PollOutcome.CLOSED` (the event queue is empty AND the receiver is
terminal -- not an aggregate end of stream). It never consults the interrupt
latch. `TrackEvent.track` is an opaque `Track` whose identity is the native
handle: the same handle yields the same `Track` while any reference exists,
and two tracks with identical descriptions are still distinct.
`CATALOG_READY` carries no track or description.

`TrackEvent.description` and `Track.description` are owned Python copies:
every span and array is `bytes`, nothing borrows native memory, and a copy a
caller retains is never mutated and survives `close()`. The description is
read through the service's sized current-description copy under the receiver
mutex at poll time, so `description.vod` (`VodState.is_live`,
`track_duration_ms`) is the track's CURRENT live/VOD state, not the state at
the moment the event was queued. Optional values the catalog did not carry,
or that the native stamped prefixes (event, description, nested `info` and
`init`) do not cover, are `None`, never zero; unknown enum values are kept as
integers. `TrackEvent.largest` / `expires_ms` are `UPDATE_OK` observations,
`parse_drop` a coalesced `PARSE_DROP` snapshot.

A native poll consumes the event. If the bridge cannot convert it,
`EventLost` (a `BindingError`) names the validated kind and the failing
stage and carries no address; nothing about the track or its cached
description changes, and the next poll proceeds. An allocation failure is
the bare `MemoryError`. `MoqError` is raised only for a signed result the
service actually returned.

## Media objects

`poll_object()` dequeues one `MediaObject`, or `PollOutcome.INTERRUPTED`
while the endpoint's sticky latch is set (checked before the queue and
before terminal), `PollOutcome.CLOSED` when the object queue is empty AND
the receiver is terminal, or `PollOutcome.EMPTY`. It makes exactly one
native poll and never waits; the owner-thread, same-process and liveness
checks run before anything is consumed. Track events and objects are two
queues: drain `poll_track()` before `poll_object()` on each wakeup.

A successful native dequeue transfers the object's buffers to the binding.
The bridge copies every byte and sample record into Python-owned values
(`bytes`, a tuple of `CmafSample`), releases the transferred buffers exactly
once on the polling thread, and only then returns. Nothing borrows native
memory: a `MediaObject` survives `Receiver.close()` and `Endpoint.close()`,
may be read from any thread, and is immutable (`dataclasses.replace` copies
it field for field). RAW media is `payload` with `fragment` empty; CMAF media
is the whole `fragment` with the mdat slice at `mdat_offset` / `mdat_len`
and `samples` in the track's timescale ticks (`payload` empty). Widths and
signs are the C ones: unsigned 64-bit timestamps, a signed
`composition_offset_us`, unsigned 32-bit sample fields with a signed
`composition_offset`; `capture_time_us` is `None` when absent.
`config_generation` is carried as reported.

`status` decides `is_status_only`, never byte length: a zero-length NORMAL
object is media with `payload == b""`; END_OF_GROUP / END_OF_TRACK carry no
media. Unknown `status` and `packaging` values are preserved as integers
and their bytes are not interpreted; that is a contract for future values,
not a claim that the current native parser accepts them.

`MediaObject.track` is the same cached `Track` a track event yields for that
handle, whichever arrives first, and is described from the track's CURRENT
state at poll time (not the state when the object was queued). A new
`Track`, or a replaced `Track.description`, is published only after the
whole object converted.

A consumed object the bridge cannot convert raises `ObjectLost` (a
`BindingError`): `stage` names the failing field or bound (`payload`,
`fragment`, `samples`, `bounds:mdat_offset`, `bounds:mdat_len`, `status`,
`struct_size`, `track`, `track.description`), and `presentation_time_us`,
`status`, `packaging` are carried only when they were validated before that
stage. Its buffers were released exactly once; no address is carried; the
cache and any prior description are untouched; the next poll proceeds. An
allocation failure during conversion is the bare `MemoryError` -- the object
was consumed and no per-object metadata is promised. `MoqError` is raised
only for a signed result the service actually returned.

## Drain and the receive loop

`Receiver.drained()` is an observation over the two streams this binding
exposes: it is true only when the most recent `poll_track()` AND the most
recent `poll_object()` each returned `PollOutcome.CLOSED`. It starts false,
never polls, waits, clears the interrupt latch, or infers anything from
terminal state, stats or queue counts. Any newer non-CLOSED outcome on a
stream (an item, EMPTY, INTERRUPTED, a native error, or a consumed-item
conversion failure) withdraws that stream's evidence while the other
stream's is kept. It requires the owner thread, process and a live
receiver; a refused caller changes nothing. Close is not drain. The native
SAP and media-timeline queues are not exposed here and are not covered.

`examples/receive_loop.py` is a handler-driven loop over this surface: it
drains every track event and every media object through the same handlers,
subscribes cmaf/loc tracks as they are added, blocks on `wait()` only when
nothing is queued, and distinguishes transport drain, a requested
interruption, and finite broadcast completion. Completion is the loop's own
policy over what the facade observes (MSF-01 section 11.3): every wanted
track reached TRACK_ENDED (which includes rejection) and either every wanted
track's description became non-live WITH a track duration, or the catalog
latched isComplete with every wanted track removed. It is not proof of the
publisher's wire sequence. A finite receiver campaign against our own relay on
loopback has been run, with an independent C publisher and declared synthetic
RAW/LOC payloads; acceptance against real media remains a later slice.

The package requires a LibMoQ service SDK that declares
`moq_media_receiver_track_desc_copy`; building against an older SDK fails
at compile time rather than falling back to the borrowed descriptor.

## Sender shell

`Sender.attach(endpoint, SenderConfig(...))` attaches a media sender to a live
endpoint and retains it, exactly as the receiver does: `sender.endpoint is ep`,
and the endpoint's own `close()` is refused by the service (`MoqError`,
`WRONG_STATE`) while a sender is attached. `Sender()` is refused; attachment is
the only constructor. Every fallible Python step of the attachment completes
before the native call, so a successful native attachment is never stranded by
a later Python failure, and a refusal retains nothing.

`SenderConfig` is frozen and keyword-only. It copies its namespace parts and
catalog track name, keeps the native field widths, and leaves every unset field
at the service default. `Backpressure` is the closed set of the four native
policies; `UNSET` is refused. `SenderConfig.live()` and `SenderConfig.lossless()`
are presets over the same fields, and `CATALOG_REFRESH_DISABLED` is the native
sentinel that turns periodic catalog refresh off.

`sender.ready` means the namespace was accepted and the catalog published, so
the publish path can accept media. It does NOT mean anyone subscribed: demand
is a separate observation, exposed by `subscriptions()`, `has_subscriber()` and
`has_media_subscriber()` and described below. `sender.terminal` copies the
sender's closed/fatal/fatal_code plus the endpoint's terminal snapshot; `fatal`
includes an endpoint failure and `fatal_code` falls back to the endpoint's code
when the sender itself is not fatal. A native terminal is not closure:
`sender.closed` reports only that this owner released the sender.

`Sender.close()` is DESTRUCTIVE and idempotent. It detaches immediately and
discards whatever the service had queued. It is not a flush and it does not
complete a broadcast. Ownership follows the endpoint rules below: the attaching
thread owns every operation except `closed`, and a handle inherited across
`fork` is refused rather than used.

Tracks are described in the next section and writing media objects in the one
after. There is no graceful finish or drain. A finite publishing run has been
performed on loopback against our own relay -- eight declared synthetic RAW/LOC
objects, observed exactly by a real receiver, with that track's terminal event
seen -- which is receipt evidence for that run, not a delivery, flush or
completion guarantee.

## Tracks

`Sender.add_track(SendTrackConfig(...))` declares a track and returns a
`SendTrack`; `Sender.remove_track(track)` removes it. Both preserve the
service's signed result codes. The one special case is a retryable add: while a
just-removed name, or a generated `<name>.sap` or `<name>.timeline` sibling, is
still held by a teardown that has not finished, the service returns
WOULD_BLOCK and the binding raises `TrackNameBusy`, which is a `MoqError`.
Retry later.

`SendTrackConfig` is frozen and keyword-only. It requires `name`,
`media_type`, `packaging`, `codec` and `bitrate`, and accepts `timescale`,
`init_data`, `role`, `lang`, `is_live`, `width`, `height`, `framerate_millis`,
`samplerate`, `channel_config` and `track_duration_ms`. Every span is bytes and
is kept byte-exact, embedded NULs included; nothing is sniffed or parsed. A
`timescale` of 0 selects the service default. MSF-01 requires a codec and a
maximum bitrate for audio and video, and a sample rate and channel
configuration for audio; a live track must not declare a duration. Those rules
are mirrored here, so a malformed track is refused before the service sees it.

The NATIVE sender owns its tracks until it is destroyed. Dropping a `SendTrack`
removes nothing and ends nothing: there is no per-track native destructor and
no implicit removal on collection. A live track keeps its sender, and through
it the endpoint, alive. A removed track keeps a valid but inert handle until
the sender is destroyed, so its `name`, `sender` and `removed` stay readable.
`removed` reports removals made through this binding -- this track's own
`remove_track`, and an accepted `request_complete()`, which the service
answers by marking every track the sender then had removed.
Removing twice is refused with WRONG_STATE. `Sender.close()` remains
destructive and is not a flush or a completion barrier.

`SendTrack` is returned by `add_track` and cannot be constructed directly. Its
identity is its own: two tracks are distinct keys, and hash and equality do not
change when a track is removed or when its sender is closed. There is no track
registry.

Track CONFIGURATION fields that this binding does not expose are sent absent
by `SendTrackConfig`: the generated SAP and media timelines,
content-protection reference ids, the CMSF maximum SAP starting types and the
alternate group.

Sender METHODS not in the binding yet -- a different thing from an unexposed
field -- are a graceful finish and a drain. Writing objects, ending a track,
requesting completion, statistics and the demand queries are described in the
sections below.

## Writing media objects

`Sender.write(track, SendObject(...)) -> WriteOutcome` submits one object, in
decode order, with exactly one synchronous native call per invocation.

`SendObject` is frozen, slotted and keyword-only. `payload` is required and is
immutable bytes; `b""` is a VALUE and is passed as a present zero-length
buffer, while omitting it is an error. `properties` is the CMAF property block,
passed through byte-exact: `None` is ABSENCE and `b""` is a present empty
block, and the two are never collapsed -- the service requires properties to be
absent on a RAW track, where it owns the LOC block itself. `is_sync`,
`starts_group` and `ends_group` are strict bools. `decode_time_us` is advisory
in v0, because per track write order IS decode order. `presentation_time_us`
drives the generated timelines. `capture_time_us` and `sap_type` are optional:
`None` is absence and an explicit value is a declaration, so `sap_type=SapType.NONE`
declares "not a SAP", which is not the same as declaring nothing.
`capture_time_us` is the LOC Capture Timestamp (LOC-01 2.3.1.1) -- wall-clock
microseconds since the Unix epoch, emitted verbatim; its encodable range
follows the negotiated codec and is enforced by the service, not clamped here.

Spans are bytes only. `bytearray`, `memoryview`, `str` and `int` are each
refused, so nothing mutable is held across the call. Nothing is parsed,
sniffed or generated: no codec inference, no second LOC writer.

`SapType` offers `NONE`, `TYPE_1`, `TYPE_2` and `TYPE_3`. The native internal
sentinel `UNKNOWN` is deliberately absent, because the service rejects it.

`WriteOutcome` carries the native codes: `ACCEPTED` (0), `WOULD_BLOCK` (-8),
`INTERRUPTED` (-13) and `CLOSED` (-4). **ACCEPTED means accepted by the
service, not delivered**: under a drop policy an accepted object may still be
evicted, and there is no receipt in this surface. WOULD_BLOCK is backpressure
-- a full queue under RETURN_WOULD_BLOCK, an expired BLOCK_TIMEOUT, a missing
sync anchor, or a GOP larger than the bound under a drop policy. INTERRUPTED
means the endpoint's interrupt latch is set; CLOSED means terminal. None of the
three took ownership of the object, and none is retried for you. Any other
native result is a `MoqError` keeping its signed code and the operation
`"write"`.

Ownership is transfer-on-success. The binding copies each span into a
refcounted buffer before the call; on ACCEPTED those buffers belong to the
service, and on any other result they are released before a Python result or
error is built. A failure to build the result cannot strand a buffer.

**Signals.** The GIL is released only around the native call. A signal
delivered while it is in flight reaches the caller after ownership has been
settled, so it can never strand a buffer -- but it does NOT certify that the
object was refused. If the service accepted the object, the transfer stands.
Do not retry blindly on `KeyboardInterrupt`: a second `write` is a NEW object.
`stats()` reports aggregate counters, not a per-write acknowledgment, so it
cannot tell you whether THAT object was accepted.

Guards are the same as everywhere else: the owning thread only, handles
inherited across `fork` refused, a closed sender refused, and a track that
belongs to another sender -- or a receiver `Track` -- refused before any native
effect. `close()` stays destructive: it discards whatever was queued.

## Ending one track

`Sender.end_track(track) -> WriteOutcome` requests that one track be ended,
with exactly one synchronous native call per invocation.

ACCEPTED means the service accepted the REQUEST. After this track's queued
objects drain it emits a reliable END_OF_TRACK, which receivers see as a
track-ended event. It does NOT mean the terminal has been emitted, that prior
media was flushed, or that anything was delivered. WOULD_BLOCK means the send
queue was momentarily full and commits nothing -- retry it yourself, because
the binding never retries for you. INTERRUPTED and CLOSED commit nothing
either. Any other native result is a `MoqError` keeping its signed code and
the operation `"end_track"`; a REMOVED track is refused that way, since
removal is not ending.

Idempotence belongs to the SERVICE: this binding keeps no ended state, so a
repeat is simply a second call, and the service answers OK without queuing a
second terminal. Because it checks its interrupt and terminal conditions
BEFORE that idempotent no-op, a repeat after the sender terminalizes is
CLOSED, not success -- do not read "idempotent" as "always succeeds".

Ending removes nothing, destroys nothing and closes nothing. The track keeps
its handle, name and owner; `removed` stays False; other tracks are untouched;
and the broadcast and endpoint stay up. A later `write` on an ended track is
refused by the SERVICE with WRONG_STATE, not by a Python latch. This is not a
broadcast completion -- that is `request_complete()`, below -- and not a
graceful finish, which does not exist.

A signal delivered while the call is in flight reaches the caller after the
native result is settled, so it does not undo a request the service already
accepted.

## Requesting broadcast completion

`Sender.request_complete() -> None` asks the service to terminate the
broadcast permanently (MSF-01 section 11.3). It is a REQUEST: a successful
return means the service ACCEPTED it, and nothing more.

What happens synchronously is one thing only -- the service marks every track
this sender has as removed, so `SendTrack.removed` answers True immediately
afterwards. Everything a receiver would see happens later, on the service's
own network thread: each active track is ended reliably (END_OF_TRACK), and
then a terminal INDEPENDENT catalog generation carrying `isComplete` and an
empty track list is published to active subscribers and retained, so a later
joiner still learns the broadcast completed. A successful request is NOT
emitted, flushed, acknowledged or received completion.

The queued media of those removed tracks is discarded by the service as it
tears them down, and counted there -- so statistics read around this call may
move, because the service's own thread is running. An unchanged snapshot is
not a promise, and there is no queue-empty completion predicate anywhere in
this binding.

Completion is legal only once the broadcast is ready; the service answers a
pre-ready request with WRONG_STATE. Idempotence belongs to the SERVICE, and
this binding keeps no completed state, so a repeat is simply a second call
that the service answers. Because it checks its interrupt latch and terminal
condition BEFORE that idempotent no-op, a repeat after either is a refusal,
not a courtesy success.

Every non-OK native result is a `MoqError` keeping its signed code and the
operation `"request_complete"`. There is no retryable outcome here and
nothing is retried. A signal delivered while the call is in flight reaches
the caller after the native result is settled: it does not undo a request the
service already accepted, and the tracks still report the removal.

After an accepted request the SERVICE refuses `add_track`, `write`,
`end_track` and `remove_track` with WRONG_STATE; the handles stay valid but
inert, keeping their name and owner. The namespace and the endpoint stay up:
this is not `close()`. `close()` and leaving a `with` block remain
DESTRUCTIVE -- they detach and discard whatever was still queued -- and this
binding offers no flush, drain or graceful finish to pair with them.

## The publishing example

`examples/publish_loop.py` is a caller-driven finite submission loop over the
sender surface:

    record = publish(sender, plan, now_us, deadline_us,
                     WriteOutcome, WaitResult, MoqError, note_dropped,
                     wait_us=250_000, require_demand=False)

`plan` is a FINITE ordered sequence of `(track, SendObject)` pairs. The loop
holds the exact object it was given until that submission is ACCEPTED,
advances only then, and never submits an accepted object again; a
WOULD_BLOCK is retried after a wait, and INTERRUPTED, CLOSED or any other
error ends the attempt rather than looping. A KeyboardInterrupt on the way
out of an accepted write is ambiguous and is never resubmitted.

The CALLER owns the endpoint, the sender and the tracks on both sides of the
call. The loop never closes or destroys an owner, never adds or removes a
track, never requests completion, never drains, and never starts a thread or
a process -- `close()` discards queued work and an accepted completion
request removes every track, so either would turn accepted work into lost
work. Those remain your decision, after the record has been read.

`deadline_us`, read through the injected `now_us`, is a COOPERATIVE
submission budget: it bounds how long the loop keeps attempting and clips
every wait to `min(wait_us, remaining)`. It cannot cancel a native call
already in progress or your own callback, so a BLOCK_TIMEOUT sender can spend
its whole block timeout inside ONE write, past the remaining budget. Use
RETURN_WOULD_BLOCK for a bounded run.

`note_dropped` receives the increment between OBSERVATIONS whenever the named
drop counters advance, and the final observation reports its remainder once;
the record's `dropped` aggregate and its `stats` come from that same final
snapshot. Accepted is not delivered, and no lossless guarantee follows from a
policy or from zero observed drops. `require_demand` is opt-in and finite,
and `has_media_subscriber()` is the service's AGGREGATE question, so an
unrelated subscribed track satisfies it; readiness is never queried, because
writing pre-ready is legal and bounded by the pre-ready caps.

The record carries `stop` ("submitted", "deadline", "interrupted" or
"closed"), `submitted`, `attempts`, `pending`, `would_block`, `ended`,
`ends_refused`, `interrupted`, `dropped`, `terminal` and `stats`. It is a
SUBMISSION report: `stop == "submitted"` means every planned object was
accepted and every end requested (accepted, or refused with the one named
track-local WRONG_STATE). It is not emission, flush, acknowledgment or
receipt; it says nothing about END_OF_TRACK reaching a peer or a catalog
being published; and it is not a completion predicate. The finite loopback run
described above used this example; it establishes what that run observed, not a
general guarantee.

## Waiting for the write level

`Sender.wait(timeout_us) -> WaitResult` blocks until this sender can accept
another write, the endpoint wakes, or the timeout elapses. It is sliced
exactly like `Endpoint.wait` and `Receiver.wait`, against ONE monotonic
deadline: bounded native slices, the remainder rounded up, and an exhausted or
zero budget still making exactly one observation. `timeout_us` is an integer
(not a bool) from 0 to INT64_MAX.

The service's priority applies unchanged, and this binding does not reorder
it: **the interrupt latch wins, then terminal state, then the write level** --
because `write()` refuses the first two, so reporting "write now" there would
be a lie.

**WOKEN is advisory.** It means "try another write now". It is also what an
endpoint wake reports even when the level does not hold, so it is NOT
subscriber demand, NOT a promise that the next write will be accepted, and not
completion, drain or flush -- another thread may fill the queue first, a
single object may exceed the byte budget, and a drop-policy sender still
reports per-object outcomes. `TIMED_OUT` means the budget elapsed with the
level not holding, `INTERRUPTED` means the endpoint's latch is set, and
`CLOSED` means the sender or endpoint is terminal. Any other native result is
a `MoqError` keeping its signed code and the operation `"wait"`.

Waiting has no side effects: it does not clear the interrupt latch, write, end
or remove a track, close anything, or change the drop policy. A live sender
that is merely TERMINAL still answers with CLOSED; a closed owner is refused
before the service is entered. The GIL is released only around the native
call, and a signal delivered during it reaches the caller afterwards.

## Subscriber demand

Three independent point-in-time queries:

    Sender.subscriptions(track) -> int
    Sender.has_subscriber(track) -> bool
    Sender.has_media_subscriber() -> bool

`subscriptions` is the service's mirrored count for that exact track, as an
ordinary nonnegative int covering the whole native range -- nothing is
narrowed or clamped. `has_subscriber` is the service's own `count > 0`, asked
of the service rather than recomputed here. `has_media_subscriber` is the
"should I encode?" question: ONE native observation over app-visible media
tracks, never a sum over Python wrappers.

The internal catalog track, the generated SAP and media-timeline tracks and
removed tracks are excluded from `has_media_subscriber` by the service, so a
**catalog-only subscription does not make it true**. A generated track's exact
handle, if you hold one, still reports its own count.

Demand and readiness are separate: demand without `ready`, and `ready` without
demand, are both ordinary, and neither gates the other.

A REMOVED track reports what the service reports -- its mirrored count until
teardown has drained it, then zero. The binding applies no removed latch and
forces no immediate zero.

Nothing here is cached, and **a count is not a promise about a later write**:
demand can change between the query and the call, and the write result is
always the service's own answer. These are not events or callbacks, there is
no subscription registry, and no query has any lifecycle effect.

Guards are the same as everywhere else: the owning thread only, handles
inherited across `fork` refused, a closed sender refused, and a track owned by
another sender -- or a receiver `Track` -- refused before any native effect. A
live sender that is merely TERMINAL still answers; the native getters return
zero for a foreign handle, but this API refuses one rather than answering.

## Statistics

`Sender.stats() -> SenderStats` copies a snapshot. Each call takes a NEW
native snapshot: nothing is cached, computed in Python, reset or repaired, and
no native pointer is retained. A live sender answers even when it is terminal
-- terminal is not closed -- while a closed owner is refused.

`SenderStats` is a frozen, slotted value with ten counters plus a signed
`last_error`: `objects_written`, `objects_sent`, `objects_queued`,
`bytes_queued`, `objects_dropped`, `groups_dropped`, `keyframes_dropped`,
`groups_abandoned`, `backpressure_stalls`, `last_error`, and the appended
`sap_records_evicted`.

`sap_records_evicted` is `None` when the service's stamped prefix does not
cover that WHOLE field -- absence, never a fabricated zero. A stamped zero is
a present zero, and the two are not equal.

What the numbers mean, as the service defines them:

* `objects_written`, `objects_sent`, `objects_queued`, `objects_dropped` and
  `bytes_queued` are ONE population: media accepted by `write()` and the bytes
  it holds. The private END_OF_TRACK marker `end_track()` queues is not media
  and appears in none of them. So **`objects_queued == 0` means no media is
  pending -- it is not a completion barrier**, and a terminal may still be in
  flight.
* **`objects_sent` means handed to the session, not delivered to a receiver.**
* `groups_dropped` counts distinct (track, group) values that lost at least
  one queued object; `groups_abandoned` counts open wire subgroups driven
  through the RESET lifecycle. They are different facts, and one partially
  emitted group can contribute to both.
* `last_error` is the last non-OK `write()` return, 0 for none. It is DATA: a
  recorded historical failure is reported here, never raised.

Within that population the service maintains
`objects_written == objects_sent + objects_queued + objects_dropped`. The
binding reports it; it does not check, recompute or repair it.

A native failure keeps its signed code and the operation `"stats"`. A
malformed successful output -- a stamp outside the frozen v0 floor and the
capacity offered -- is a `BindingError`, not a native result.

## Ownership and close

The thread calling `connect` owns ordinary endpoint operations, including
context entry and `close`. The binding adds no worker thread; native transport
execution stays in the service tier. Native waits release the GIL and native
callbacks do not invoke application Python.

Only `wake()`, `set_interrupted(exact_bool)`, and the `closed` property may be
called from another thread. Native lifetime checks still apply. Wake requests
a coalesced service cycle; it does not clear the sticky interrupt latch.
`set_interrupted(True)` makes waits report INTERRUPTED until
`set_interrupted(False)` clears it. Interruption does not close the connection.
Inherited native handles are rejected after `fork`; create new endpoints in
the child.

`endpoint.closed` means the native resource has been released, not that the
connection has reached `EndpointState.CLOSED`. Read state, terminal, and version
before explicit close, and keep copied terminal snapshots as needed. `close()`
stops, joins, and releases native resources synchronously and is idempotent.
It provides no graceful media completion or drain guarantee.

Use `with Endpoint.connect(config) as endpoint:` for deterministic cleanup.
Normal exit propagates any body exception after closing. If cleanup alone
fails, its exception propagates. If both the body and cleanup fail, an
`ExceptionGroup` contains the body exception first and cleanup failure second;
it is a `BaseExceptionGroup` when necessary to retain `KeyboardInterrupt` or
another `BaseException`. Both existing exception causes remain intact. A
forgotten explicit close triggers the native capsule's stop/join fallback and
`ResourceWarning`; finalization is not the normal lifetime contract.
