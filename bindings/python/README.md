# moq5 Python endpoint foundation

This first foundation provides a synchronous, caller-driven connection API over
LibMoQ's public service tier. It includes checked configuration, verified
connection setup, wait/state/terminal observations, negotiated wire version,
wake/interrupt, and explicit lifetime management. It is not a full Python media
binding: publishing, receiving, asyncio, and drain/completion are not exposed.
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
