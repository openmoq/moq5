# mvfst Managed Client

Optional managed-mode C API for connecting to MoQ relays via the
mvfst QUIC transport. Requires `MOQ_BUILD_ADAPTER_MVFST=ON`.

## Overview

The managed client owns the network thread, EventBase, mvfst
QuicClientTransport, moq_session_t, and the adapter bridge. The
application drives protocol logic inside an `on_lane_pump` callback
and uses `wake()`/`wait()` for cross-thread signaling.

```
App thread                   Managed network thread
─────────                    ──────────────────────
                             create session
                             create transport
                             start QUIC handshake
                             attach adapter

  wake() ──────────────────→ EventBase.loopOnce()
                             adapter.service(now)
                             on_lane_pump(session)
  wait() ◄──────────────────  signal_activity()

  stop() ──────────────────→ teardown adapter
                             close transport
                             destroy session
  destroy()
```

## API

```c
#include <moq/mvfst.h>

moq_mvfst_managed_cfg_t cfg;
moq_mvfst_managed_cfg_init(&cfg);
cfg.perspective  = MOQ_PERSPECTIVE_CLIENT;
cfg.host         = "relay.example.com";  /* hostname or numeric IP */
cfg.port         = 4433;
cfg.insecure_skip_verify = true;  /* or: cfg.cert_path = "/path/to/ca.pem"; */
cfg.on_lane_pump      = my_pump;
cfg.user_ctx     = &my_state;
cfg.send_request_capacity = true;
cfg.initial_request_capacity = 16;

moq_mvfst_managed_t *m;
moq_mvfst_managed_create(&cfg, &m);

while (!done) {
    moq_mvfst_managed_wake(m);
    moq_result_t rc = moq_mvfst_managed_wait(m, 500000);
    if (rc == MOQ_ERR_CLOSED) break;
    /* process app-side results */
}

moq_mvfst_managed_destroy(m);  /* calls stop() internally */
```

## Session access

`moq_mvfst_managed_session()` returns non-NULL **only when called
from the managed network thread** (inside `on_lane_pump`). Calling it
from the app thread always returns NULL. All `moq_session_*` calls
must happen inside `on_lane_pump`.

## Pump callback

```c
int my_pump(moq_mvfst_managed_t *m, moq_mvfst_managed_lane_t *lane,
            uint64_t now_us, void *ctx) {
    moq_session_t *s = moq_mvfst_managed_session(m);
    if (!s) return 0;

    /* subscribe, poll events, write objects, etc. */

    return 0;  /* 0 = continue, nonzero = request shutdown */
}
```

The pump runs after `adapter.service(now)`, so inbound data is
already processed and outbound actions are drained before the
callback fires.

## Lifecycle

| Function | Thread | Notes |
|----------|--------|-------|
| `create` | app | Spawns thread, blocks until init succeeds/fails |
| `stop` | app | Joins thread, idempotent. Returns `MOQ_ERR_INVAL` if called from pump |
| `destroy` | app | Calls `stop()` internally, then frees the client session. `destroy(NULL)` is no-op |
| `wake` | any | Thread-safe, coalesced |
| `wait` | app | Returns `MOQ_OK` (activity), `MOQ_DONE` (timeout), `MOQ_ERR_CLOSED` |
| `session` | pump | Returns NULL from wrong thread or after stop (the session object itself stays allocated until `destroy`, so a service-tier consumer can still tear down after a fatal) |
| `is_fatal` | any | Atomic read |
| `fatal_code` | any | Atomic read |

## QUIC transport tuning

| Field | Default | Description |
|-------|---------|-------------|
| `max_num_ptos` | 0 (mvfst default: 7) | Max consecutive PTOs before connection close |
| `initial_rtt_us` | 0 (mvfst default: 50000) | Initial RTT estimate in microseconds |

Set `max_num_ptos = 2` and `initial_rtt_us = 10000` for fast
connection failure detection in tests.

## Lifecycle-only mode

If `host` is NULL or empty, no transport or adapter is created.
The pump loop runs with a session-only EventBase-free loop,
useful for lifecycle tests that don't need a server.

## TLS verification

Three mutually exclusive modes:

| Mode | Config | Checks |
|------|--------|--------|
| Insecure | `insecure_skip_verify = true` | None (testing only) |
| Custom CA | `cert_path = "/path/to/ca.pem"` | Chain + host/IP identity |
| System default | neither set | System trust store + host/IP identity |

Both secure modes verify:
1. **Chain trust** — the server certificate chains to a trusted CA
   (PEM file or system trust store).
2. **Host identity** — the server certificate contains an IP SAN
   or DNS SAN matching `cfg.host`.

`insecure_skip_verify` and `cert_path` are mutually exclusive;
`create()` returns `MOQ_ERR_INVAL` if both are set.

```c
/* Local testing — accept any cert */
cfg.insecure_skip_verify = true;

/* Dev/staging — trust a specific CA and verify host identity */
cfg.cert_path = "/etc/moq/relay-ca.pem";

/* Production — system trust store + host identity */
/* (leave both insecure_skip_verify and cert_path unset) */
```

`create()` returns `MOQ_ERR_INVAL` if `cert_path` is set but the
file is not readable.

## Building

### Corrected mvfst dependency input

`scripts/setup_mvfst_deps.sh` materializes only mvfst, from a caller-supplied
offline copy of the official `v2026.05.25.00` archive (commit
`d48af283a4d14583382288bd57b7093ffb80dbf0`). The required archive SHA256 is
`ec88bcabc7f8689348ad7b3a5ee4313f0dd196df16959e16ea62ab6cbd46d337`.
It does not fetch, mutate a checkout, install globally, or build the transitive
dependency closure.

Two independently retained production corrections are applied to a fresh export:

- `cmake/patches/mvfst-2026.05.25-nullability.patch`: reviewed nullability
  annotations, SHA256 `90082465ef41158ee20391f0d88234b0390f10a7c3f30795f1e9912a00015d31`.
- `cmake/patches/mvfst-2026.05.25-hostid-guard.patch`: omit redundant default-zero
  HostId initialization only during worker construction; ID0 and routing remain
  unchanged. SHA256 `95c18e1adeaca4f1f11d55bd2942ca1559a7311c1fa9ad3b9c82ecfd92437a47`.

The recipe verifies all three hashes and apply-checks both patches before CMake.
It does not apply/install the separate upstream-test discovery/mock corrections
or use their scoped diagnostic exception. Configure, build and install logs are
captured in full; an unsuccessful command or an unplanned warning stops the run.

Supply absolute existing package directories and matching OpenSSL inputs:

```sh
export MVFST_ARCHIVE=/absolute/cache/mvfst-v2026.05.25.00.tar.gz
export folly_DIR=/absolute/folly/lib/cmake/folly
export Fizz_DIR=/absolute/fizz/lib/cmake/fizz
export fmt_DIR=/absolute/fmt/lib/cmake/fmt
export OPENSSL_INCLUDE_DIR=/absolute/openssl/include
export OPENSSL_SSL_LIBRARY=/absolute/openssl/lib/libssl.dylib
export OPENSSL_CRYPTO_LIBRARY=/absolute/openssl/lib/libcrypto.dylib
MVFST_DEPS_DIR="$PWD/.deps/mvfst-reviewed" MVFST_CONFIG=Debug \
  bash scripts/setup_mvfst_deps.sh -G Ninja \
    -DCMAKE_PREFIX_PATH=/absolute/other-selected-dependencies
```

Choose the platform's actual library files, not these illustrative paths.
Additional arguments are ordinary CMake toolchain/dependency options (for example
compiler, sysroot, Boost, zlib or a toolchain file). `MVFST_CONFIG` selects the
build/install configuration, default Release; shared libraries default ON.
The source, warning policy, tests-OFF dependency build and private installation
destination are recipe-owned. This does not alter the adapter's normal
`find_package` behavior or silently replace its dependencies.

Each invocation creates a new `build.XXXXXXXX` under `MVFST_DEPS_DIR`, with its
own source/build/prefix, `inputs.txt`, phase logs and `mvfst_deps.env`. Only after
successful installation does stdout emit shell-escaped `mvfst_DIR` and
`CMAKE_PREFIX_PATH`; source the retained env file to use that specific result.
Failed runs are retained too. Cleanup is caller-owned: remove only a completed
run directory when its evidence and prefix are no longer needed.

The reviewed host closure is AppleClang17, C++20, Debug/shared mvfst with
Folly/Fizz 2026.05.25.00, fmt12.2.0, OpenSSL3.6.5, Boost1.90.0_1,
gflags2.3.0, glog0.7.1, libsodium1.0.22, and CLT MacOSX15.sdk/zlib1.2.12.
Those are caller-provided installed inputs, not source-pinned outputs of this
script. Their further runtime dependencies remain caller-owned. This recipe
does not establish Linux, fully static, whole-closure reproducibility or a new
runtime qualification; the production corrections retain their prior review.

The focused offline script controls can be run without compiling a provider:

```sh
cmake -DSOURCE="$PWD" -DARCHIVE="$MVFST_ARCHIVE" \
  -DWORK=/absolute/private/mvfst-recipe-controls \
  -P scripts/tests/test_setup_mvfst_deps.cmake
```

These use a mock CMake to assert early identity refusal and zero-exit diagnostic
refusal, not to claim a successful provider build.

### LibMoQ adapter

The mvfst adapter is optional and does not affect normal libmoq
builds:

```sh
cmake -B build -DMOQ_BUILD_ADAPTER_MVFST=ON \
    -DMOQ_MVFST_PREFIX=/prefix -DMOQ_FIZZ_PREFIX=/prefix -DMOQ_FOLLY_PREFIX=/prefix
cmake --build build
```

The prefix must also provide `fmt`'s CMake package: folly's config references
`fmt::fmt` without finding it, so the adapter's CMake finds fmt before mvfst.
The client offers QUIC v1 only (`QuicVersion::QUIC_V1`): mvfst's default
offers Meta's private `MVFST` version first and treats the Version
Negotiation packet every other QUIC server answers with as a fatal error, so
without the pin the client could only reach an mvfst server.

The mvfst adapter is consumed via CMake components:

```cmake
find_package(libmoq REQUIRED COMPONENTS adapter-mvfst)
target_link_libraries(app PRIVATE moq::adapter-mvfst)
```

The public API is C-compatible, but CMake consumers need a C++
toolchain available to link the mvfst adapter. The component
config enables CXX automatically if needed.

The adapter is not included in the default `libmoq.pc` to avoid
pulling C++/mvfst dependencies for C-only consumers.

Run the example subscriber:

```sh
./build/examples/mvfst/moq_mvfst_subscriber \
    --insecure 127.0.0.1 4433 live/cam1 video
```

## Current limitations

- **No WebTransport.** Direct QUIC only.
- **ALPN is `moqt-16`.** Not configurable.
- **Bidi STOP_SENDING ignored.** Uni-stream STOP_SENDING is
  handled; bidi STOP_SENDING is pending a core session API.
- **DNS resolution is blocking.** Hostname lookup runs on the
  managed network thread during `create()`. Unresolvable hosts
  cause `create()` to return `MOQ_ERR_INTERNAL`.
