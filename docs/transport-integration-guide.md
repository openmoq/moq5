# Transport integration guide

This guide is for an external integrator wiring libmoq into an
application (VLC, FFmpeg, OBS, a custom player/publisher) **using only
public headers, examples, and tests** — no private context.

libmoq's protocol core is sans-I/O. A *transport adapter* connects a
`moq_session_t` to a real QUIC/WebTransport stack. A *managed facade*
goes one step further: it owns the QUIC context, a network thread, the
connection lifecycle, and the session, and hands you a running
`moq_session_t` to drive. Applications almost always want a managed
facade.

The managed facades (picoquic raw, picoquic WebTransport, mvfst raw,
proxygen WebTransport) share one consumer pattern and one hard threading
rule (the session is single-thread-confined). Learn them once; the
per-backend differences are small. proxygen WebTransport also offers a
C++ attach mode for callers that already own a `proxygen::WebTransport`
(see §10).

---

## 1. Choosing a backend

| | Transport | Stack | Deps | Installed / stable today | Managed C facade | Best fit |
|---|---|---|---|---|---|---|
| **picoquic raw** | raw QUIC (ALPN `moqt-16`) | picoquic (C) | light (C, picotls, OpenSSL) | yes — `moq/picoquic.h` installed; threaded helper installed when `MOQ_BUILD_PQ_THREADED=ON` | `moq_pq_threaded_t` | default for native apps; smallest dependency footprint |
| **picoquic WebTransport** | WebTransport over HTTP/3 (ALPN `h3`) | picoquic h3zero/picowt (C) | light (same as picoquic + HTTP/3) | yes (experimental) — `moq/pico_wt.h` installs (CMake + `libmoq-pico-wt.pc`); `moq/pico_wt_managed.h` installs (CMake + `libmoq-pico-wt-managed.pc`) when `MOQ_BUILD_PICO_WT_MANAGED=ON` | `moq_pico_wt_managed_t` (experimental) | browser/WebTransport interop |
| **mvfst raw** | raw QUIC (ALPN `moqt-16`) | Meta mvfst (C++/folly) | heavy (folly, fizz, mvfst, C++ toolchain) | yes — installed, but **CMake-only** (no `pkg-config` entry) | `moq_mvfst_managed_t` | servers/relays already in the folly ecosystem; multi-connection server |
| **proxygen WebTransport** | WebTransport | proxygen (C++) | heavy (proxygen/folly) | yes (experimental) — installed as **CMake-only** components `adapter-proxygen-wt` (attach) + `adapter-proxygen-wt-managed` (managed), no `.pc` | `moq_proxygen_wt_managed_t` (experimental) | WebTransport in a proxygen/folly C++ service — see §10 |

Quick rules of thumb:

- **Native client/player, want the lightest build →** picoquic raw
  (`moq_pq_threaded_t`). This is what the VLC input module uses today.
- **Need WebTransport interop →** picoquic WT (`moq_pico_wt_managed_t`),
  accepting that it is experimental (installed under
  `MOQ_BUILD_PICO_WT_MANAGED=ON`; CMake component or `libmoq-pico-wt-managed.pc`).
- **Already a folly/mvfst service, or need a multi-connection server →**
  mvfst (`moq_mvfst_managed_t`).
- **proxygen WT →** managed facade `moq_proxygen_wt_managed_t` (drives the
  WT CONNECT, owns the thread + session; explicit-select only — see §10), or
  a C++ attach mode where you own the `proxygen::WebTransport*` and the
  session lifecycle yourself. Installable CMake-only (no `.pc`).

---

## 2. The common managed-consumer pattern

Every managed facade follows the same shape. Names differ by a prefix
(`moq_pq_threaded_`, `moq_pico_wt_managed_`, `moq_mvfst_managed_`); the
flow is identical.

```c
/* 1. Configure (ABI-safe: always cfg_init first). */
moq_<backend>_cfg_t cfg;
moq_<backend>_cfg_init(&cfg);
cfg.alloc        = moq_alloc_default();      /* or your allocator */
cfg.perspective  = MOQ_PERSPECTIVE_CLIENT;   /* or _SERVER */
cfg.host         = "203.0.113.4";            /* client: remote host */
cfg.port         = 4433;
cfg.on_pump      = on_pump;                  /* REQUIRED, see §3 */
cfg.on_pump_ctx  = &app;                     /* (mvfst uses user_ctx) */
/* cert policy: see §5 */

/* 2. Create — spawns the network thread, returns once it is running
 *    (or failed). The TLS/WT handshake completes asynchronously. */
moq_<backend>_t *t = NULL;
if (moq_<backend>_create(&cfg, &t) != MOQ_OK) { /* handle */ }

/* 3. App thread: block until the network thread signals progress.
 *    Never touch the session from here. */
while (!app.done) {
    moq_result_t r = moq_<backend>_wait(t, 100000 /* us */);
    if (r == MOQ_ERR_CLOSED) break;          /* stopped / fatal / exit */
    /* drain your own cross-thread queue here */
}
if (moq_<backend>_is_fatal(t)) { /* report fatal_code */ }

/* 4. Stop then destroy, from the app thread, never from a callback. */
moq_<backend>_stop(t);       /* idempotent; joins the network thread */
moq_<backend>_destroy(t);
```

`wait()` returns `MOQ_OK` (the pump ran), `MOQ_DONE` (timeout, no
activity), or `MOQ_ERR_CLOSED` (stopped, fatal, or the pump asked to
exit). `timeout_us == 0` is a non-blocking poll; `UINT64_MAX` waits
indefinitely. `wake()` (any thread, coalesced) nudges the network thread
to run `on_pump` when your app thread has queued work for it.

> mvfst note: `destroy()` calls `stop()` internally, so the mvfst
> example calls only `destroy()`. The other facades expect explicit
> `stop()` then `destroy()`. Calling `stop()` before `destroy()` is
> always safe.

---

## 3. Threading rules (read this twice)

`moq_session_t` is **single-thread-confined**. The managed facade runs
it on its own network thread. Therefore:

- **`on_pump` is the only place you may call `moq_session_*`,
  `moq_sub_*`, or `moq_pub_*`.** It runs on the network thread between
  transport service calls. Subscribe, poll events, read objects,
  publish — all here. Return `0` to continue, nonzero to request a clean
  loop exit (after which `wait()` returns `MOQ_ERR_CLOSED` and
  `is_fatal()` stays false).
- The session accessor (`..._session()`) returns NULL until the session
  is up (after the connection / WT CONNECT is established); treat NULL
  as "not ready yet; return 0". Even when the accessor is reachable from
  another thread, **the returned `moq_session_t` is single-thread-
  confined and must only be touched on the managed/network thread —
  normally inside `on_pump`.** Do not call session APIs on the app
  thread just because the pointer is non-NULL.
- **`on_activity` is signal-only.** Do not call any session/adapter API
  from it — use it to set a flag or signal a condvar.
- **Cross-thread work goes through your own queue.** The pump produces
  (e.g. pushes decoded objects onto a mutex-protected queue); the app
  thread consumes. `wake()`/`wait()` are the only sanctioned
  cross-thread signals into/out of the facade.
- **Never call `stop()`/`destroy()` from `on_pump`/`on_activity`.**
  `stop()` from the network thread is rejected (`MOQ_ERR_WRONG_STATE`
  for raw picoquic threaded and picoquic WT; `MOQ_ERR_INVAL` for mvfst).

This is exactly how the VLC input module is built: the pump fills a
packet queue under a mutex; the demux thread drains it after `wait()`.

---

## 4. Subscriber skeletons

The `on_pump` body is nearly identical across backends — only the
facade type and session accessor differ. A robust pump: get the session
(bail if NULL), subscribe once after setup completes, drain events.

### picoquic raw (`moq_pq_threaded_t`)

```c
#include <moq/picoquic_threaded.h>

static int on_pump(moq_pq_threaded_t *t, uint64_t now, void *ctx) {
    app_t *a = ctx;
    moq_session_t *s = moq_pq_threaded_session(t);
    if (!s) return 0;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE && !a->subscribed) {
            moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
            /* fill sc.track_namespace / sc.track_name / sc.filter */
            moq_subscription_t sub;
            if (moq_session_subscribe(s, &sc, now, &sub) >= 0)
                a->subscribed = 1;
        } else if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            /* atomic-store a flag / push payload to your queue */
        }
        moq_event_cleanup(&ev);
    }
    return 0;
}
/* cfg: perspective=CLIENT, host, port, on_pump, on_pump_ctx.
 * Raw QUIC uses MOQ_PQ_ALPN_DEFAULT ("moqt-16"). */
```

### picoquic WebTransport (`moq_pico_wt_managed_t`, experimental)

Identical pump; the cfg adds WT specifics (`path`, `sni`) and the type
changes. This header installs (experimental; CMake component or
`libmoq-pico-wt-managed.pc`) only when libmoq is built with
`MOQ_BUILD_PICO_WT_MANAGED=ON` (§9).

```c
#include <moq/pico_wt_managed.h>

static int on_pump(moq_pico_wt_managed_t *m, uint64_t now, void *ctx) {
    app_t *a = ctx;
    moq_session_t *s = moq_pico_wt_managed_session(m);
    if (!s) return 0;
    /* ...same SETUP_COMPLETE → subscribe, OBJECT_RECEIVED → consume... */
    return 0;
}
/* cfg: perspective=CLIENT, host, port, path="/moq", sni,
 * on_pump, on_pump_ctx. WT uses ALPN "h3". */
```

### mvfst raw (`moq_mvfst_managed_t`)

Same pump shape; note mvfst uses a single `user_ctx` (no separate
`on_pump_ctx`) and `moq_session_poll_events_ex` in the example.

```c
#include <moq/mvfst.h>

static int on_pump(moq_mvfst_managed_t *m, uint64_t now, void *ctx) {
    app_t *a = ctx;
    moq_session_t *s = moq_mvfst_managed_session(m);
    if (!s) return 0;
    /* ...same SETUP_COMPLETE → subscribe, OBJECT_RECEIVED → consume... */
    return 0;
}
/* cfg: perspective=CLIENT, host, port, on_pump, user_ctx,
 * send_request_capacity=true, initial_request_capacity=16.
 * Raw QUIC. ALPN/version via cfg.alpn_list/alpn_count (default "moqt-16").
 * Exact-version adapter: a single ALPN only; alpn_count > 1 (AUTO) →
 * MOQ_ERR_UNSUPPORTED. cfg.sni overrides the cert-verified name. */
```

Working, runnable references:
`examples/picoquic/`, `examples/pico_wt/client.c`,
`examples/mvfst/managed_subscriber.c`.

> Request credit: to send a SUBSCRIBE the **peer** must grant request
> capacity. A server that wants to accept subscribes sets
> `send_request_capacity = true` and `initial_request_capacity = N`.
> If `moq_session_subscribe` returns an error before the peer has
> granted credit, this is usually why.

---

## 5. Certificate policy

Cert handling is explicit on every client facade:

- **`insecure_skip_verify = true` is test/demo only.** It installs a
  null verifier that accepts any server certificate. Never ship it.
- **Production must verify — and the safe default differs by backend.**
  - **mvfst** does the right thing on default: a client with neither
    `insecure_skip_verify` nor `cert_path` set uses the **system trust
    store** + host/IP identity. Set `cert_path` (PEM CA trust; mutually
    exclusive with `insecure_skip_verify`) to pin a CA.
  - **picoquic (raw threaded and WebTransport)** does **not**.
    `insecure_skip_verify = false` merely declines to install the null
    verifier; it installs no CA/identity policy, and picoquic's built-in
    default has no CA store and **accepts the peer cert**. So the
    picoquic default is **NOT production-safe** — you must install a real
    verifier via the `configure_quic(quic, ctx)` hook (it runs after the
    QUIC context is created and before any connection). The verifier
    types are not reachable from installed headers, so libmoq ships the
    helper below for exactly this.
- **Servers** require `cert_path` + `key_path`; `insecure_skip_verify`
  is rejected for servers.

### The safe picoquic pattern (copy this)

`<moq/picoquic_verify.h>` provides `moq_picoquic_set_cert_verifier(quic,
ca_file)` — an OpenSSL-backed verifier (chain + hostname/SNI) using the
system trust store (`ca_file == NULL`) or a PEM bundle. Call it from
`configure_quic`; picoquic owns the verifier's lifetime. The same call
works for **both** `moq_pq_threaded` and `moq_pico_wt_managed` (both pass
a `picoquic_quic_t*`):

```c
#include <moq/picoquic_verify.h>

static int configure_quic(picoquic_quic_t *quic, void *ctx) {
    (void)ctx;
    /* NULL = system trust store; or pass a PEM CA bundle path. */
    return moq_picoquic_set_cert_verifier(quic, NULL);  /* 0 = OK */
}

cfg.insecure_skip_verify = false;       /* not enough on its own */
cfg.configure_quic       = configure_quic;   /* this is what verifies */
```

This is the exact pattern the FFmpeg and OBS integrations use for **both**
raw picoquic (`moq_pq_threaded`) and pico WT managed
(`moq_pico_wt_managed`) — one helper, both transports.

**Set the server name correctly, or verification cannot pass.** The
helper validates the certificate *chain* AND the *server name*, so the
name the client presents as SNI must match the peer certificate:
- raw `moq_pq_threaded`: the `host` field is the connection target **and**
  the SNI / verified name. Set it to the cert's name (not a bare IP unless
  the cert has that IP SAN).
- pico WT managed: the `sni` field (default `"localhost"`) is the verified
  name; `host` is only the dial target. A consumer connecting to
  `example.com` must set `cfg.sni = "example.com"` — leaving the default
  verifies against `"localhost"` and fails.

The helper lives in the **picoquic adapter**. Raw `moq_pq_threaded`
consumers already link it (the `adapter-picoquic-threaded` component pulls
in `adapter-picoquic`, and `libmoq.pc` carries it). **pico WT managed
consumers must also pull in the picoquic adapter component** (the managed
facade does not depend on it):
- CMake: `find_package(libmoq COMPONENTS adapter-pico-wt-managed adapter-picoquic)`, then link `moq::adapter-pico-wt-managed moq::adapter-picoquic`.
- pkg-config: there is no standalone `.pc` for the raw picoquic adapter
  (it is folded into `libmoq.pc`); add `libmoq` alongside
  `libmoq-pico-wt-managed`, e.g. `pkg-config --cflags --libs
  libmoq-pico-wt-managed libmoq`.

### How a rejection looks (verify it, don't assume)

A rejected/untrusted cert fails the handshake **closed**, and both managed
facades now surface it the same way: as terminal **fatal**. Any client
transport disconnect *before* the MoQ session reaches
`MOQ_SESS_ESTABLISHED` (cert rejection, connection refused, dead peer) is
latched as fatal:
- `wait()` returns `MOQ_ERR_CLOSED` within a bounded time.
- `is_fatal()` is true; `fatal_code()` is `0` for transport/handshake-level
  failures (the connection died before MoQ setup, so there is no MoQ-level
  close code to report).
- pico WT managed additionally keeps `session()` NULL (the session is
  created lazily on WT accept); raw `moq_pq_threaded` created the session at
  create time, so `session()` is non-NULL but never reached
  `MOQ_SESS_ESTABLISHED`.

So the portable check across both facades is `is_fatal()` after a bounded
`wait()`. Do not infer "the cert was fine" from "no error yet" — wait for
an established session or a terminal fatal.

Bottom line: do not assume `insecure_skip_verify = false` means
verification is happening on the picoquic backends. It does not — call
`moq_picoquic_set_cert_verifier` from `configure_quic` for any production
picoquic / pico-WT client.

---

## 6. URL and backend selection for player-style apps

Recommended conventions for an app that exposes MoQ as a URL:

- **`moq://host:port/...` → raw QUIC** (picoquic or mvfst). This is what
  the VLC module registers today (`add_shortcut("moq")`).
- **`https://host/path` → WebTransport**, where the path is the WT
  CONNECT path (e.g. `/moq`). WT carries real HTTP/3 path semantics, so
  reuse them rather than inventing a scheme.
- **Pick the QUIC stack with a module option, not a new scheme.** Prefer
  one option like `:moq-transport=picoquic|pico-wt|mvfst` over a
  proliferation of URL schemes. The scheme says *what* (raw vs WT); the
  option says *which stack*.

Keep transport selection out of the protocol layer: the subscriber /
publisher facades (`moq_sub_*`, `moq_pub_*`) and media parsing are
backend-agnostic and should not change when you switch stacks.

---

## 7. Publisher path

Servers/publishers use the same managed pattern with
`perspective = MOQ_PERSPECTIVE_SERVER` and `cert_path`/`key_path` set.
Inside `on_pump`:

- Accept incoming subscribes: on `MOQ_EVENT_SUBSCRIBE_REQUEST`, call
  `moq_session_accept_subscribe(...)`, then open a subgroup and write
  objects (`moq_session_open_subgroup` → `moq_session_write_object` →
  `moq_session_close_subgroup`), or use the `moq_pub_*` publisher facade.
- mvfst servers are multi-connection: iterate with
  `moq_mvfst_managed_next_conn()` and use `moq_mvfst_conn_session()` per
  connection. The picoquic WT managed server is **single-connection** —
  a second WT CONNECT is refused with HTTP 501, and the refused client
  sees a deterministic terminal **fatal** (`wait()` → `MOQ_ERR_CLOSED`,
  `is_fatal()`, `fatal_code()==0`, `session()` NULL), not a silent
  timeout.

References: `examples/mvfst/managed_server.c`, `examples/pico_wt/server.c`.

---

## 8. What "it works" must mean (test before claiming integration)

Do not claim a backend integration works until you have, against a real
endpoint (loopback is fine):

1. **Setup handshake** completes (`MOQ_EVENT_SETUP_COMPLETE`).
2. **Subscribe** is accepted (`MOQ_EVENT_SUBSCRIBE_OK`).
3. **Object delivery** — a real object arrives and matches.
4. **Publisher path** — a server accepts a subscribe and the client
   receives the published object end to end.
5. **Clean close** — `stop()`/`destroy()` with no leaked thread/socket.
6. **Fatal connect failure** — connecting where nothing listens reaches
   a terminal fatal state (does not hang), with no session exposed.
7. **Cert failure** — with verification enabled, a rejected cert reaches
   fatal and exposes no session. Prove the verifier actually ran (e.g. a
   flag set in your verify callback); a no-object timeout is **not**
   proof of why a connection failed.
8. **ASAN** — run the above under AddressSanitizer; managed facades own
   threads and sockets, so teardown is where bugs hide.

The in-tree managed tests exercise exactly this list; mirror them.

---

## 9. Packaging (what you can actually link)

| Artifact | Installed? | Consumption |
|---|---|---|
| `moq-core`, `moq/*.h` core headers | yes | CMake + `pkg-config` (`libmoq`) |
| `moq-adapter-picoquic`, `moq/picoquic.h` | yes | CMake `find_package(libmoq COMPONENTS adapter-picoquic)`; pkg-config via the `libmoq` package (folds in `-lmoq-adapter-picoquic` when built with `MOQ_BUILD_ADAPTER_PICOQUIC=ON`) — no standalone `adapter-picoquic` `.pc` |
| `moq-adapter-picoquic-threaded`, `moq/picoquic_threaded.h` | yes, when `MOQ_BUILD_PQ_THREADED=ON` | CMake component `adapter-picoquic-threaded`; pkg-config via the `libmoq` package (folds in `-lmoq-adapter-picoquic-threaded`) — no standalone `.pc` |
| `moq-adapter-pico-wt`, `moq/pico_wt.h` | yes | component `adapter-pico-wt` or `.pc` (`libmoq-pico-wt`); requires picoquic built with `BUILD_HTTP=ON` |
| `moq-adapter-pico-wt-managed`, `moq/pico_wt_managed.h` | yes (experimental), when `MOQ_BUILD_PICO_WT_MANAGED=ON` | component `adapter-pico-wt-managed` or `.pc` (`libmoq-pico-wt-managed`); pulls in `adapter-pico-wt` |
| `moq-adapter-mvfst`, `moq/mvfst.h` + `.hpp` | yes | CMake `find_package(libmoq COMPONENTS adapter-mvfst)` only — **no `.pc` entry** (CMake-only by design; see below) |
| `moq-adapter-proxygen-wt`, `moq/proxygen_wt.hpp` | yes (experimental) | CMake component `adapter-proxygen-wt` only — **no `.pc`** (CMake-only; needs a C++ compiler + a discoverable `proxygen` package). C++ attach (§10) |
| `moq-adapter-proxygen-wt-managed`, `moq/proxygen_wt_managed.h` | yes (experimental), when `MOQ_BUILD_ADAPTER_PROXYGEN=ON` | CMake component `adapter-proxygen-wt-managed` only — **no `.pc`**; managed client facade (`moq_proxygen_wt_managed_t`), pulls in `adapter-proxygen-wt` (§10) |

Implications:

- **`pkg-config` consumers (VLC, FFmpeg via Meson/autotools):** the
  picoquic path is exposed through `libmoq.pc`, and pico WT through its own
  `libmoq-pico-wt.pc` / `libmoq-pico-wt-managed.pc`. **mvfst is CMake-only**
  (no `.pc` — see "Why mvfst is CMake-only" below); a Meson/autotools build
  cannot pull mvfst via `pkg-config`. VLC's MoQ module is gated on
  `libmoq_dep` + `picoquic_dep` for this reason.
- **CMake consumers:** use `find_package(libmoq COMPONENTS ...)` with the
  component names above. mvfst additionally needs a C++ compiler (the
  config errors out otherwise) and a discoverable `mvfst` package.
- **pico_wt WebTransport** requires the picoquic dependency built with
  `BUILD_HTTP=ON` (it ships the h3zero/picowt headers + `picohttp-core`).
- **The managed picoquic facades require source-tree picoquic.** Both
  `MOQ_BUILD_PQ_THREADED` (`moq_pq_threaded_t`) and `MOQ_BUILD_PICO_WT_MANAGED`
  (`moq_pico_wt_managed_t`) implement `moq_endpoint_drain`'s graceful flush from
  picoquic's private stream state, so they must be configured with
  `-DMOQ_PICOQUIC_SOURCE_DIR=/path/to/picoquic`. The installed picoquic CONFIG
  package does not ship that private header; configuration fails early with a
  clear message if the source dir is missing.
- **The managed pico WT facade** installs (experimental) under
  `MOQ_BUILD_PICO_WT_MANAGED=ON` as component `adapter-pico-wt-managed`
  (CMake) or `.pc` (`libmoq-pico-wt-managed`); expect API churn while
  experimental. **proxygen WT** installs (experimental) under
  `MOQ_BUILD_ADAPTER_PROXYGEN=ON` as the CMake-only components
  `adapter-proxygen-wt` (attach) and `adapter-proxygen-wt-managed`
  (managed `moq_proxygen_wt_managed_t`), neither with a `.pc` (§10).

### Why mvfst is CMake-only

mvfst is intentionally **not** exposed through `pkg-config`, and this is
the honest contract rather than a missing feature:

- mvfst's own dependencies (folly, fizz, wangle, glog, gflags,
  double-conversion, boost, libsodium, …) ship CMake config packages, not
  `.pc` files. A hand-written `libmoq-mvfst.pc` would have to hard-code
  that entire transitive `-l`/`-L` set with no upstream `.pc` to compose
  against — brittle, version-specific, and wrong the moment a dependency
  moves. The mvfst CMake package resolves all of it via
  `find_dependency(mvfst CONFIG)`.
- The public C++ attach header (`moq/mvfst.hpp`) requires the consumer to
  have mvfst's own headers and a C++ toolchain anyway, so a C-style
  `pkg-config` line cannot stand alone.

Meson/autotools consumers that need mvfst should drive the libmoq build
via CMake and consume `find_package(libmoq COMPONENTS adapter-mvfst)`
from a small CMake shim, or vendor the mvfst stack through their own CMake
sub-build. Do not expect a `libmoq-mvfst.pc`.

---

## 10. proxygen WebTransport: honest current state

proxygen WT offers **two** public surfaces, both **experimental** and
both binding directly to `proxygen::WebTransport` (no moxygen dependency):

1. **Managed client facade** (`moq/proxygen_wt_managed.h`,
   `moq_proxygen_wt_managed_t`) — a C facade that owns the network thread,
   drives the WebTransport CONNECT (manual QUIC/H3 handshake), negotiates
   the MoQ version from the selected WT protocol, creates the session, and
   runs the pump. Same consumer shape as the other managed facades
   (`wait`/`wake`, single-thread-confined session). This is what the
   service endpoint uses for `MOQ_TRANSPORT_BACKEND_PROXYGEN`.
2. **Attach mode** (`moq/proxygen_wt.hpp`, `moq::wt::Adapter`) — for a
   caller that already owns a `proxygen::WebTransport*` + `moq_session_t`
   and runs the proxygen event loop itself: install the adapter as the
   WebTransport handler and drive `service()` on the executor thread.

**Service endpoint:** selecting `MOQ_TRANSPORT_BACKEND_PROXYGEN` (WebTransport
only) routes through the managed facade when built with
`MOQ_BUILD_ADAPTER_PROXYGEN=ON`; it is **explicit-select only** — `AUTO`
WebTransport stays on picoquic WT. proxygen is **client-only**; there is no
server/listen mode.

**Verification posture:** publish **and** receive are **byte-exact through
moqx** in the E2E matrix (catalog + media, both directions). Deterministic
in-tree tests cover the attach adapter, endpoint resolution, and the managed
facade's config/lifecycle, but **not** a full real-proxygen WT-server receive
(the local fake transport does not model proxygen's WT stream-credit), so the
end-to-end receive proof is the E2E moqx cell rather than a local unit test.
proxygen/mvfst are **not run in GitHub CI** (dependency-heavy); they are
exercised locally and in E2E only.

**Interop shims** (peers like moqx map WT streams onto QUIC stream limits and
do not drive proxygen's newer capsule-based WT stream flow control): the
managed facade seeds WT stream credit in both directions — inbound via the H3
egress SETTINGS (`WT_INITIAL_MAX_STREAMS_UNI/BIDI`, so the peer may open
streams to us = receive) and outbound via `H3WtSession::onMaxStreams` (so we
may open streams to the peer = publish). The underlying QUIC stream limits
remain the real bound.

**Packaging:** both surfaces install (experimental; no ABI/API stability
promise) under `MOQ_BUILD_ADAPTER_PROXYGEN=ON` as **CMake-only** components
(`adapter-proxygen-wt` attach, `adapter-proxygen-wt-managed` managed) with
**no `pkg-config`** entry (same posture as mvfst — proxygen/folly are
CMake-native). Consuming the headers pulls in proxygen + folly, which never
leaks into core or any non-proxygen header. A C++ compiler and a discoverable
`proxygen` package are required.

---

## 11. What not to do

- **Do not call `moq_session_*` (or `moq_sub_*`/`moq_pub_*`) from the
  app thread.** Only `on_pump` may touch the session. This is the
  single most common way to corrupt state.
- **Do not call `stop()`/`destroy()` from `on_pump`/`on_activity`.**
- **Do not call session/adapter APIs from `on_activity`** — it is
  signal-only.
- **Do not include internal adapter headers** (`pico_wt_adapter.h`,
  `pico_wt_endpoint.h`, `moq_picoquic.h`, the adapter `src/`). Integrate
  only against the installed/public `moq/*.h` facade headers.
- **Do not depend on test harnesses** (the conformance pairs, fake-WT,
  loopback helpers, `picoquic-test`). They are source-tree-only seams
  and are not installed.
- **Do not treat a no-object timeout as proof of a failure cause.** A
  timeout could be a busy port or a dropped packet — assert the specific
  signal (setup-complete, subscribe-ok, fatal state, a verifier-ran flag)
  you actually mean. (A WT CONNECT refusal is *not* one of these: it is a
  deterministic terminal fatal, so assert `is_fatal()`, not a timeout.)
- **Do not build relay/routing/forwarding logic into the transport
  facade.** Keep the facade owning *transport lifecycle*; keep
  *protocol policy* (which tracks, which subscriptions, forwarding) in
  your app above the session.
- **Do not ship `insecure_skip_verify`.** It is for tests and local
  bring-up only.

---

## 12. Reference: current VLC integration (evidence)

The VLC tree is read-only evidence of what a cold integration looks like
today.

- **Input (active):** `modules/access/moq/moq.c` uses the raw picoquic
  threaded facade (`moq_pq_threaded_t`, `<moq/picoquic_threaded.h>`)
  plus the backend-agnostic subscriber facade (`moq_sub_*`) and media
  parsers. The pump (network thread) runs `moq_sub_*` and fills a
  mutex-protected packet queue; the demux thread drains it after
  `moq_pq_threaded_wait(...)`. Close order is `stop` → `moq_sub_destroy`
  → `destroy`. URL scheme `moq://`, namespace from the URL query
  (`ns=`), `insecure_skip_verify` hardcoded true. There is **no
  transport-selection option** — picoquic-threaded is wired directly.
  Built only when both `libmoq` and `picoquic` are found via
  `pkg-config`.
- **Output/publisher (disabled, stale):**
  `modules/access_output/moq.c` is commented out in its `meson.build`
  ("output module disabled pending adapter migration"). It predates the
  current API (it uses a project-local `moq/adapter.h` abstraction and
  an older `moq_url_parse` argument order) and does not build.

A future backend switch would factor out the picoquic-threaded-specific
touchpoints in `access/moq/moq.c`: the `moq_pq_threaded_t` handle and
`moq_pq_threaded_cfg_t`/`cfg_init`, `..._create`, the `..._session()`
call inside `on_pump`, `..._is_fatal()`/`..._wait()` on the app thread,
and `..._stop()`/`..._destroy()` at close — plus the `on_pump`/
`on_activity` callback signatures, which take the concrete facade type.
Everything above the session accessor (subscriber facade, media parsing,
ES output) is already backend-agnostic. Note the cfg shapes differ
slightly per backend (picoquic uses split `on_pump_ctx`/`on_activity_ctx`;
mvfst uses a single `user_ctx`), so a neutral wrapper must normalize
config and expose a transport-neutral pump handle.

---

## 13. MoQ version & ALPN selection

libmoq is strict about the MoQ wire version (the "profile"). A session
binds to exactly one profile at create time via
`moq_session_cfg_t.version` (`0` = the draft-16 default), and it is
immutable thereafter. There is **no** auto-detection — libmoq never tries
multiple decoders to guess the version. An unsupported value fails
`moq_session_create` with `MOQ_ERR_INVAL`.

The version is decided by **transport negotiation**, and which surface
carries it depends on how you run MoQ:

- **Native MoQ-over-QUIC** (picoquic raw/threaded, mvfst): the QUIC ALPN
  *is* the MoQ version. `moqt-16` ⇒ draft-16. Today's managed adapters
  negotiate `moqt-16` and create draft-16 sessions.
- **H3 WebTransport** (pico_wt and proxygen WT — the in-tree WebTransport
  adapters): the QUIC ALPN is `h3`, **not** a MoQ ALPN. The MoQ version
  is negotiated by the WebTransport protocol layer
  (`WT-Available-Protocols`). Automatic wiring of that into `cfg.version`
  is future work; the H3-WT adapters create draft-16 sessions today.
- **WebTransport-over-QUIC** (a MoQ ALPN such as `moqt-16` carried
  directly over a `QuicWebTransport`-style stack, e.g. proxygen's): the
  QUIC ALPN *is* the MoQ version, selected the same way as native. This
  is **not** a transport shape any in-tree adapter uses today — noted
  only for integrators wiring such a stack themselves.

Who sets `cfg.version`:

- **Attach-mode hosts own the handshake** and therefore know the
  negotiated transport version. Set `cfg.version` to match the ALPN /
  WT protocol you negotiated. For a `moqt-16` connection leave it `0`
  (or `MOQ_VERSION_DRAFT_16`). libmoq cannot infer it for you.
- **Managed native/QuicWebTransport adapters** negotiate the ALPN
  themselves; reading the negotiated ALPN back into `cfg.version` (and
  failing a connection whose ALPN is unknown/mismatched before any SETUP
  byte is parsed) is future work — the managed adapters create draft-16
  sessions today.

Legacy peers: drafts ≤14 used the `moq-00` ALPN and carried a version
list inside the client's setup message. Draft-16 removed the version list
(ALPN is the sole version surface), so such a setup is rejected by the
strict parser. The server emits an advisory close reason noting the bytes
look like a legacy version-list setup and that draft-16 negotiates the
version via ALPN (expecting `moqt-16`) — pointing at the likely ALPN
mismatch. The rejection itself is unchanged; only the diagnostic is more
specific.
