# Rust bindings

Four crates, along libmoq's tiers:

- [`moq5-sys`](moq5-sys): raw FFI, generated from the installed headers with
  bindgen at build time, by tier: the core by default, and the `sim`,
  `media`, `service` and `player` features. It owns the one native link.
  The header comments are the contract.
- [`moq5-core`](moq5-core): the sans-I/O core, for a consumer that owns its
  transport, and the vocabulary every tier shares (`Buffer`, `Error`,
  `Namespace`, `Version`, locations, object statuses, group orders).
- [`moq5-media`](moq5-media): the media parsers -- CMAF parsing, LOC
  metadata, the MPEG-TS catalog records and the media value types.
- [`moq5`](moq5): the service tier -- endpoint, media sender and receiver,
  the stream-terminal lane, draft-22 object metadata, track properties and
  fills, MPEG-TS writes, and with the `player` feature the media player --
  whose types enforce the ownership, teardown, sizing and threading rules
  the C API states in comments. It re-exports the names of the lower
  crates, so its users need no other.

All are `publish = false`; nothing is uploaded or reserved.

[`examples/quinn-subscriber`](examples/quinn-subscriber) is a small host for
`moq5-core` over a real QUIC stack: a subscriber on
[quinn](https://github.com/quinn-rs/quinn), in a workspace of its own.

## Initial sender catalogs

`Sender::attach_with_tracks` queues attachment and the complete initial track
set in one endpoint turn, before the sender's first pump. Poll its
`SenderStartup` with `poll`, waiting on the endpoint between polls. The result
contains the sender and track handles in input order; peer acceptance is still
observed separately through `Sender::is_ready`. No application closure or media
payload runs on the managed thread. Dropping startup cancels a queued task or
settles a running registration, and drops an unclaimed result on the application
thread. Failed registration detaches the partial sender before its first hook.

## Style

Handwritten Rust follows the
[Google Rust Style Guide](https://google.github.io/styleguide/rustguide.html),
including its library API design rules:
leaf modules named after their primary item, no glob imports, module-qualified
calls to free functions, descriptive type lifetimes, `try_new` for a fallible
constructor and standard traits (`FromStr`, `TryFrom`) beside the named
constructors, `#[must_use]` on pure functions and the error types, a
`SAFETY:` justification on every `unsafe` block, `expect` with a reason over
`unwrap` (in the tests too), and a poisoned lock never reached into.
`#[non_exhaustive]` is applied where the guide's reasons hold and stated
where they do not: on every enum that mirrors a native set (each keeps an
`Other(raw)` and a `code()`), and on the records the library produces from
native records that grow with the SDK (events, statistics, terminals, parse
results). It is not applied to `SendObject`, the application's own record
(constructed with `new` and struct update), to `WriteError`, a fixed pair
meant to be destructured, to `Location` and the other fixed value types, or
to the builders, whose fields are private. Justified exceptions: the
endpoint is an `Arc` handle (the attachments hold it, and a clone on another
thread wakes or interrupts a blocked call); the generated `sys::` names are
used qualified rather than imported, as one large group of generated types;
the rustfmt options the guide lists that are nightly-only
(`imports_granularity`, `format_macro_matchers`) are followed by hand on
stable rustfmt. Rules scoped to Chromium or Fuchsia do not apply to this
Cargo workspace.

## Requirements

- An installed libmoq SDK carrying the pkg-config package of each tier the
  dependency graph asks for: `libmoq-core.pc` for the core alone,
  `libmoq-sim.pc` for the simulator (`MOQ_BUILD_SIM=ON`), `libmoq-media.pc`
  for the media parsers (all five built, MPEG-TS included), and
  `libmoq-service.pc` for the service. `moq5-sys` links the packages' own
  pkg-config closures, all from one SDK, so the supported service SDK
  builds are exactly the ones that install `libmoq-service.pc`: the service
  tier without adapters (the `install` preset), the picoquic raw-QUIC and
  PicoWT managed backends, and the WTQuic managed backends. A service built
  with mvfst, proxygen or the direct MsQuic managed backend has no
  pkg-config representation and is not linked by the crate; `moq5-sys`
  reports that case, and any other missing package, rather than guessing a
  link line.
- For the `player` feature (`moq5::player`), an SDK built with the player
  (`MOQ_BUILD_PLAYBACK=ON`, which installs `media_player.h`); the feature
  fails the build without one. MPEG-TS tracks play through an SDK built
  with `MOQ_BUILD_MPEG2TS=ON`; without it the service refuses them
  (`Error::UNSUPPORTED`).
- `pkg-config`, and libclang for bindgen (`LIBCLANG_PATH` when it is not on
  the loader's path; `BINDGEN_EXTRA_CLANG_ARGS` for a libclang installed
  without its resource headers).
- `python3`, for the diagnostic classifier the consumer, package, fake and
  gate-control rows run (`scripts/diagnostics.py`; the tree requires it when
  the rows are enabled).
- For the AddressSanitizer row, the sanitizer runtime of the C compiler that
  builds the fake provider: clang's compiler-rt runtime (on Ubuntu,
  `libclang-rt-dev` beside `clang`) or gcc's libasan. `fake-tests/run.sh`
  links that compiler's own runtime and stops, naming the package, when it
  is missing; it never substitutes another compiler's.
- Rust 1.85 or later (`rust-version`), proven by the `msrv` row below. The
  rows run the MSRV check, the docs.rs snapshot and the cross rows through
  rustup toolchains (`1.85` and `stable` by default, `MOQ_RUST_MSRV` and
  `MOQ_RUST_TOOLCHAIN`), since rustup owns the foreign targets they add.

Point the build at the SDK with `MOQ_PREFIX=<prefix>` (a convenience that
puts `<prefix>/lib/pkgconfig` first on `PKG_CONFIG_PATH`), or set
`PKG_CONFIG_PATH` yourself. Static or shared linking follows the installed
libraries, or is selected with the `pkg-config` crate's own variables for
each package it probes (`LIBMOQ_CORE_STATIC`, `LIBMOQ_SERVICE_STATIC`, ...,
and their `_DYNAMIC` forms); with neither
set, pkg-config reports only the `.pc` files' public `Libs`, so an SDK
installed static whose `.pc` files keep dependencies in `Libs.private` (the
WTQuic managed SDK: msquic) needs `LIBMOQ_SERVICE_STATIC=1` to link (the rows
set the mode from the SDK's installed library type). A cross build
names the target SDK (`MOQ_PREFIX_<target>`) and the target sysroot
(`MOQ_SYSROOT_<target>`) and is refused before any probe without both; the
host's prefix, search path and headers are never used for a foreign target
(the full rule is in [`moq5-sys/README.md`](moq5-sys/README.md)). A shared
SDK's libraries are found at run time through the loader path
(`scripts/sdk_paths.sh` derives it from the SDK's pkg-config closure for the
rows); cargo adds only directories inside its target tree on its own.

```sh
cmake --preset install && cmake --build build/install && cmake --install build/install
cd bindings/rust
MOQ_PREFIX=../../../local-prefix cargo test     # unit and compile-fail tests
```

## Layout

```
bindings/rust/
├── Cargo.toml          the product workspace: moq5-sys, moq5-core,
│                       moq5-media, moq5
├── moq5-sys/           bindgen crate, by tier; wrapper_<tier>.h restate the
│                       constants bindgen cannot read, build_tiers.rs holds
│                       the tier rules; docs/ holds each tier's docs.rs
│                       snapshot
├── moq5-core/          the core crate and the shared vocabulary
├── moq5-media/         the media crate
├── moq5/               the service crate; tests/compile_fail (fixtures cargo
│                       type-checks against the crate) and tests/live_*
│                       (the relay rows, #[ignore]d)
├── fake-tests/         its own workspace: a scripted C service provider
│                       (fake_service.c), the layout probe (layout_probe.c)
│                       and the deterministic rows; run.sh substitutes the
│                       provider through a generated libmoq-service.pc
├── consumer/           its own workspace: out-of-tree consumers built in a
│                       clean environment against a static or shared SDK,
│                       of moq5 and (core/) of moq5-core alone
├── examples/           quinn-subscriber: its own workspace, a moq5-core
│                       host over quinn; its tests are a relay row
├── scripts/            ctest_row.sh (every row), package_check.sh,
│                       stage_sdk.cmake, diagnostics.py (the diagnostic
│                       classifier) + the dependency allowlist,
│                       gate_controls.sh (the gates' negative and positive
│                       controls), sdk_paths.sh (link mode and loader paths
│                       from the SDK's pkg-config closure), tier_graphs.sh
│                       (each tier graph and the refusals), update_snapshot.sh
└── CMakeLists.txt      the ctest rows (MOQ_BUILD_RUST_BINDING)
```

The test workspaces and the example are excluded from the product workspace,
so nothing they depend on (quinn, tokio and rustls, for the example) can
unify into a consumer's build or reach its lock file; the fake provider is reached
through the same pkg-config path a real SDK uses, never through a feature or
a runtime table.

## Qualification rows

With `-DMOQ_BUILD_RUST_BINDING=ON` (needs `MOQ_BUILD_TESTS=ON`,
`MOQ_BUILD_SERVICE=ON`, cargo and rustup), the tree registers ctest rows that
stage an SDK from the build and run `scripts/ctest_row.sh` for each:

| Row | What passes |
|---|---|
| `rust_sdk_install` | the tree installs into a test prefix that carries `libmoq-service.pc` and `libmoq-core.pc` (fixture) |
| `rust_fmt` | `cargo fmt --check` on the three workspaces |
| `rust_clippy` | `cargo clippy --all-targets -D warnings` on the three workspaces (the consumer's both members), again with the player feature where it is qualified, and on each lower tier's graph alone: `moq5-core`, `moq5-sys` with no feature, and where the SDK carries them `moq5-media`, `moq5-sys` with `media`, `moq5-sys` with `sim` |
| `rust_doc` | `cargo doc --no-deps` under `missing_docs` and `-D warnings` (again with the player feature where it is qualified, and on each lower tier's graph alone), and the `cfg(docsrs)` snapshots without an SDK for each target the manifests restrict docs.rs to, each crate alone with the features its manifest documents (every tier for `moq5-sys`, the player for `moq5`) |
| `rust_tests` | the product tests of the four crates, again with the player feature where it is qualified, and on each lower tier's graph alone (each enabled tier's snapshot checked in a graph that enables no other): `moq5-sys` (each tier's snapshot and header preambles match a fresh generation: the preambles byte for byte, the bindings line for line, a macro constant's documentation alone allowed on one side only, as libclang releases attach it differently, with the comparison's own regressions; the header comments, the `rcbuf.h` threading contract and each tier's contracts reach the documentation), the build script's tier rules (`build_tiers.rs`: each feature set's tiers and packages, the refusal texts, the derive facts read from formatted and unformatted bindings), the build script's cross sysroot rules (`build_sysroot.rs`: every root operand counts, every root must be one existing directory); the unit tests of `moq5-core` (the `Namespace` root: constructed, cloned, compared and converted to and from native, zero parts accepted and an empty part refused; the semantic object statuses), `moq5-media` and `moq5`; with LOCMAF, `moq5-media`'s LOCMAF rows: the codec through its public API (track facts from a CMAF Header, a group decoded and written as its canonical CMAF chunks and encoded back to the same Objects with an explicit commit, rawBoxes, the bound, the refusals with their reasons, the counters), its storage under a counting global allocator (allocated once with the library's size and alignment and freed once with the same layout, a chain surviving a move into a container and to another thread, an independent copy) and its compile-fail fixtures (a chunk past its decoder or its Object, a second push while a chunk lives, a decoder or encoder shared across threads, a cloned decoder, and the positive control); the live harness's verdict on its native peer, with shell children (`tests/live_harness.rs`); and the compile-fail fixtures, checked by the cargo running the tests against the crate as the path dependency of a fresh scratch package: cargo's artifact record of the crate compiled in that run, each refused fixture's error with no artifact, the positive fixture's artifact with no error |
| `rust_tier_graphs` | `scripts/tier_graphs.sh`: each supported graph built with `cargo -vv --message-format=json` against the SDK, both streams classified by the strict diagnostic gate first (a successful build that warned fails its graph), then cargo's own record of `moq5-sys`'s build script (its libraries and out_dir, fresh runs included) and its report read: the core alone (`moq5-sys` with no feature, and `moq5-core`) probes `libmoq-core` and links `moq-core` and nothing else; `sim` adds `libmoq-sim` and `moq-sim`; `media` (and `moq5-media`) links the five media parsers and the core and nothing of the service; `sim` with `media` both packages; `locmaf` (and `moq5-media` with `locmaf`) adds `libmoq-locmaf` and `moq-locmaf` after the media package, and after the service's beside `moq5`; `service`, `service` with `sim`, `player` and `player` with `sim`; independent crates unified into one graph (`moq5-core` with `moq5-media`; with `moq5`; with `moq5-sys`'s `sim` or `moq5`'s player); each with the whole SDK-owned metadata in the SDK's one pkg-config directory. A tier the SDK is stated not to carry is left out, and refused where the SDK lacks its package. The refusals, on views of the SDK, every view (the matching controls included) searched with the SDK's external providers beside it (copies of the packages it does not own, from its pkg-config directory and `MOQ_RUST_SDK_PKG_CONFIG_PATH`, the first spelling of a name kept; never an SDK-owned one), each failing for its reason, naming none it must not, with exactly one occurrence of the build script's failure as its only failing diagnostic (the predicate's own controls: once passes; twice, or beside another failing identity, refuses): a missing `libmoq-core.pc`, `libmoq-sim.pc`, `libmoq-media.pc` or `libmoq-locmaf.pc` while `libmoq.pc` resolves (never used in their place); a tier package, the service package, or the simulator beside the service resolving from another directory than the rest of the SDK-owned metadata (the service's aggregate `libmoq.pc` included); one directory whose packages disagree on their version (the aggregate included); with, as the matching controls, a view holding the whole SDK, which builds; and the same refusals and matching views (`service`, and `service` with `sim`) for an SDK whose service requires an external provider of its own directory, prefix and version, configured as `MOQ_RUST_SDK_PKG_CONFIG_PATH` configures one and collected by the same code from two directories (the first one's provider is kept with its prefix resolved; a later provider of the same name, and the SDK-owned packages beside the first, are left out), with controls in which that provider is missing and the service's requirements fail for that reason alone. The warning gate's control: the core graph with an unknown clang warning option exits 0 and holds its assertions, and fails on the classifier alone. Accepted upstream warnings are classified and listed in each inventory, not absent |
| `rust_fake_tests` | the deterministic rows on the scripted provider: configuration mapping (the MPEG-TS records and inputs, draft-22 track properties and object metadata, locations and fills), teardown order, write transfer, transport-stream writes and what they took, per-track statistics, new-group requests taken once, bounded polls, the receiver information, foreign handles, stamp refusal with cleanup, snapshots, the lane, durations, allocator balance, whole log lines; with the player feature the player's rows (attach and its refusal, selection, progress and waiting, outputs that own their references, events with every detail, feedback, statistics, teardown order); and the `Suite` exclusivity fixture (one shared across threads does not compile). The build's own diagnostics gate the row (its stderr classified: without `-vv` any diagnostic is this build's) |
| `rust_fake_asan` | the same rows with the provider compiled under AddressSanitizer and linked with that compiler's own runtime, which the provider's generated pkg-config file names right after the provider archive, before libc (linked after libc, GNU ld on aarch64 prints glibc's link warnings for the runtime's `tmpnam` interceptors); under the same diagnostic gate, where a linker message is a diagnostic |
| `rust_layout` | sizeof/offsetof of every record the safe crate reads or writes, from a C translation unit, against the generated types and the v0 floors; with the player feature, the player's records and their union members too; with LOCMAF, the LOCMAF records `moq5-media` reads, writes or allocates (the decoder and encoder states by size and alignment) |
| `rust_installed_consumer_static` / `_shared` | the out-of-tree consumers of `moq5` and of `moq5-core` alone, cold-built in a clean environment with `cargo build -vv --message-format=json`, every diagnostic classified (`scripts/diagnostics.py`): an error, a diagnostic of the consumer's own crates, a manifest or unlocated warning, or a dependency warning that `scripts/dependency_diagnostics.allow` does not name by exact crate, version, source location and lint fails, and every diagnostic is inventoried beside the log (a row that passes with the accepted upstream warnings passes with warnings, which the inventory lists); every `rustc-link-search` directory inside the SDK's pkg-config closure with the SDK's own libdir among them exactly; no provider symbol (`nm`, required); the endpoint's entry point, and with the player feature the player's, defined in a static binary and imported by a shared one; the link proof of the core tier: the `moq5-core` consumer's build probes `libmoq-core` alone and links `moq-core` alone, its binary defines (static) or imports (shared) the session's, rcbuf's and the version query's entry points and holds no symbol of the simulator, the media parsers, the service, the player or a transport adapter, and a shared one loads `libmoq-core` and no other libmoq library; run with the closure's loader path and every import bound at launch; the link mode follows the installed library type (the row matches the tree's) |
| `rust_package` | `cargo package` of the four crates and their file lists (every tier's wrapper, snapshot and preambles, the tier rules, the player's module and compile-fail fixtures, and the LOCMAF module and its compile-fail fixtures, included), each step's stderr classified as well as its exit status checked (`scripts/cargo_step.sh`: cargo's own package warnings exit 0), a consumer of the extracted `moq5-core` alone (its graph probing `libmoq-core` alone), and a consumer built in a clean environment from the extracted packages with every lower crate resolved through `[patch.crates-io]`, with the player feature where it is qualified, under the same diagnostic and link-closure gates |
| `rust_gate_controls` | the gates' own controls (`scripts/gate_controls.sh`): the classifier passes a build whose only diagnostic is an allowed upstream warning while inventorying it, and refuses a manifest warning, an own or a dependency build-script warning, a dependency lint or codeless diagnostic not listed, the allowed lint at another location or in another version, an own warning, a dependency error at an allowed identity, an unlocated tool warning, and an allowlist with a crate-wide or location-free entry; link-mode selection for static-only, `.dylib`, `.so`, dual and empty libdirs, with the export clearing the opposite variable; the link-search assertion refusing a prefix of a path, a directory outside the closure and a missing line; a row refusing a missing SDK with the helper's reason; a row refusing, with the reason, a player, MPEG-TS, simulator or LOCMAF statement that the SDK it links contradicts, a value other than 1 or 0, or none, while passing one that matches (a player left out is named in its log); a row run with the opposite link-mode variable inherited stating the SDK's own mode to its builds, the opposite cleared; the snapshot updater taking the generation its own cargo invocation records, after two feature sets were built in one target directory with the core alone last (the union's for the union, the core's for the core, each compared with that generation byte for byte, into a scratch directory); the diagnostics a build script prints under `-vv` (clang's, bindgen's `clang diag:`, located or not, on either stream) failing while echoed `cargo:` directives pass; and, with real cargo runs, `moq5-sys` checked against the SDK passing clean and failing when an unknown clang warning option makes libclang warn from its build script; a rebuilt static archive making `cargo check` compile `moq5-sys` again (rustc bundles the static libraries it links into its rlib) where an unchanged one leaves it fresh, which proves the rebuild trigger, not a consumer linking the replacement; a view of the SDK whose metadata places its headers under `headers/` found by the configure defaults (`scripts/sdk_features.cmake`), by the statement check (a match passes, a contradicted MPEG-TS statement is refused), by a `moq5-sys`/`moq5` check with the player and by the fake provider's layout rows; the player feature refused by `moq5-sys`'s build script itself, with its remedy, on a view without the player's headers that builds without the feature; those positive builds and probes classified under the rows' warning policy, each run a second time with one warning injected while every exit status stays 0 (a libclang warning, or a cargo, cc or pkg-config that warns before running the real one), which must fail with the warning in its inventory; and the package step refusing a package without metadata while passing the real crate |
| `rust_msrv` | `rustup run 1.85 cargo check --locked --all-targets` on the three workspaces, again with the player feature where it is qualified, and on each lower tier's graph alone |
| `rust_cross_refusal` | a cross build with the host SDK alone, a host search path (plain, or the generic `TARGET_`/`HOST_` spellings) beside a sysroot, a sysroot alone, a target SDK without a sysroot, a `--sysroot` spelled inside an unrelated define or in a spelling bindgen does not read, mixed `-isysroot`/`--sysroot` operands naming two roots in either order, or two sysroots that disagree is refused before any probe, `PKG_CONFIG_ALLOW_CROSS` notwithstanding; a target search configuration holding no SDK is refused by the probe with the host SDK on every inherited spelling (no fallback) |
| `rust_cross_build` | a target SDK configured from the tree (macOS: the other architecture) with the player, MPEG-TS, the simulator and LOCMAF as stated, its cache and its own statement checked; the crates checked for that target against it (`cargo check --target`; nothing is linked or run) without the player and, where it is stated, with it, each pass naming the host SDK on every spelling and proving it unused: the link-search directories emitted are exactly the target SDK's libdir, and pkg-config, run through a recorder, was given exactly the target's directories as its library list and no search path on every probe; the same root named through `MOQ_SYSROOT` and both clang root options is accepted. The player pass must show the target's generated player bindings and both crates compiled with `feature="player"`, which the base pass's log must not (a control for a missing player pass); the simulator pass, where it is stated, the target's generated simulator bindings and `moq5-sys` compiled with `feature="sim"`, which the base pass's log must not; where it is stated absent, the target SDK's refusal of the simulator tier |
| `rust_relay_rows` | `moq5/tests/live_*` against a `moq5-relay` subprocess on loopback with the test-only credentials, drafts 22 and 18, one row per process; the status-propagation row drives a native publisher peer (`moq5/tests/support/done_peer.c`) that completes its subscription with a chosen PUBLISH_DONE status (the peer injects it; no expiry timer runs) and must then exit 0 (its stop checks the status, also when it exited first); the same row with the peer exiting 3 after its done must fail, on that exit, on both drafts; `MOQ5_RELAY_PEER_SANITIZE=thread` builds the peer under ThreadSanitizer, whose report (exit 66) fails the row. MPEG-TS: 188- and 192-octet transport streams written in chunks that split packets across calls arrive byte for byte (the 192-octet prefix stripped by the receiver) in Location order, every Group opening with the PAT, the sender's counters exact; on an SDK stated without MPEG-TS, the track's refusal (the writer handles short writes, WOULD_BLOCK and held Objects without requiring them, and those results are the fake rows' coverage). Draft 22: explicit LOC-04 metadata and Track Properties arrive with each object (neither on drafts 16/18), and a Track Properties set that does not parse is refused when the track is added, on every draft, the sender still usable and the name free for the corrected set; a bounded poll leaves a head larger than its allowance queued and takes it at exactly its charge; the receiver information is the effective configuration; a late subscriber's fill delivers the retained objects under its request ID and ends with one COMPLETE terminal, after which it cannot be cancelled (drafts 16/18 refuse the fill). With the player feature, a raw video track plays through a player in decode order with its exact bytes, configuration (codec, dimensions, timescale), timeline and end, decoder-queue feedback pausing and resuming release; an opus track plays with the catalog's sample rate and channel count in its configuration |
| `rust_quinn_example` | only with `MOQ_RUST_QUINN_EXAMPLE=ON`: `examples/quinn-subscriber` formatted, clippy-clean and checked with the 1.85 toolchain against its lock file; its own tests of how a session ends, which need no relay; then its three rows against a `moq5-relay` subprocess on loopback over real QUIC, on drafts 22 and 18, the relay rows' publisher peer behind the relay: the publisher completes the subscription; a session with an event queue of two keeps input and is resumed; an unsubscribe stops the streams being read, and each is settled before the session closes |

`MOQ_RUST_PLAYER`, `MOQ_RUST_MPEG2TS`, `MOQ_RUST_SIM` and `MOQ_RUST_LOCMAF`
state what the SDK carries beyond the service: the player (the rows build
with the `player` feature), MPEG-TS (the MPEG-TS relay rows require the
stream rather than the refusal, and the media tier package exists only with
it), the simulator (the tier graphs build `moq5-sys`'s `sim` tier rather
than requiring its refusal) and LOCMAF (the rows build with `moq5-media`'s
`locmaf` feature, which runs its LOCMAF rows and `moq5-sys`'s `locmaf`
tier, and the tier graphs build that tier rather than requiring its
refusal). Each defaults to what the tree builds (`MOQ_BUILD_PLAYBACK`,
`MOQ_BUILD_MPEG2TS`, `MOQ_BUILD_SIM`, `MOQ_BUILD_LOCMAF`), or for a named
SDK to the headers its own pkg-config
metadata places (`libmoq-service`'s includedir, whatever
`CMAKE_INSTALL_INCLUDEDIR` it was installed with; anything pkg-config prints
on its error stream there is a configure warning). A
statement the staged tree contradicts is a configure error, and every row
checks the statement against the SDK it links, so a stated feature the SDK
lacks, or a refusal of MPEG-TS it carries, fails the row.
`MOQ_RUST_QUINN_EXAMPLE=ON` adds the example's row; it is off by default
because it alone needs the crates.io registry, for the example's
dependencies, and `openssl`, for the relay's certificate.
`MOQ_RUST_RELAY_ROWS=OFF` leaves the relay rows out explicitly,
`MOQ_RUST_CROSS_BUILD=OFF` the cross-build row, `MOQ_RUST_PLAYER=OFF` the
player's builds and `MOQ_RUST_LOCMAF=OFF` LOCMAF's; a tree whose service has no
pkg-config file (the relay's direct MsQuic backend) names an SDK built
without one through `MOQ_RUST_SDK_PREFIX`, and an SDK whose pkg-config
closure spans its dependency prefixes (the WTQuic managed SDK Requires the
wtquic install's own `.pc` files) names their pkg-config directories
through `MOQ_RUST_SDK_PKG_CONFIG_PATH`. Any other missing input is a
configure error. `.github/workflows/rust.yml` runs the same rows on Ubuntu
and macOS against static and shared SDKs, and the relay rows and the
example's row against a picoquic SDK on both, every SDK built with the
player and MPEG-TS and both stated ON.

## Provenance

The crates build on Vitor Azevedo's contribution (`vitorgaz/moq5`, branch
`feat/rust-bindings`, commits `a6c01ef` and `20f6d9c`): the two-crate
architecture, the configuration builders, the result/error mapping, transfer-
on-success writes, owner-tagged handles and the CMAF module come from it.
