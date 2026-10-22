# Rust bindings

Two crates over libmoq's service tier:

- [`moq5-sys`](moq5-sys): raw FFI, generated from the installed headers with
  bindgen at build time. The header comments are the contract.
- [`moq5`](moq5): a safe API on top -- endpoint, media sender and receiver,
  the stream-terminal lane, CMAF parsing -- whose types enforce the ownership,
  teardown, sizing and threading rules the C API states in comments.

Both are `publish = false`; nothing is uploaded or reserved.

## Style

Handwritten Rust follows the
[Google Rust Style Guide](https://google.github.io/styleguide/rustguide.html),
including its library API design rules (`API_STYLE_GUIDE.md` section 13.1):
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

- An installed libmoq service SDK whose install carries
  `lib/pkgconfig/libmoq-service.pc`. The crate links the SDK's own pkg-config
  closure, so the supported SDK builds are exactly the ones that install that
  file: the service tier without adapters (the `install` preset), the
  picoquic raw-QUIC and PicoWT managed backends, and the WTQuic managed
  backends. A service built with mvfst, proxygen or the direct MsQuic managed
  backend has no pkg-config representation and is not linked by the crate;
  `moq5-sys` reports that case rather than guessing a link line.
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
libraries, or is selected with the `pkg-config` crate's own
`LIBMOQ_SERVICE_STATIC` / `LIBMOQ_SERVICE_DYNAMIC` variables; with neither
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
├── Cargo.toml          the product workspace: moq5-sys, moq5
├── moq5-sys/           bindgen crate; wrapper.h restates the constants bindgen
│                       cannot read; docs/bindings.rs is the docs.rs snapshot
├── moq5/               the safe crate; tests/compile_fail (fixtures cargo
│                       type-checks against the crate) and tests/live_*
│                       (the relay rows, #[ignore]d)
├── fake-tests/         its own workspace: a scripted C service provider
│                       (fake_service.c), the layout probe (layout_probe.c)
│                       and the deterministic rows; run.sh substitutes the
│                       provider through a generated libmoq-service.pc
├── consumer/           its own workspace: an out-of-tree consumer built in
│                       a clean environment against a static or shared SDK
├── scripts/            ctest_row.sh (every row), package_check.sh,
│                       stage_sdk.cmake, diagnostics.py (the diagnostic
│                       classifier) + the dependency allowlist,
│                       gate_controls.sh (the gates' negative and positive
│                       controls), sdk_paths.sh (link mode and loader paths
│                       from the SDK's pkg-config closure), update_snapshot.sh
└── CMakeLists.txt      the ctest rows (MOQ_BUILD_RUST_BINDING)
```

The test workspaces are excluded from the product workspace, so nothing they
depend on can unify into a consumer's build; the fake provider is reached
through the same pkg-config path a real SDK uses, never through a feature or
a runtime table.

## Qualification rows

With `-DMOQ_BUILD_RUST_BINDING=ON` (needs `MOQ_BUILD_TESTS=ON`,
`MOQ_BUILD_SERVICE=ON`, cargo and rustup), the tree registers ctest rows that
stage an SDK from the build and run `scripts/ctest_row.sh` for each:

| Row | What passes |
|---|---|
| `rust_sdk_install` | the tree installs into a test prefix that carries `libmoq-service.pc` (fixture) |
| `rust_fmt` | `cargo fmt --check` on the three workspaces |
| `rust_clippy` | `cargo clippy --all-targets -D warnings` on the three workspaces |
| `rust_doc` | `cargo doc --no-deps` under `missing_docs` and `-D warnings`, and the `cfg(docsrs)` snapshot without an SDK for each target the manifests restrict docs.rs to |
| `rust_tests` | the product tests: `moq5-sys` (the snapshot and the header preambles equal a fresh generation; the header comments and the `rcbuf.h` threading contract reach the documentation), the build script's cross sysroot rules (`build_sysroot.rs`: every root operand counts, every root must be one existing directory); `moq5`'s unit tests; the live harness's verdict on its native peer, with shell children (`tests/live_harness.rs`); and the compile-fail fixtures, checked by the cargo running the tests against the crate as the path dependency of a fresh scratch package: cargo's artifact record of the crate compiled in that run, each refused fixture's error with no artifact, the positive fixture's artifact with no error |
| `rust_fake_tests` | the deterministic rows on the scripted provider: configuration mapping, teardown order, write transfer, foreign handles, stamp refusal with cleanup, snapshots, the lane, durations, allocator balance, whole log lines; and the `Suite` exclusivity fixture (one shared across threads does not compile). The build's own diagnostics gate the row (its stderr classified: without `-vv` any diagnostic is this build's) |
| `rust_fake_asan` | the same rows with the provider compiled under AddressSanitizer and linked with that compiler's own runtime, which the provider's generated pkg-config file names right after the provider archive, before libc (linked after libc, GNU ld on aarch64 prints glibc's link warnings for the runtime's `tmpnam` interceptors); under the same diagnostic gate, where a linker message is a diagnostic |
| `rust_layout` | sizeof/offsetof of every record the safe crate reads or writes, from a C translation unit, against the generated types and the v0 floors |
| `rust_installed_consumer_static` / `_shared` | the out-of-tree consumer, cold-built in a clean environment with `cargo build -vv --message-format=json`, every diagnostic classified (`scripts/diagnostics.py`): an error, a diagnostic of the consumer's own crates, a manifest or unlocated warning, or a dependency warning that `scripts/dependency_diagnostics.allow` does not name by exact crate, version, source location and lint fails, and every diagnostic is inventoried beside the log (a row that passes with the accepted upstream warnings passes with warnings, which the inventory lists); every `rustc-link-search` directory inside the SDK's pkg-config closure with the SDK's own libdir among them exactly; no provider symbol (`nm`, required); run with the closure's loader path; the link mode follows the installed library type (the row matches the tree's) |
| `rust_package` | `cargo package` of both crates and their file lists, each step's stderr classified as well as its exit status checked (`scripts/cargo_step.sh`: cargo's own package warnings exit 0), and a consumer built in a clean environment from the extracted packages with `moq5-sys` resolved through `[patch.crates-io]`, under the same diagnostic and link-closure gates |
| `rust_gate_controls` | the gates' own controls (`scripts/gate_controls.sh`): the classifier passes a build whose only diagnostic is an allowed upstream warning while inventorying it, and refuses a manifest warning, an own or a dependency build-script warning, a dependency lint or codeless diagnostic not listed, the allowed lint at another location or in another version, an own warning, a dependency error at an allowed identity, an unlocated tool warning, and an allowlist with a crate-wide or location-free entry; link-mode selection for static-only, `.dylib`, `.so`, dual and empty libdirs, with the export clearing the opposite variable; the link-search assertion refusing a prefix of a path, a directory outside the closure and a missing line; a row refusing a missing SDK with the helper's reason; a row run with the opposite link-mode variable inherited stating the SDK's own mode to its builds, the opposite cleared; the diagnostics a build script prints under `-vv` (clang's, bindgen's `clang diag:`, located or not, on either stream) failing while echoed `cargo:` directives pass; and, with real cargo runs, `moq5-sys` checked against the SDK passing clean and failing when an unknown clang warning option makes libclang warn from its build script, and the package step refusing a package without metadata while passing the real crate |
| `rust_msrv` | `rustup run 1.85 cargo check --locked --all-targets` on the three workspaces |
| `rust_cross_refusal` | a cross build with the host SDK alone, a host search path (plain, or the generic `TARGET_`/`HOST_` spellings) beside a sysroot, a sysroot alone, a target SDK without a sysroot, a `--sysroot` spelled inside an unrelated define or in a spelling bindgen does not read, mixed `-isysroot`/`--sysroot` operands naming two roots in either order, or two sysroots that disagree is refused before any probe, `PKG_CONFIG_ALLOW_CROSS` notwithstanding; a target search configuration holding no SDK is refused by the probe with the host SDK on every inherited spelling (no fallback) |
| `rust_cross_build` | a target SDK configured from the tree (macOS: the other architecture), the crates checked for that target against it (`cargo check --target`; nothing is linked or run), the host SDK named on every spelling and proven unused: the link-search directories emitted are exactly the target SDK's libdir, and pkg-config, run through a recorder, was given exactly the target's directories as its library list and no search path on every probe; the same root named through `MOQ_SYSROOT` and both clang root options is accepted |
| `rust_relay_rows` | `moq5/tests/live_*` against a `moq5-relay` subprocess on loopback with the test-only credentials, drafts 22 and 18, one row per process; the status-propagation row drives a native publisher peer (`moq5/tests/support/done_peer.c`) that completes its subscription with a chosen PUBLISH_DONE status (the peer injects it; no expiry timer runs) and must then exit 0 (its stop checks the status, also when it exited first); the same row with the peer exiting 3 after its done must fail, on that exit, on both drafts; `MOQ5_RELAY_PEER_SANITIZE=thread` builds the peer under ThreadSanitizer, whose report (exit 66) fails the row |

`MOQ_RUST_RELAY_ROWS=OFF` leaves the relay rows out explicitly and
`MOQ_RUST_CROSS_BUILD=OFF` the cross-build row; a tree whose service has no
pkg-config file (the relay's direct MsQuic backend) names an SDK built
without one through `MOQ_RUST_SDK_PREFIX`, and an SDK whose pkg-config
closure spans its dependency prefixes (the WTQuic managed SDK Requires the
wtquic install's own `.pc` files) names their pkg-config directories
through `MOQ_RUST_SDK_PKG_CONFIG_PATH`. Any other missing input is a
configure error. `.github/workflows/rust.yml` runs the same rows on Ubuntu
and macOS against static and shared SDKs, and the relay rows against a
picoquic SDK on both.

## Provenance

The crates build on Vitor Azevedo's contribution (`vitorgaz/moq5`, branch
`feat/rust-bindings`, commits `a6c01ef` and `20f6d9c`): the two-crate
architecture, the configuration builders, the result/error mapping, transfer-
on-success writes, owner-tagged handles and the CMAF module come from it.
