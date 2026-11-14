# moq5-sys

Raw FFI bindings to libmoq, the MOQ5 Media over QUIC library, generated
with bindgen from the installed headers, by tier. The tiers are additive
features:

| Feature | Tier | Headers | Package |
|---|---|---|---|
| (none) | the core: the session, its vocabulary, the publisher and subscriber facades | `types`, `rcbuf`, `version`, `location`, `fill`, `cluster`, `session`, `transport_binding`, `connection_target`, `publisher`, `subscriber` | `libmoq-core` |
| `sim` | the deterministic simulator | `sim` | `libmoq-sim` |
| `media` | the media parsers | `loc`, `cmaf`, `media_object`, `msf` | `libmoq-media` |
| `service` (implies `media`) | the managed endpoint, media sender and receiver | `endpoint`, `media_auth`, `media_sender`, `media_receiver` | `libmoq-service` |
| `player` (implies `service`) | the media player and the playback configuration it takes | `playback` (that configuration), `media_player` | `libmoq-service` |

The core's codec headers (`codec`, `control`, `buf`, `kvp`, `wire`,
`vi64`) are not bound. Each tier is generated into a file of its own, in
tier order: every libmoq header outside the tier is blocklisted, so what a
tier references from a lower one is the lower tier's item, and the traits
bindgen asks about for those types are read from the lower tiers' generated
text. A tier's bindings are therefore the same whichever other tiers a
graph enables. A tier whose headers the SDK lacks fails the build, naming
the remedy (`MOQ_BUILD_SIM=ON`, `MOQ_BUILD_PLAYBACK=ON`, ...).

Each `wrapper_<tier>.h` restates the values bindgen cannot evaluate
(`UINT32_C()` constants, casts, the offsetof-based v0 floors) as
`static const` items whose prefix the build script strips, so they appear
under their C names; the core restates every such value of its headers.
The headers' comments are the documentation of every generated item
(clang parses them all, not only doc comments), and each header's file
preamble (for `rcbuf.h`, the threading contract: refcount operations are
not atomic, every reference to an rcbuf stays within one shard, and incref
aborts on overflow) is carried into the crate documentation as a section
per header; `tests/comments.rs` holds representative lifecycle, threading
and ownership contracts of each tier, and the preambles' phrases, to that.
For a safe API use [`moq5-core`](../moq5-core), [`moq5-media`](../moq5-media)
or the service crate [`moq5`](../moq5), which re-export these bindings as
`sys`.

## One native SDK per graph

Cargo unifies features across a dependency graph, so the build script binds
and links the union of the tiers every crate asked for, from one source:
the installed SDK, found through pkg-config. There is no other source and no
fallback. Each tier links through its own package: the core alone through
`libmoq-core`; the simulator through `libmoq-sim` and the media parsers
through `libmoq-media` (each `Requires` the core's); the service through
`libmoq-service`, whose `Requires` closure carries the core, the media
parsers and every managed provider the SDK expresses through pkg-config.
`libmoq.pc` is never probed for a lower tier: it names every component the
SDK was built with, adapters included. Every package the graph needs must
resolve, and the whole SDK-owned metadata the graph depends on -- the
packages probed and every `libmoq.pc` and `libmoq-*.pc` their `Requires`
and `Requires.private` name, transitively (for the service, its aggregate
`libmoq.pc` and the managed adapters' packages) -- must come from one
pkg-config directory and report one version. That metadata is only read:
it adds no link line, and the aggregate is never linked for a lower tier.
External providers (wtquic, MsQuic, OpenSSL) keep their own prefixes and
versions. A missing package, metadata split across two SDKs, or packages
that disagree on their version fails the build, naming what was asked for,
what was found and the remedy. The build script prints which tiers it bound,
from which directory and why (shown by `cargo build -vv`, and written to
`provenance.txt` beside the bindings). It declares `links = "moq"` and
exports the include directories to dependents as `DEP_MOQ_INCLUDE`.

- `MOQ_PREFIX`: an install prefix whose `lib/pkgconfig` (and `lib64/pkgconfig`
  when present) goes first on `PKG_CONFIG_PATH`.
- `LIBMOQ_CORE_STATIC`, `LIBMOQ_SIM_STATIC`, `LIBMOQ_MEDIA_STATIC`,
  `LIBMOQ_SERVICE_STATIC` (each with its `_DYNAMIC` form, and the
  `PKG_CONFIG_ALL_*` forms): the `pkg-config` crate's static/shared
  selection for each package it probes. Unset, the installed libraries
  decide which kind links, but pkg-config then reports only the `.pc` files'
  public `Libs`: an SDK installed static whose `.pc` files keep dependencies
  in `Libs.private` (the WTQuic managed SDK's `wtquic-msquic.pc` keeps
  msquic there) links only with `LIBMOQ_SERVICE_STATIC=1`.
- `PKG_CONFIG_PATH`: an SDK whose pkg-config closure spans its dependency
  prefixes (the WTQuic managed SDK Requires the wtquic install's own `.pc`
  files) needs their pkg-config directories on it beside `MOQ_PREFIX`.
- `LIBCLANG_PATH`, `BINDGEN_EXTRA_CLANG_ARGS`: where libclang is, and extra
  arguments for it (the include directory of a libclang installed without
  its resource headers).

The build script reruns when any of these, or `DOCS_RS` and `RUSTFMT`,
changes. A source build of the tiers is not offered by this crate.

## Cross builds

A build whose `--target` differs from the host resolves its inputs once,
before anything is probed. With `<target>` the target triple (or the triple
with `-` as `_`, as the `pkg-config` crate spells it), two inputs are
required:

- the target search configuration: the pkg-config directories of
  `MOQ_PREFIX_<target>`, and `PKG_CONFIG_PATH_<target>` and
  `PKG_CONFIG_LIBDIR_<target>`. Only these spellings, which name the target,
  count. Together they become the whole search path, handed to pkg-config as
  its library directory list (which replaces its default search path), and
  every other spelling of the path and libdir variables is removed from the
  probe: an inherited plain, `HOST_` or `TARGET_` one (`PKG_CONFIG_PATH`,
  `HOST_PKG_CONFIG_PATH`, `TARGET_PKG_CONFIG_PATH`, `TARGET_PKG_CONFIG_LIBDIR`)
  is never consulted, and a sysroot alone names no search directory;
- the target sysroot libclang parses the headers against, from every input
  the build uses: `MOQ_SYSROOT_<target>`; the sysroot pkg-config applies
  (`PKG_CONFIG_SYSROOT_DIR`, as the `pkg-config` crate reads it:
  `_<target>`, then `TARGET_`, then plain); and every `--sysroot`/`-isysroot`
  argument among the clang arguments bindgen passes
  (`BINDGEN_EXTRA_CLANG_ARGS_<target>`, else the plain variable; bindgen
  reads no `TARGET_` spelling), parsed as arguments: a `--sysroot` inside
  another option's text is not one, and each root operand counts, not the
  last, since clang takes headers from `-isysroot` over `--sysroot`. At
  least one is required, and every sysroot named must be the same
  directory.

Without either, or with sysroots that disagree, the build is refused with the
missing input named; `PKG_CONFIG_ALLOW_CROSS` on its own is not support (the
validated pair is what permits the probe), and bindgen runs with
`--target=<target>` and that sysroot. The `rust_cross_refusal` and
`rust_cross_build` rows exercise the refusals and a target SDK built from the
tree, against which the crates are checked (`cargo check --target`), not
linked or run.

## Supported SDKs and the qualified baseline

Supported SDK builds are the ones that install the tier packages a graph
needs: `libmoq-core.pc` (every SDK of this tree), `libmoq-sim.pc`
(`MOQ_BUILD_SIM=ON`), `libmoq-media.pc` (all five media parsers, MPEG-TS
included) and, for the service, `libmoq-service.pc`: the service tier
without adapters, picoquic raw-QUIC plus PicoWT managed, and the WTQuic
managed backends. A service built with mvfst, proxygen or the direct MsQuic
managed backend installs no `libmoq-service.pc` and is not linked; the
build script reports what it found (no pkg-config, or no package on the
search path, and whether `libmoq.pc` resolved there) with the remedies,
never a guessed link line.

Compatibility is by declaration, not version number: the build fails when a
declaration or constant the safe crates use is missing from the installed
headers (the tier wrappers restate the ones bindgen cannot evaluate), the layout
row compares every record with the C compiler's view, and appended fields in
a newer SDK are tolerated by the `struct_size` contract. The qualified
baseline is the libmoq tree these crates are developed in, at the commit
that carries them, installed as: the no-adapter service SDK
(`MOQ_BUILD_MSF=ON`, `MOQ_BUILD_MEDIA_OBJECT=ON`, `MOQ_BUILD_SERVICE=ON`,
static and `BUILD_SHARED_LIBS=ON`) with the player, MPEG-TS and the
simulator (`MOQ_BUILD_PLAYBACK=ON`, `MOQ_BUILD_MPEG2TS=ON`,
`MOQ_BUILD_SIM=ON`); the static one also installed with its headers under
`headers/` (`CMAKE_INSTALL_INCLUDEDIR`: the build finds them, as everything
here does, where the SDK's pkg-config metadata places them); the picoquic
raw-QUIC plus PicoWT managed SDK (`MOQ_BUILD_ADAPTER_PICOQUIC`,
`MOQ_BUILD_PQ_THREADED`, `MOQ_BUILD_ADAPTER_PICO_WT`,
`MOQ_BUILD_PICO_WT_MANAGED`) with the player and MPEG-TS; and the WTQuic
managed SDK (`MOQ_BUILD_ADAPTER_WTQUIC`, `MOQ_BUILD_WTQUIC_MSQUIC_MANAGED`)
without them. The rows of [`../README.md`](../README.md) ran on this
matrix; each cell says how far that configuration was taken. Where the SDK
carries the player, every build ran with and without the `player` feature:

| Host | SDK | Rows run | Snapshot equality | Cross check |
|---|---|---|---|---|
| macOS 15, aarch64, Apple clang, Homebrew LLVM libclang | no-adapter with the player, MPEG-TS and the simulator, static | all, natively, and once more as the CI workflow runs them | `aarch64-apple-darwin`, every tier's, the player's too | `x86_64-apple-darwin`: a target SDK built from the tree with the player, MPEG-TS and the simulator, the crates checked against it without and with the player and moq5-sys's simulator tier with it, not linked or run |
| macOS 15, aarch64 | no-adapter with the player, MPEG-TS and the simulator, shared | all, natively, as the CI workflow runs them | as above | as above |
| macOS 15, aarch64 | WTQuic managed (MsQuic) with the simulator, static (`MOQ_RUST_SDK_PKG_CONFIG_PATH` for the wtquic install), without the player and MPEG-TS, both stated | all, natively, the player's builds left out | `aarch64-apple-darwin` | refusal row only |
| macOS 15, aarch64 | no-adapter with the player, MPEG-TS and the simulator, static, its headers installed under `headers/` (`CMAKE_INSTALL_INCLUDEDIR=headers`, `MOQ_RUST_SDK_PREFIX`) | all, natively | `aarch64-apple-darwin`, every tier's, the player's too | refusal row only |
| macOS 15, aarch64 | picoquic plus PicoWT managed with the player and MPEG-TS, without the simulator, static (`MOQ_RUST_SDK_PREFIX`), beside the relay tree | all and the relay rows on drafts 22 and 18, natively | `aarch64-apple-darwin`, the player's too | refusal row only |
| Ubuntu 24.04 container, aarch64, clang | no-adapter with the player, MPEG-TS and the simulator, static and shared | all, natively | `aarch64-unknown-linux-gnu`, every tier's, the player's too | refusal row only |
| Ubuntu 24.04 container, aarch64, gcc (the SDK, the relay, and the rows' C: the CTest environment carries the tree's compiler) | picoquic plus PicoWT managed with the player and MPEG-TS, without the simulator, static (`MOQ_RUST_SDK_PREFIX`), beside the relay tree | all and the relay rows on drafts 22 and 18, natively | `aarch64-unknown-linux-gnu`, the player's too | refusal row only |

The macOS rows ran with the libclang of Homebrew's LLVM 22.1.4, the Linux
rows with Ubuntu 24.04's libclang 18.1.3. The `rust_doc` row builds the
`cfg(docsrs)` snapshots, the player's included, for all three docs.rs
targets on every host above. Unrun: Windows (any configuration), Linux
x86_64 (natively or under emulation, any SDK), macOS x86_64 at run time
(checked only), the picoquic SDK without the player and MPEG-TS, the WTQuic
managed SDK on Linux or with the player or MPEG-TS, the relay rows' native
peer under ThreadSanitizer, libclang 23, and every target outside the
docs.rs list.

## The docs.rs snapshots

`docs/bindings_<tier>.rs` is a snapshot of each tier's generated bindings,
and `docs/preambles_<tier>.md` of its header preambles, compiled only under
`cfg(docsrs)`, where no SDK exists to generate them from; the core's always,
each other tier's under its feature. docs.rs documents this crate with
every tier (`sim` and `player`). Each bindings snapshot's header records
the libmoq commit, the packages the generating build linked and their
`Requires`, and the generating target and tooling (bindgen 0.73, the
libclang and its version, the output formatted by the toolchain's
rustfmt); `scripts/update_snapshot.sh <sdk-prefix> [tier...]` regenerates
them from a build against an installed SDK, every tier by default, from
the generation its own cargo invocation records for `moq5-sys` (never
another feature set's build in the same target directory).
`tests/snapshot.rs` holds each enabled tier's pair equal to a fresh
generation on every target the tests run on: the preambles byte for byte,
and the bindings line for line, every declaration, attribute, layout
assertion and documentation attribute, with one exception. A constant
bindgen made from a macro may carry its documentation on one side only.
libclang 23 attaches the comment before a macro definition to the macro
under `-fparse-all-comments`, earlier releases attach none, and no snapshot
can equal both. The build script lists the constants each generation made
from macros beside it (never a value a wrapper restates, which is a
declaration, nor an enumerator: the test holds the list to that), and where
both sides document such a constant the texts must agree. A difference is
reported with both sides' lines, the snapshot's provenance and the libclang
the build used. The snapshots carry those targets' layouts (their layout
assertions are compiled into every build), so the manifests restrict
docs.rs to them (`x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`,
`aarch64-apple-darwin`, the targets the `rust_doc` row builds them for); a
target outside that list is not documented rather than documented from a
layout it does not have.

Layout tests for every type are compiled into the crate; every function that
returns `moq_result_t` is `#[must_use]`.

## License

Apache-2.0
