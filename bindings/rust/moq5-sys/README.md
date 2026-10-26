# moq5-sys

Raw FFI bindings to libmoq, the MOQ5 Media over QUIC library, generated with
bindgen from the installed service-tier headers (`types`, `rcbuf`, `version`,
`endpoint`, `cmaf`, `msf`, `media_object`, `media_sender`, `media_receiver`),
and with the `player` feature from the player's (`playback`,
`media_player`), generated into a second file beside the base bindings
that reuses the base items rather than generating them again. The feature
needs an SDK built with the player
(`MOQ_BUILD_PLAYBACK=ON`); the build fails, naming that remedy, without one.
The headers' comments are the documentation of every generated item (clang
parses them all, not only doc comments), and each header's file preamble
(for `rcbuf.h`, the threading contract: refcount operations are not atomic,
every reference to an rcbuf stays within one shard, and incref aborts on
overflow) is carried into the crate documentation as a section per header;
`tests/comments.rs` holds representative lifecycle, threading and ownership
contracts, and the preamble's phrases, to that. For a safe API use the
[`moq5`](../moq5) crate, which re-exports these bindings as `moq5::sys`.

The crate links the SDK through pkg-config on `libmoq-service`, whose
`Requires` closure carries every library the service was built with. It
declares `links = "moq"` and exports the include directories to dependents
as `DEP_MOQ_INCLUDE`.

- `MOQ_PREFIX`: an install prefix whose `lib/pkgconfig` (and `lib64/pkgconfig`
  when present) goes first on `PKG_CONFIG_PATH`.
- `LIBMOQ_SERVICE_STATIC` / `LIBMOQ_SERVICE_DYNAMIC` (and the
  `PKG_CONFIG_ALL_*` forms): the `pkg-config` crate's static/shared selection.
  Unset, the installed libraries decide which kind links, but pkg-config
  then reports only the `.pc` files' public `Libs`: an SDK installed static
  whose `.pc` files keep dependencies in `Libs.private` (the WTQuic managed
  SDK's `wtquic-msquic.pc` keeps msquic there) links only with
  `LIBMOQ_SERVICE_STATIC=1`.
- `PKG_CONFIG_PATH`: an SDK whose pkg-config closure spans its dependency
  prefixes (the WTQuic managed SDK Requires the wtquic install's own `.pc`
  files) needs their pkg-config directories on it beside `MOQ_PREFIX`.
- `LIBCLANG_PATH`, `BINDGEN_EXTRA_CLANG_ARGS`: where libclang is, and extra
  arguments for it (the include directory of a libclang installed without
  its resource headers).

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

Supported SDK builds are the ones that install `libmoq-service.pc`: the
service tier without adapters, picoquic raw-QUIC plus PicoWT managed, and the
WTQuic managed backends. A service built with mvfst, proxygen or the direct
MsQuic managed backend installs no `.pc` and is not linked; the build script
reports what it found (no pkg-config, no `.pc` on the search path, or a core
`.pc` without a service one) with the remedies, never a guessed link line.

Compatibility is by declaration, not version number: the build fails when a
declaration or constant the safe crate uses is missing from the installed
headers (`wrapper.h` restates the ones bindgen cannot evaluate), the layout
row compares every record with the C compiler's view, and appended fields in
a newer SDK are tolerated by the `struct_size` contract. The qualified
baseline is libmoq commit `174b6428` (the tree these crates are developed
in), installed as: the no-adapter service SDK (`MOQ_BUILD_MSF=ON`,
`MOQ_BUILD_MEDIA_OBJECT=ON`, `MOQ_BUILD_SERVICE=ON`, static and
`BUILD_SHARED_LIBS=ON`), the picoquic raw-QUIC plus PicoWT managed SDK
(`MOQ_BUILD_ADAPTER_PICOQUIC`, `MOQ_BUILD_PQ_THREADED`,
`MOQ_BUILD_ADAPTER_PICO_WT`, `MOQ_BUILD_PICO_WT_MANAGED`), each of those
with the player and MPEG-TS (`MOQ_BUILD_PLAYBACK=ON`,
`MOQ_BUILD_MPEG2TS=ON`), the picoquic SDK also without them, the static
no-adapter SDK with them also installed with its headers under `headers/`
(`CMAKE_INSTALL_INCLUDEDIR`: the build finds them, as everything here does,
where the SDK's pkg-config metadata places them), and the WTQuic managed SDK
(`MOQ_BUILD_ADAPTER_WTQUIC`, `MOQ_BUILD_WTQUIC_MSQUIC_MANAGED`) without
them. The rows of [`../README.md`](../README.md) ran on this matrix;
each cell says how far that configuration was taken. Where the SDK carries
the player, every build ran with and without the `player` feature:

| Host | SDK | Rows run | Snapshot equality | Cross check |
|---|---|---|---|---|
| macOS 15, aarch64, Apple clang, Homebrew LLVM libclang | no-adapter with the player and MPEG-TS, static | all, natively | `aarch64-apple-darwin`, the player's too | `x86_64-apple-darwin`: a target SDK built from the tree with the player and MPEG-TS, the crates checked against it without and with the player, not linked or run |
| macOS 15, aarch64 | no-adapter with the player and MPEG-TS, shared | all, natively | as above | as above |
| macOS 15, aarch64 | WTQuic managed, static (`MOQ_RUST_SDK_PKG_CONFIG_PATH` for the wtquic install), without the player and MPEG-TS, both stated | all, natively, the player's builds left out | `aarch64-apple-darwin` | refusal row only |
| macOS 15, aarch64 | no-adapter with the player and MPEG-TS, static, its headers installed under `headers/` (`CMAKE_INSTALL_INCLUDEDIR=headers`, `MOQ_RUST_SDK_PREFIX`) | all, natively | `aarch64-apple-darwin`, the player's too | refusal row only |
| macOS 15, aarch64 | picoquic plus PicoWT managed with the player and MPEG-TS, static, from the relay tree (`MOQ_RUST_SDK_PREFIX`) | all and the relay rows on drafts 22 and 18, natively; the relay rows once more with the native peer under ThreadSanitizer | `aarch64-apple-darwin`, the player's too | refusal row only |
| macOS 15, aarch64 | picoquic plus PicoWT managed without the player and MPEG-TS, both stated, static | the relay rows on drafts 22 and 18 (the MPEG-TS rows requiring the refusal, the player's left out) | -- | -- |
| Ubuntu 24.04 container, aarch64, clang | no-adapter with the player and MPEG-TS, static and shared | all, natively | `aarch64-unknown-linux-gnu`, the player's too | refusal row only |
| Ubuntu 24.04 container, aarch64, gcc (the SDK, the relay, and the rows' C: the CTest environment carries the tree's compiler) | picoquic plus PicoWT managed with the player and MPEG-TS, static, from the relay tree | all and the relay rows on drafts 22 and 18, natively | `aarch64-unknown-linux-gnu`, the player's too | refusal row only |
| Ubuntu 24.04 container, x86_64 under emulation on an aarch64 host, clang | no-adapter with the player and MPEG-TS, static | all, under emulation | `x86_64-unknown-linux-gnu`, the player's too | refusal row only |

The macOS rows ran with the libclang of Homebrew's LLVM 22.1.4, and on the
first, second and fifth configurations once more with libclang 23.1.4 (an
llvm-project release build); the snapshot test also ran with 23.1.3.
The `rust_doc` row builds the `cfg(docsrs)` snapshots, the player's
included, for all three docs.rs targets on every host above. Unrun: Windows
(any configuration), Linux x86_64 with a shared or a picoquic SDK, native
(unemulated) x86_64 Linux hardware, macOS x86_64 at run time (checked only),
the WTQuic managed SDK on Linux or with the player or MPEG-TS, the native
peer under ThreadSanitizer on Linux, and every target outside the docs.rs
list.

## The docs.rs snapshot

`docs/bindings.rs` is a snapshot of the generated bindings, and
`docs/preambles.md` of the header preambles, compiled only under
`cfg(docsrs)`, where no SDK exists to generate them from;
`docs/bindings_player.rs` and `docs/preambles_player.md` are the player's,
compiled with the `player` feature, which both manifests document. The
bindings'
header records the libmoq commit, the SDK's `Requires` closure and the
generating target and tooling (bindgen 0.73, the libclang and its version,
the output formatted by the toolchain's rustfmt, on which rustfmt 1.9 and
1.10 agree byte for byte); `scripts/update_snapshot.sh <sdk-prefix>`
regenerates all four from a build against an installed SDK with the player.
`tests/snapshot.rs` holds them equal to a fresh generation on every target
the tests run on (the player's pair under the feature): the preambles byte
for byte, and the bindings line for line, every declaration, attribute,
layout assertion and documentation attribute, with one exception. A
constant bindgen made from a macro may carry its documentation on one side
only. libclang 23 attaches the comment before a macro definition to the
macro under `-fparse-all-comments`, earlier releases attach none, and no
snapshot can equal both. The build script lists the constants each
generation made from macros beside it (never a value wrapper.h restates,
which is a declaration, nor an enumerator: the test holds the list to
that), and where both sides document such a constant the texts must agree.
A difference is reported with both sides' lines, the snapshot's provenance
and the libclang the build used. The snapshots
carry those targets' layouts (their layout assertions are compiled into
every build), so both manifests restrict docs.rs to them
(`x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`,
`aarch64-apple-darwin`, the targets of the matrix above); a target outside
that list is not documented rather than documented from a layout it does not
have.

Layout tests for every type are compiled into the crate; every function that
returns `moq_result_t` is `#[must_use]`.

## License

Apache-2.0
