#!/usr/bin/env bash
#
# setup_wtquic_deps.sh - pinned wtquic setup for the wtquic adapter
# (CI and reproducible local dev).
#
# libmoq's wtquic adapter consumes wtquic as an installed CMake package
# (find_package(wtquic CONFIG COMPONENTS msquic)). This script
# materializes that install at a KNOWN-GOOD pinned commit so CI and
# developers consume the same dependency input instead of relying on
# whatever happens to be checked out next to the repo: it clones
# (shallow, by exact commit) wtquic into a deterministic deps dir,
# builds it, installs it into a private prefix, and prints the values
# the libmoq configure needs:
#
#     CMAKE_PREFIX_PATH=<prefix>[:<existing CMAKE_PREFIX_PATH>]
#     wtquic_DIR=<prefix>/lib/cmake/wtquic     (when that path exists)
#
# Then configure libmoq, e.g.:
#
#     # Sets the variables in the CURRENT shell (KEY=VALUE, not
#     # `export`); pass them explicitly to cmake in the same shell.
#     eval "$(scripts/setup_wtquic_deps.sh)"
#     cmake -S . -B build-wtquic \
#         -DMOQ_BUILD_ADAPTER_WTQUIC=ON \
#         -DCMAKE_PREFIX_PATH="$CMAKE_PREFIX_PATH"
#
# Apple only - the managed Network.framework MoQ client
# (moq_wtquic_network_managed) additionally needs wtquic's `network` component,
# which this script builds by default on macOS. Its canonical configure is:
#
#     eval "$(scripts/setup_wtquic_deps.sh)"
#     cmake -S . -B build-wtquic \
#         -DMOQ_BUILD_ADAPTER_WTQUIC=ON \
#         -DMOQ_BUILD_WTQUIC_NETWORK_MANAGED=ON \
#         -DCMAKE_PREFIX_PATH="$CMAKE_PREFIX_PATH"
#
# The values are also written to <deps>/wtquic_deps.env (source-able)
# and, under GitHub Actions, appended to $GITHUB_ENV so later steps see
# them.
#
# wtquic's MsQuic backend is REQUIRED here (the libmoq adapter needs
# the msquic component), so MsQuic must be discoverable through
# wtquic's own mechanisms: an installed msquic CMake package (e.g.
# Homebrew libmsquic), or WTQ_MSQUIC_ROOT / msquic_DIR, both forwarded
# when set. The Network.framework backend (`network` component) is
# additionally built by default on Apple for the managed NF client and
# is skipped elsewhere (it is Apple-only); see WTQ_BUILD_NETWORK.
#
# Env overrides (all optional):
#   WTQUIC_REPO       wtquic remote    (default: rwl4/wtquic)
#   WTQUIC_REF        wtquic commit    (default: pinned below)
#   WTQUIC_DEPS_DIR   deps root        (default: <repo>/.deps/wtquic-ci)
#   CMAKE_BUILD_TYPE  wtquic build type (default: Release)
#   WTQ_BUILD_NETWORK build the Network.framework `network` component
#                     (default: ON on Apple, OFF elsewhere; Apple-only)
#   WTQ_FETCH_ONLY    =1 -> materialize the pinned checkout and exit
#                     (no configure/build/install, no MsQuic needed) --
#                     for consumers that cross-compile wtquic themselves,
#                     e.g. scripts/build_ios_xcframework.sh
#   WTQ_MSQUIC_ROOT   forwarded to wtquic's configure if set
#   msquic_DIR        forwarded to wtquic's configure if set
#
# Requires: git, cmake, a C compiler, and a discoverable MsQuic.
# (WTQ_FETCH_ONLY=1 needs only git: it stops after the pinned checkout.)

set -euo pipefail

# -- Pinned, known-good dependency commit ------------------------------
WTQUIC_REPO="${WTQUIC_REPO:-https://github.com/rwl4/wtquic.git}"
# Adds the symmetric WebTransport-over-H3 wire-profile selector on BOTH sides:
# the client (wtq_connect_config_t.webtransport_profile) and the server/listener
# (wtq_msquic_listener_cfg_t.webtransport_profile), each a sized ABI tail
# defaulting to H3_CURRENT (draft ":protocol = webtransport-h3"). The explicit
# H3_DRAFT_13_14_COMPAT profile emits the drafts-13/14 ":protocol = webtransport"
# token + max-sessions signal (what proxygen/moxygen/moqx and the picoquic
# h3zero family speak) and a compat listener accepts ONLY that matching dialect
# -- one profile per connection, never mixed, never auto-negotiated. libmoq
# selects the profile explicitly on both its wtquic-msquic client and server
# paths. Atop the MsQuic managed-domain contract (guard / paired admission /
# transport-quiescence), the wtquic-msquic.pc MsQuic library-dir fix, the
# client-connect abandon path, and the Network.framework ready-transition
# workarounds.
#
# Also adds the Network-backend NATIVE DELAYED DOORBELL:
# wtq_nw_conn_doorbell_ring_after(conn, delay_us) / _doorbell_cancel_after(conn)
# -- a preallocated, replace-latest delayed wake (host-uptime relative delay, no
# per-arm allocation, nonblocking, never fires during/after on_stopped). libmoq's
# wtquic-Network managed facade uses it to wake an otherwise-idle loop at a
# service deadline (periodic catalog refresh), with no adapter-owned timer.
#
# This is origin/main's tip. The wtquic repo squashes history to a single
# "Initial commit"; pin the exact branch-tip SHA (fetchable via a full fetch).
WTQUIC_REF="${WTQUIC_REF:-87a937ee22cf2398c677bbe323edd1a6880a3023}"

script_dir=$(cd "$(dirname "$0")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)
WTQUIC_DEPS_DIR="${WTQUIC_DEPS_DIR:-$repo_root/.deps/wtquic-ci}"
CMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE:-Release}"

# The Network.framework backend (wtquic's `network` component, wtq::network)
# powers the managed NF client (libmoq's MOQ_BUILD_WTQUIC_NETWORK_MANAGED). It is
# APPLE-ONLY: wtquic's WTQ_BUILD_NETWORK is a FATAL error off Apple, so default
# it ON only on Darwin (override explicitly with WTQ_BUILD_NETWORK=ON/OFF). The
# MsQuic backend stays required on every platform, so the non-Apple path is
# unchanged.
case "$(uname -s)" in
    Darwin) WTQ_BUILD_NETWORK="${WTQ_BUILD_NETWORK:-ON}" ;;
    *)      WTQ_BUILD_NETWORK="${WTQ_BUILD_NETWORK:-OFF}" ;;
esac

wtquic_dir="$WTQUIC_DEPS_DIR/wtquic"
wtquic_build="$WTQUIC_DEPS_DIR/build"
wtquic_prefix="$WTQUIC_DEPS_DIR/prefix"

# Only the KEY=VALUE result lines may reach real stdout (so callers can
# `eval "$(...)"`). Route everything else - including git/cmake chatter,
# which prints to stdout - to stderr via fd 3.
exec 3>&1 1>&2

log() { printf '[setup_wtquic_deps] %s\n' "$*" >&2; }
die() { printf '[setup_wtquic_deps] ERROR: %s\n' "$*" >&2; exit 1; }

command -v git   >/dev/null 2>&1 || die "git not found"
command -v cmake >/dev/null 2>&1 || die "cmake not found"

# Fetch a repo at an exact commit into $dir (idempotent, shallow when
# the server allows fetch-by-SHA, which GitHub does).
fetch_at() {
    local repo=$1 ref=$2 dir=$3
    if [ ! -d "$dir/.git" ]; then
        log "init $dir"
        mkdir -p "$dir"
        git -C "$dir" init -q
        git -C "$dir" remote add origin "$repo"
    fi
    git -C "$dir" remote set-url origin "$repo"
    if git -C "$dir" cat-file -e "${ref}^{commit}" 2>/dev/null; then
        log "$dir already has $ref"
    elif git -C "$dir" fetch --depth 1 origin "$ref" 2>/dev/null; then
        log "fetched $ref (shallow) from $repo"
    else
        log "shallow fetch unavailable; full fetch from $repo"
        git -C "$dir" fetch origin
    fi
    git -C "$dir" checkout -q --detach "$ref"
    local got
    got=$(git -C "$dir" rev-parse HEAD)
    [ "$got" = "$ref" ] || die "checkout mismatch in $dir: want $ref got $got"
    # REPRODUCIBILITY: a pinned checkout must be exactly the pinned
    # tree. Tracked modifications, staged changes, and untracked files
    # all disqualify it -- a dirty checkout at the right HEAD would
    # still build unpinned sources.
    local dirty
    dirty=$(git -C "$dir" status --porcelain 2>/dev/null | head -5)
    if [ -n "$dirty" ]; then
        die "pinned checkout $dir is DIRTY at $ref:
$dirty
A pinned dependency build refuses modified/staged/untracked content.
Remove $dir (it will be re-materialized) or clean it, then re-run."
    fi
}

mkdir -p "$WTQUIC_DEPS_DIR"
log "deps dir: $WTQUIC_DEPS_DIR"

fetch_at "$WTQUIC_REPO" "$WTQUIC_REF" "$wtquic_dir"

if [ "${WTQ_FETCH_ONLY:-0}" = "1" ]; then
    log "fetch-only: pinned checkout ready at $wtquic_dir"
    exit 0
fi

# -- Configure / build / install wtquic --------------------------------
# The libmoq adapter needs only the libraries and the CMake package:
# wtquic's tests, fuzzers, benchmarks and examples stay off. The MsQuic
# backend is required - a missing MsQuic is a configure error here, not
# a silent core-only install that libmoq's adapter configure would then
# reject with a less direct message.
log "configuring wtquic -> $wtquic_build ($CMAKE_BUILD_TYPE)"
cmake_args=(
    -S "$wtquic_dir" -B "$wtquic_build"
    -DCMAKE_BUILD_TYPE="$CMAKE_BUILD_TYPE"
    -DCMAKE_INSTALL_PREFIX="$wtquic_prefix"
    -DWTQ_REQUIRE_MSQUIC=ON
    -DWTQ_BUILD_NETWORK="$WTQ_BUILD_NETWORK"
    -DWTQ_BUILD_TESTS=OFF
    -DWTQ_BUILD_SIM=OFF
    -DWTQ_BUILD_BENCHMARKS=OFF
    -DWTQ_BUILD_EXAMPLES=OFF
)
if [ -n "${WTQ_MSQUIC_ROOT:-}" ]; then
    cmake_args+=(-DWTQ_MSQUIC_ROOT="$WTQ_MSQUIC_ROOT")
fi
if [ -n "${msquic_DIR:-}" ]; then
    cmake_args+=(-Dmsquic_DIR="$msquic_DIR")
fi
cmake "${cmake_args[@]}" >/dev/null

log "building wtquic"
cmake --build "$wtquic_build" \
    -j"$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2)" >/dev/null

log "installing wtquic -> $wtquic_prefix"
cmake --install "$wtquic_build" >/dev/null

# Sanity: the package the libmoq adapter resolves must exist.
wtquic_pkg_dir="$wtquic_prefix/lib/cmake/wtquic"
[ -f "$wtquic_pkg_dir/wtquicConfig.cmake" ] \
    || die "install produced no wtquicConfig.cmake in $wtquic_pkg_dir"
# When the Network.framework backend was requested, its component must have
# installed too (libmoq's MOQ_BUILD_WTQUIC_NETWORK_MANAGED resolves
# find_package(wtquic COMPONENTS network)).
if [ "$WTQ_BUILD_NETWORK" = "ON" ] \
    && [ ! -f "$wtquic_pkg_dir/wtquic-networkTargets.cmake" ]; then
    die "WTQ_BUILD_NETWORK=ON but the network component (wtq::network) was not installed"
fi

# -- Emit the configure inputs ------------------------------------------
# stdout (fd 3) and the env file carry Bash-source/eval-safe assignments:
# the values are %q-escaped so paths with spaces or shell metacharacters
# survive `eval "$(setup_wtquic_deps.sh)"` and `. wtquic_deps.env`.
# Nothing else reaches fd 3, so the eval consumes exactly these lines.
result_prefix_path="$wtquic_prefix"
if [ -n "${CMAKE_PREFIX_PATH:-}" ]; then
    result_prefix_path="$wtquic_prefix:$CMAKE_PREFIX_PATH"
fi

env_file="$WTQUIC_DEPS_DIR/wtquic_deps.env"
{
    printf 'CMAKE_PREFIX_PATH=%q\n' "$result_prefix_path"
    printf 'wtquic_DIR=%q\n'        "$wtquic_pkg_dir"
} | tee "$env_file" >&3

# GitHub Actions: make them available to subsequent steps. $GITHUB_ENV is
# NOT a shell file - Actions parses it as raw NAME=value env-file lines,
# so these stay unescaped (unlike the eval/source output above).
if [ -n "${GITHUB_ENV:-}" ]; then
    {
        printf 'CMAKE_PREFIX_PATH=%s\n' "$result_prefix_path"
        printf 'wtquic_DIR=%s\n'        "$wtquic_pkg_dir"
    } >> "$GITHUB_ENV"
fi

log "done. env written to $env_file"
