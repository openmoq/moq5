#!/usr/bin/env bash
#
# setup_picoquic_deps.sh - pinned picoquic + picotls setup for the
# picoquic adapter (CI and reproducible local dev).
#
# libmoq's picoquic adapter resolves picoquic in "source-tree mode" (see
# cmake/FindPicoquic.cmake): it needs a picoquic source checkout and a
# built picotls. This script materializes both at KNOWN-GOOD pinned
# commits so CI and developers consume the same dependency input instead
# of relying on whatever happens to be checked out next to the repo.
#
# It clones (shallow, by exact commit) picoquic and picotls into a
# deterministic deps dir, builds picotls, and prints the two values the
# libmoq configure needs:
#
#     MOQ_PICOQUIC_SOURCE_DIR=<deps>/picoquic
#     MOQ_PICOTLS_PREFIX=<deps>/picotls/build
#
# Then configure libmoq, e.g.:
#
#     # Sets MOQ_PICOQUIC_SOURCE_DIR / MOQ_PICOTLS_PREFIX as shell
#     # variables in the CURRENT shell (KEY=VALUE, not `export`); pass
#     # them explicitly to cmake in the same shell as below.
#     eval "$(scripts/setup_picoquic_deps.sh)"
#     cmake -B build/pq-ci \
#         -DMOQ_BUILD_ADAPTER_PICOQUIC=ON \
#         -DMOQ_BUILD_PQ_THREADED=ON \
#         -DMOQ_BUILD_TESTS=ON \
#         -DMOQ_PICOQUIC_SOURCE_DIR="$MOQ_PICOQUIC_SOURCE_DIR" \
#         -DMOQ_PICOTLS_PREFIX="$MOQ_PICOTLS_PREFIX"
#
# The two vars are also written to <deps>/picoquic_deps.env (source-able)
# and, under GitHub Actions, appended to $GITHUB_ENV so later steps see
# them. Because the printed lines are KEY=VALUE (not `export`), `eval`
# only sets them for the current shell - it does not export them to child
# processes, which is why the example passes them via -D... explicitly.
#
# Env overrides (all optional):
#   MOQ_DEPS_DIR    deps root            (default: <repo>/.deps/picoquic-ci)
#   PICOQUIC_REPO   picoquic remote      (default: private-octopus/picoquic)
#   PICOQUIC_REF    picoquic commit      (default: pinned below)
#   PICOTLS_REPO    picotls remote       (default: h2o/picotls)
#   PICOTLS_REF     picotls commit       (default: pinned below)
#   OPENSSL_ROOT_DIR  passed to picotls' cmake if set (macOS/brew)
#
# The tracked dependency patches (PICOQUIC_PATCHES and PICOTLS_PATCHES below,
# each in order) are applied after checkout. Each declared list must be exactly
# the patches tracked under patches/<dependency>, numbered from 0001 in
# ascending order. A patch that is already applied is accepted; a missing or
# conflicting patch fails before any dependency build. --self-test checks this
# step without fetching or building.
#
# Requires: git, cmake, a C compiler, and OpenSSL dev headers
# (Ubuntu: apt-get install -y libssl-dev cmake).

set -euo pipefail

# -- Pinned, known-good dependency commits -----------------------------
PICOQUIC_REPO="${PICOQUIC_REPO:-https://github.com/private-octopus/picoquic}"
PICOQUIC_REF="${PICOQUIC_REF:-55b473e207e436d06ea9a2895cc1fc555d42c81c}"
PICOTLS_REPO="${PICOTLS_REPO:-https://github.com/h2o/picotls.git}"
PICOTLS_REF="${PICOTLS_REF:-7c32032f91449d695b24b82955f20d04d47e6cff}"

# -- Tracked PicoQUIC patches, applied in this order ---------------------
# Each is independent of the others today; the numbering fixes the order so a
# later patch may rely on an earlier one.
PICOQUIC_PATCHES=(
    0001-h3zero-reset-capsule-value-read.patch
    0002-short-initial-bounds-and-test-link-closure.patch
    0003-core-owns-qlog-streaming.patch
    0004-picohttp-core-single-picotls-link.patch
)

# -- Tracked PicoTLS patches, applied in this order ----------------------
PICOTLS_PATCHES=(
    0001-minicrypto-distinct-chacha20-object.patch
    0002-micro-ecc-initialize-point-add-scratch.patch
    0003-test-select-cipher-initialize-selected.patch
)

script_dir=$(cd "$(dirname "$0")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)
MOQ_DEPS_DIR="${MOQ_DEPS_DIR:-$repo_root/.deps/picoquic-ci}"

picoquic_dir="$MOQ_DEPS_DIR/picoquic"
picotls_dir="$MOQ_DEPS_DIR/picotls"
picotls_build="$picotls_dir/build"

log() { printf '[setup_picoquic_deps] %s\n' "$*" >&2; }
die() { printf '[setup_picoquic_deps] ERROR: %s\n' "$*" >&2; exit 1; }

# check_patch_list <dependency> <patch...>: the declared list is exactly the
# patches tracked under patches/<dependency>, each present, numbered 0001,
# 0002, ... in declared order.
check_patch_list() {
    local dep=$1; shift
    local dir="$repo_root/patches/$dep" p n=1 tracked declared
    [ "$#" -gt 0 ] || die "no $dep patches are declared"
    for p in "$@"; do
        [ -f "$dir/$p" ] || die "missing $dep patch: $dir/$p"
        [[ "$p" =~ ^([0-9]{4})-[A-Za-z0-9._-]+\.patch$ ]] \
            || die "$dep patch list entry '$p' is not named NNNN-<name>.patch"
        [ "$((10#${BASH_REMATCH[1]}))" -eq "$n" ] \
            || die "$dep patch list is out of order at '$p': expected number $(printf '%04d' "$n")"
        n=$((n + 1))
    done
    tracked=$(cd "$dir" && ls -1 -- *.patch 2>/dev/null) || tracked=""
    declared=$(printf '%s\n' "$@")
    [ "$tracked" = "$declared" ] \
        || die "the declared $dep patch list does not match the patches tracked in $dir: declared [$(printf '%s ' $declared)], tracked [$(printf '%s ' $tracked)]"
}

# apply_patch <tree> <dependency> <name>: apply one tracked patch, accepting it
# when already applied.
apply_patch() {
    local dir=$1 dep=$2 name=$3
    local patch="$repo_root/patches/$dep/$name"
    [ -f "$patch" ] || die "missing $dep patch: $patch"
    if git -C "$dir" apply --check "$patch" >/dev/null 2>&1; then
        git -C "$dir" apply "$patch" || die "$dep patch $name failed to apply"
        log "applied $dep patch $name"
    elif git -C "$dir" apply --reverse --check "$patch" >/dev/null 2>&1; then
        log "$dep patch $name already applied"
    else
        die "$dep patch $name conflicts with $dir"
    fi
}

# apply_patch_list <tree> <dependency> <patch...>: every declared patch, in
# order, after the identity check.
apply_patch_list() {
    local dir=$1 dep=$2 p; shift 2
    check_patch_list "$dep" "$@"
    for p in "$@"; do
        apply_patch "$dir" "$dep" "$p"
    done
}

patch_selftest() (
    set -eu
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    expect_die() {  # expect_die <label> <expected message> <command...>
        local label=$1 want=$2; shift 2
        if ("$@") > "$tmp/log" 2>&1; then
            die "self-test: $label was accepted"
        fi
        grep -qF -- "$want" "$tmp/log" || die "self-test: $label failed for another reason: $(head -1 "$tmp/log")"
    }

    # 1. The real declared lists are exactly the tracked patches, in order.
    check_patch_list picoquic "${PICOQUIC_PATCHES[@]}"
    check_patch_list picotls "${PICOTLS_PATCHES[@]}"

    # 2. The real capsule patch against a minimal upstream context (the
    #    behavioral regression links picohttp-core).
    mkdir -p "$tmp/tree/picohttp"
    git -C "$tmp/tree" init -q
    printf '\t\t%s\n' \
        '/* reset the fields to expected value */' \
        'capsule->header_length = 0;' \
        'capsule->header_read = 0;' \
        'capsule->capsule_type = 0;' \
        'capsule->capsule_length = 0;' \
        'capsule->is_stored = 0;' > "$tmp/tree/picohttp/h3zero_common.c"
    apply_patch "$tmp/tree" picoquic "${PICOQUIC_PATCHES[0]}"
    grep -qF 'capsule->value_read = 0;' "$tmp/tree/picohttp/h3zero_common.c"

    # 3. The ordered-list machinery, on two synthetic patches in a private
    #    repository root.
    mk_list_fixture() {
        rm -rf "$tmp/root" "$tmp/src"
        mkdir -p "$tmp/root/patches/fixture" "$tmp/src"
        git -C "$tmp/src" init -q
        printf 'a\n' > "$tmp/src/one.c"; printf 'b\n' > "$tmp/src/two.c"
        git -C "$tmp/src" add one.c two.c
        git -C "$tmp/src" -c user.name=t -c user.email=t@t commit -qm base
        printf 'a\nA\n' > "$tmp/src/one.c"; git -C "$tmp/src" diff > "$tmp/root/patches/fixture/0001-one.patch"
        git -C "$tmp/src" checkout -q -- one.c
        printf 'b\nB\n' > "$tmp/src/two.c"; git -C "$tmp/src" diff > "$tmp/root/patches/fixture/0002-two.patch"
        git -C "$tmp/src" checkout -q -- two.c
    }
    with_list() {  # with_list <entries...> -- : apply the fixture list to $tmp/src
        local -a list=()
        while [ "$1" != "--" ]; do list+=("$1"); shift; done; shift
        ( repo_root="$tmp/root"; apply_patch_list "$tmp/src" fixture "${list[@]}" )
    }
    mk_list_fixture
    with_list 0001-one.patch 0002-two.patch --
    grep -qx A "$tmp/src/one.c"; grep -qx B "$tmp/src/two.c"
    cp "$tmp/src/one.c" "$tmp/one.expected"; cp "$tmp/src/two.c" "$tmp/two.expected"
    with_list 0001-one.patch 0002-two.patch --
    cmp -s "$tmp/one.expected" "$tmp/src/one.c"; cmp -s "$tmp/two.expected" "$tmp/src/two.c"

    # 3a. missing second patch: refused before anything is applied
    mk_list_fixture
    rm "$tmp/root/patches/fixture/0002-two.patch"
    expect_die "a missing second patch" "missing fixture patch" \
        with_list 0001-one.patch 0002-two.patch --
    test "$(cat "$tmp/src/one.c")" = a

    # 3b. a second patch that conflicts
    mk_list_fixture
    printf 'conflicting source\n' > "$tmp/src/two.c"
    expect_die "a conflicting second patch" "fixture patch 0002-two.patch conflicts" \
        with_list 0001-one.patch 0002-two.patch --
    test "$(cat "$tmp/src/two.c")" = 'conflicting source'

    # 3c. reversed order
    mk_list_fixture
    expect_die "a reversed patch list" "out of order" \
        with_list 0002-two.patch 0001-one.patch --
    test "$(cat "$tmp/src/one.c")" = a

    # 3d. a tracked patch skipped from the declared list
    mk_list_fixture
    expect_die "a skipped tracked patch" "does not match the patches tracked" \
        with_list 0001-one.patch --
    test "$(cat "$tmp/src/one.c")" = a

    # 3e. a declared patch that is not tracked is a missing patch
    mk_list_fixture
    expect_die "an untracked declared patch" "missing fixture patch" \
        with_list 0001-one.patch 0002-two.patch 0003-three.patch --

    printf 'patch list self-test: picoquic+picotls identity, capsule apply, ordered apply, repeat, missing, conflict, reversed, skipped, untracked PASS\n'
)

if [ "${1:-}" = "--self-test" ]; then
    patch_selftest
    exit $?
fi

# Only the two KEY=VALUE result lines may reach real stdout (so callers
# can `eval "$(...)"`). Route everything else - including git/cmake and
# git-submodule chatter, which print to stdout - to stderr via fd 3.
exec 3>&1 1>&2

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
}

mkdir -p "$MOQ_DEPS_DIR"
log "deps dir: $MOQ_DEPS_DIR"

fetch_at "$PICOQUIC_REPO" "$PICOQUIC_REF" "$picoquic_dir"
apply_patch_list "$picoquic_dir" picoquic "${PICOQUIC_PATCHES[@]}"
fetch_at "$PICOTLS_REPO"  "$PICOTLS_REF"  "$picotls_dir"

# picotls carries minicrypto backends as submodules pinned by the parent
# commit - deterministic with the pinned ref.
log "picotls submodules"
git -C "$picotls_dir" submodule update --init --recursive --depth 1
apply_patch_list "$picotls_dir" picotls "${PICOTLS_PATCHES[@]}"

log "building picotls -> $picotls_build"
# Warnings are errors here, as in the Android cross-build. Release defines
# NDEBUG, which leaves eight assert-only variables in the pinned picotls unused
# (lib/picotls.c:2192, lib/openssl.c:1259..1555), so -UNDEBUG follows it and
# picotls keeps its assertions -- the same dependency-local assertion policy
# bindings/kotlin/tools/build-native-crossbuild.sh applies. No warning is
# disabled or filtered. The archives are position-independent because a shared
# LibMoQ build links them into libpicoquic-core; an ELF linker refuses non-PIC
# members in a shared object.
cmake_args=(-S "$picotls_dir" -B "$picotls_build" -DCMAKE_BUILD_TYPE=Release
            -DCMAKE_C_FLAGS=-Werror "-DCMAKE_C_FLAGS_RELEASE=-O3 -DNDEBUG -UNDEBUG"
            -DCMAKE_POSITION_INDEPENDENT_CODE=ON)
if [ -n "${OPENSSL_ROOT_DIR:-}" ]; then
    cmake_args+=(-DOPENSSL_ROOT_DIR="$OPENSSL_ROOT_DIR")
fi
cmake "${cmake_args[@]}" >/dev/null
# Build ONLY the picotls libraries that picoquic's FindPTLS consumes.
# picotls' own test/cli executables are not needed and pull in extra
# OpenSSL link requirements (e.g. they fail against macOS LibreSSL);
# building just the lib targets is faster and portable.
cmake --build "$picotls_build" \
    --target picotls-core picotls-openssl picotls-minicrypto \
    -j"$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2)" >/dev/null

# Sanity: picotls-core archive must exist where FindPicoquic expects it.
ls "$picotls_build"/libpicotls-core.* >/dev/null 2>&1 \
    || die "picotls build produced no libpicotls-core in $picotls_build"

# -- Emit the two configure inputs -------------------------------------
# stdout (fd 3) and the env file carry Bash-source/eval-safe assignments:
# the values are %q-escaped so paths with spaces or shell metacharacters
# survive `eval "$(setup_picoquic_deps.sh)"` and `. picoquic_deps.env`.
# Nothing else reaches fd 3, so the eval consumes exactly these two lines.
env_file="$MOQ_DEPS_DIR/picoquic_deps.env"
{
    printf 'MOQ_PICOQUIC_SOURCE_DIR=%q\n' "$picoquic_dir"
    printf 'MOQ_PICOTLS_PREFIX=%q\n'      "$picotls_build"
} | tee "$env_file" >&3

# GitHub Actions: make them available to subsequent steps. $GITHUB_ENV is
# NOT a shell file - Actions parses it as raw NAME=value env-file lines,
# so these stay unescaped (unlike the eval/source output above).
if [ -n "${GITHUB_ENV:-}" ]; then
    {
        printf 'MOQ_PICOQUIC_SOURCE_DIR=%s\n' "$picoquic_dir"
        printf 'MOQ_PICOTLS_PREFIX=%s\n'      "$picotls_build"
    } >> "$GITHUB_ENV"
fi

log "done. env written to $env_file"
