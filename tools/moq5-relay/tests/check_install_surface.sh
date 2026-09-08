#!/usr/bin/env bash
# The installed surface of the relay command.
#
# The command and the two public relay libraries may ship. Private policy,
# test/inspection libraries and headers must remain in the build tree.
#
#   $1  staged install prefix (already populated by `cmake --install`)
#   $2  the source tree root
set -u

prefix="${1:-}"
srcroot="${2:-}"
# Where the command's EXTERNAL prebuilt dependencies live, as a colon-separated
# list of directories. A dynamically linked command needs them resolvable at run
# time, and the relay does not install libraries it did not build. What it must
# NOT carry is a path into the build tree, which is checked separately below and
# is unaffected by this.
deplib="${3:-}"
if [ ! -d "$prefix" ] || [ ! -d "$srcroot" ]; then
    echo "FAIL: usage: $0 <staged-prefix> <source-root> [dependency-libdir]" >&2
    exit 2
fi
run_installed() {
    if [ -n "$deplib" ]; then
        DYLD_FALLBACK_LIBRARY_PATH="$deplib" LD_LIBRARY_PATH="$deplib" "$@"
    else
        "$@"
    fi
}

failures=0
fail() { echo "FAIL: $1" >&2; failures=$((failures + 1)); }

# -- 1. the source directory and the root build wiring --------------------
[ -d "$srcroot/tools/moq5-relay" ] || fail "tools/moq5-relay does not exist"
[ -d "$srcroot/tools/moq-relay" ] && fail "the old tools/moq-relay still exists"
if ! grep -qF -- "add_subdirectory(tools/moq5-relay)" "$srcroot/CMakeLists.txt"; then
    fail "the root build does not include tools/moq5-relay"
fi
if grep -qF -- "add_subdirectory(tools/moq-relay)" "$srcroot/CMakeLists.txt"; then
    fail "the root build still includes tools/moq-relay"
fi

# -- 2. exactly the command, at the conventional location ------------------
cmd="$prefix/bin/moq5-relay"
[ -x "$cmd" ] || fail "missing installed command: bin/moq5-relay"
[ -e "$prefix/bin/moq-relay" ] && fail "an old-name alias was installed"

installed=$(cd "$prefix" && find . -type f -o -type l | sed 's|^\./||' | sort)

# -- 3. documentation and one inert example --------------------------------
printf '%s\n' "$installed" | grep -q "share/man/man1/moq5-relay.1" ||
    fail "missing man page share/man/man1/moq5-relay.1"
printf '%s\n' "$installed" | grep -q "share/man/man5/moq5-relay.json.5" ||
    fail "missing man page share/man/man5/moq5-relay.json.5"
printf '%s\n' "$installed" | grep -q "share/doc/moq5-relay/README.md" ||
    fail "missing share/doc/moq5-relay/README.md"
printf '%s\n' "$installed" | grep -q "share/doc/moq5-relay/examples/" ||
    fail "missing the example configuration"

# -- 4. nothing private may ship -------------------------------------------
while IFS= read -r f; do
    case "$f" in
    */libmoq-relay.a|*/libmoq-relay.so|*/libmoq-relay.dylib|\
    */libmoq-relay-core.a|*/libmoq-relay-core.so|*/libmoq-relay-core.dylib|\
    */pkgconfig/libmoq-relay.pc|*/pkgconfig/libmoq-relay-core.pc|\
    */cmake/libmoq/libmoqRelayComponents.cmake|\
    */cmake/libmoq/libmoqRelayTargets*.cmake|\
    */cmake/libmoq/libmoqRelayCoreTargets*.cmake)
        continue ;;
    include/moq/relay/*)
        case "${f##*/}" in
        export.h|types.h|auth.h|capacity.h|log.h|placement.h|relay.h|trace.h|\
        wire_codes.h|moqr_bind.h|moqr_shards.h|moqr_obs.h) continue ;;
        *) fail "private relay header installed: $f"; continue ;;
        esac ;;
    *libmoq-relay*|*moqrelay/*|\
    *moq-relay-verify*|*moq-relay-measure*|\
    *moq5-relay-verify*|*moq5-relay-measure*|*test_relay*|*bench_relay*|\
    *-test-internals*|*check_*.sh)
        fail "private artifact installed: $f" ;;
    esac
done <<EOF
$installed
EOF

# -- 5. the installed command answers informational requests ---------------
if [ -x "$cmd" ]; then
    run_installed "$cmd" --help  >/dev/null 2>&1 || fail "installed --help failed"
    run_installed "$cmd" --version >/dev/null 2>&1 || fail "installed --version failed"
    run_installed "$cmd" --help 2>/dev/null | grep -qF -- "moq5-relay" ||
        fail "help does not name the moq5-relay command"
    run_installed "$cmd" --help 2>/dev/null | grep -qE '(^|[^5])moq-relay' &&
        fail "help still names the old moq-relay command"
    run_installed "$cmd" --version 2>/dev/null | grep -qF -- "MOQ5 Relay" ||
        fail "version does not name the product"
fi

# -- 6. no build-tree path leaks into the installed binary -----------------
if [ -x "$cmd" ] && command -v otool >/dev/null 2>&1; then
    if otool -l "$cmd" | grep -A2 LC_RPATH | grep -qE 'CMakeFiles|/build|-build'; then
        fail "a build-tree RPATH leaked into the installed command"
    fi
fi

if [ "$failures" -ne 0 ]; then
    echo "FAIL: $failures install-surface violation(s)" >&2
    exit 1
fi
echo "PASS: installed relay surface"
exit 0
