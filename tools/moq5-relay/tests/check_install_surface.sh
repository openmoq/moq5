#!/usr/bin/env bash
# The installed surface of the relay command.
#
# What ships is exactly one executable plus its documentation. Relay archives,
# relay-private headers, verify/measure builds, test seams and fixtures are
# build-tree things and must never reach a prefix: none of them is a supported
# interface, and installing one would make it look like a promise.
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
    *libmoq-relay*|*moqrelay/*|*moq-relay-verify*|*moq-relay-measure*|\
    *moq5-relay-verify*|*moq5-relay-measure*|*test_relay*|*bench_relay*|\
    *-test-internals*|*check_*.sh)
        fail "private artifact installed: $f" ;;
    esac
done <<EOF
$installed
EOF

# The relay must not add a pkg-config surface or export private CMake targets.
printf '%s\n' "$installed" | grep -qE 'pkgconfig/.*relay' &&
    fail "a relay pkg-config file was installed"
printf '%s\n' "$installed" | grep -qE 'cmake/.*[Rr]elay' &&
    fail "relay CMake targets were exported"

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
