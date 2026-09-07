#!/usr/bin/env bash
# The shipped executables must build from the exact current bytes.
#
# A unit target that compiles only cli/*.c can be green while cli/main.c does
# not compile at all, and ctest will then happily run a previously-built
# binary. That is how a full-suite PASS can be reported against a tree whose
# product does not build. This gate rebuilds the real executables and fails if
# either does not, so no stale object can carry a matrix.
set -u
build_dir="${1:-}"
[ -n "$build_dir" ] || { echo "usage: $0 <build-dir> [targets...]" >&2; exit 2; }
shift || true
targets="${*:-moq5-relay moq-relay-verify}"

failures=0
for t in $targets; do
    if ! cmake --build "$build_dir" --target "$t" >/tmp/mprod.$$ 2>&1; then
        # A target absent from this configuration is not a failure; a target
        # that exists and does not compile is.
        if grep -q "No rule to make target" /tmp/mprod.$$; then
            echo "skip: $t is not configured in $build_dir"
        else
            echo "FAIL: $t does not build from the current source" >&2
            grep -E " error:" /tmp/mprod.$$ | head -5 >&2
            failures=$((failures + 1))
        fi
    else
        echo "ok: $t builds"
    fi
    rm -f /tmp/mprod.$$
done

if [ "$failures" -ne 0 ]; then
    echo "FAIL: $failures production target(s) do not build" >&2
    exit 1
fi
echo "PASS: every configured production target builds from current source"
exit 0
