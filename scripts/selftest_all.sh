#!/usr/bin/env bash
#
# Run every shell script's own --self-test under one chosen interpreter.
#
# The point is the interpreter axis. A script can pass on the Bash that CI's
# Linux runner ships (5.x) and break on the Bash a developer or user actually
# runs - macOS still ships 3.2 as /bin/bash, and the two disagree on real
# behavior (empty-array expansion under `set -u`, `${x,,}`, associative arrays,
# `mapfile`, ...). Running the self-tests under a *pinned* interpreter turns
# that divergence from a field surprise into a CI signal, the same way the
# deterministic sim turns network races into reproducible failures.
#
# Enrollment is automatic: any scripts/*.sh that dispatches `--self-test` is
# discovered and run. Add a self-test to a script and it joins this lane.
#
# Usage:
#   scripts/selftest_all.sh                 # run under this interpreter ($BASH)
#   scripts/selftest_all.sh --bash /bin/bash    # run under a specific Bash
#   scripts/selftest_all.sh --bash "$(brew --prefix)/bin/bash"
#
# CI runs it once per interpreter in a matrix (floor 3.2 + latest), so a script
# that only works on one is caught. Exits non-zero if any self-test fails.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Interpreter under test. Default to the one running this meta-runner; a script
# self-test that re-invokes itself must use "$BASH" (not its shebang) so the
# chosen interpreter is honored end to end - a self-test that re-execs via
# `#!/usr/bin/env bash` would silently fall back to PATH's bash and test the
# wrong shell, hiding the very divergence this lane exists to catch.
BASH_UNDER_TEST="${BASH}"

while [ $# -gt 0 ]; do
    case "$1" in
        --bash) BASH_UNDER_TEST="$2"; shift 2;;
        --help|-h)
            sed -n '2,22p' "$0" | sed 's/^# \{0,1\}//'
            exit 0;;
        *) echo "unknown argument: $1" >&2; exit 2;;
    esac
done

if [ ! -x "$BASH_UNDER_TEST" ] && ! command -v "$BASH_UNDER_TEST" >/dev/null 2>&1; then
    echo "not an executable bash: $BASH_UNDER_TEST" >&2
    exit 2
fi

ver="$("$BASH_UNDER_TEST" -c 'echo "${BASH_VERSINFO[0]}.${BASH_VERSINFO[1]}"')"
echo "=== self-test lane under Bash $ver ($BASH_UNDER_TEST) ==="

# Discover scripts that dispatch --self-test. The dispatch token, not a mere
# mention, is the enrollment signal - a comment referencing the flag does not
# enroll a script that cannot honor it.
enrolled=()
self="$(basename "$0")"
for f in "$SCRIPT_DIR"/*.sh; do
    [ "$(basename "$f")" = "$self" ] && continue
    if grep -qE -- '--self-test\)|"--self-test"|== ?"--self-test"' "$f"; then
        enrolled+=("$f")
    fi
done

if [ "${#enrolled[@]}" -eq 0 ]; then
    echo "no scripts advertise --self-test" >&2
    exit 1
fi

fail=0
for f in "${enrolled[@]}"; do
    name="$(basename "$f")"
    if "$BASH_UNDER_TEST" "$f" --self-test; then
        echo "OK:   $name"
    else
        echo "FAIL: $name" >&2
        fail=1
    fi
done

if [ "$fail" -ne 0 ]; then
    echo "=== self-test lane FAILED under Bash $ver ===" >&2
    exit 1
fi
echo "=== self-test lane PASSED under Bash $ver (${#enrolled[@]} scripts) ==="
