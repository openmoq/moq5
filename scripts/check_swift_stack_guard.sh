#!/usr/bin/env bash
# Negative link test for the moq_swift_stack_guard binary-exclusivity canary.
#
# One app binary must link EITHER the source-built Swift stack (MoQ) OR the
# installed-mode MoQService, never both. Both stacks define the same strong
# C symbol; this script builds a fixture that imports both and asserts the
# link FAILS with that duplicate symbol -- any other outcome (a successful
# link, or a different failure) is an error.
#
# Requires: MOQ_SERVICE prerequisites (an installed libmoq-service prefix on
# PKG_CONFIG_PATH).
set -u

cd "$(dirname "$0")/.." || exit 2
fixture=bindings/swift/Tests/fixtures/DualStackConsumer

if [ -z "${PKG_CONFIG_PATH:-}" ] || ! pkg-config --exists libmoq-service; then
    echo "SKIP-FAIL: libmoq-service not resolvable via PKG_CONFIG_PATH" >&2
    exit 2
fi

output=$(cd "$fixture" && MOQ_SERVICE=1 swift build 2>&1)
status=$?

if [ $status -eq 0 ]; then
    echo "FAIL: the dual-stack fixture LINKED -- the canary is dead" >&2
    exit 1
fi
if ! printf '%s\n' "$output" | grep -q "duplicate symbol.*moq_swift_stack_guard"; then
    echo "FAIL: the fixture failed, but not on the canary symbol:" >&2
    printf '%s\n' "$output" | tail -20 >&2
    exit 1
fi
echo "OK: duplicate-symbol canary fired (moq_swift_stack_guard)"
