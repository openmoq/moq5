#!/usr/bin/env bash
#
# Narrow CI tripwire for the Swift Buffer concurrency contract.
#
# Buffer wraps a moq_rcbuf_t whose refcount is non-atomic and
# shard/executor-confined (see core/include/moq/rcbuf.h). The Swift
# Buffer is therefore intentionally NOT Sendable. This guard fails if any
# Swift source under bindings/swift/Sources declares `Buffer` Sendable -
# directly, via extension (incl. simple multi-line `extension Buffer:\n
# Sendable`), via `@unchecked Sendable`, or by making it an `actor`
# (actors are implicitly Sendable).
#
# It is scoped to `Buffer` specifically (not all @unchecked Sendable, so
# legitimate uses like ThreadedPumpContext are untouched). It is a
# deliberately dumb text check, not a Swift parser; the authoritative
# enforcement is the Swift 6 strict-concurrency compile in `swift test`.

set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
sources="$repo_root/bindings/swift/Sources"

if [ ! -d "$sources" ]; then
    echo "error: $sources not found" >&2
    exit 1
fi

command -v perl >/dev/null 2>&1 || { echo "error: perl not found" >&2; exit 1; }

status=0

while IFS= read -r f; do
    # A: a Buffer type/extension conforming to Sendable. [^{] spans
    #    newlines, so this also catches `Buffer: @unchecked Sendable` and
    #    a multi-line `extension Buffer:\n    Sendable {`.
    if ! perl -0777 -ne \
        'exit 1 if /(?:class|struct|enum|extension|actor)\s+Buffer\b[^{]*\bSendable\b/' \
        "$f"; then
        echo "error: $f declares Buffer as Sendable." >&2
        status=1
    fi
    # B: `actor Buffer` is implicitly Sendable even without a clause.
    if ! perl -0777 -ne 'exit 1 if /\bactor\s+Buffer\b/' "$f"; then
        echo "error: $f declares Buffer as an actor (implicitly Sendable)." >&2
        status=1
    fi
done < <(find "$sources" -name '*.swift')

if [ "$status" -ne 0 ]; then
    echo "       Buffer must stay non-Sendable (non-atomic rcbuf refcount)." >&2
    exit 1
fi

echo "check_swift_buffer_sendable: OK (Buffer is non-Sendable)"
