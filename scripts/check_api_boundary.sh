#!/usr/bin/env bash
#
# Verify that stable application-facing headers and examples do not
# reference wire-level internals. These belong behind <moq/codec.h>
# or in internal source files.
#
# Exit 0 if clean, 1 if violations found.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

STABLE_HEADERS=(
    core/include/moq/moq.h
    core/include/moq/session.h
    core/include/moq/types.h
    core/include/moq/rcbuf.h
    core/include/moq/export.h
    core/include/moq/version.h
    core/include/moq/publisher.h
    core/include/moq/subscriber.h
    media/loc/include/moq/loc.h
    media/object/include/moq/media_object.h
)

STABLE_DOCS=(
    README.md
)

# Include docs/*.md
while IFS= read -r file; do
    # Protocol references, design notes, simulator internals, and wire-format
    # references legitimately name wire-level concepts.
    case "$file" in docs/conformance.md|docs/*_DESIGN.md|docs/simulation.md|docs/*-wire-reference.md) continue ;; esac
    STABLE_DOCS+=( "$file" )
done < <(cd "$ROOT" && find docs -type f -name '*.md' -print | sort)

STABLE_EXAMPLES=()
while IFS= read -r file; do
    STABLE_EXAMPLES+=( "$file" )
done < <(cd "$ROOT" && find examples -path 'examples/_archive' -prune \
    -o -path 'examples/picoquic' -prune \
    -o -path 'examples/pico_wt' -prune \
    -o -type f \( -name '*.c' -o -name '*.h' \) -print | sort)

while IFS= read -r file; do
    STABLE_DOCS+=( "$file" )
done < <(cd "$ROOT" && find examples -path 'examples/_archive' -prune \
    -o -path 'examples/picoquic' -prune \
    -o -path 'examples/pico_wt' -prune \
    -o -type f \( -name '*.md' -o -name '*.txt' \) -print | sort)

# sim.h is also stable (app + sim boundary)
if [ -f "$ROOT/sim/include/moq/sim.h" ]; then
    STABLE_HEADERS+=( sim/include/moq/sim.h )
fi

# Wire-level terms that must not appear in stable surfaces.
# Uses extended grep alternation and case-insensitive matching so comments
# and prose do not bypass the boundary with Varint/Kvp spelling variants.
#
# Lines containing the exact marker "api-boundary-exempt" or references
# to draft-16 codec identifiers (MOQ_D16_, moq_d16_, fuzz_) are exempted.
# Broad wording exemptions removed to prevent accidental bypass.
# MOQ_SUB_FETCH_OK exempts the subscriber facade fetch-item enum
# value whose name contains the wire message substring "FETCH_OK".
FORBIDDEN='varint|moq_buf_|moq_quic_varint|MOQ_D16|moq_d16_|moq_control_|MOQ_SETUP_PARAM|moq_kvp_|MOQ_KVP|CLIENT_SETUP|SERVER_SETUP|MAX_REQUEST_ID|REQUESTS_BLOCKED|control stream|extensions?[^i]|subgroup stream|subscribe_options|request ID credit'
FORBIDDEN_INCLUDES='^[[:space:]]*#[[:space:]]*include[[:space:]]*[<"]([^>"]*/)?(wire|buf|kvp|control|codec)[.]h[>"]'
EXEMPT='api-boundary-exempt|MOQ_D16_|moq_d16_|fuzz_|MOQ_EVENT_|moq_publish_ok|moq_fetch_ok|moq_request_error|MOQ_SUB_FETCH_OK'

# Wire message names in prose/docs — checked separately with
# case-sensitive grep so they don't false-positive on public API
# identifiers (e.g. moq_publish_ok_event_t, moq_request_error_t).
FORBIDDEN_WIRE_MSG='SUBSCRIBE_NAMESPACE|PUBLISH_NAMESPACE|PUBLISH_NAMESPACE_CANCEL|REQUEST_OK([^_]|$)|REQUEST_ERROR([^_]|$)|REQUEST_UPDATE([^_]|$)|PUBLISH_OK([^_]|$)|PUBLISH_DONE([^_]|$)|FETCH_OK([^_]|$)|FETCH_CANCEL([^_]|$)|NAMESPACE_DONE([^_]|$)|(^|[^a-z_])UNSUBSCRIBE([^_D]|$)|OBJECT_DATAGRAM'

violations=0

check_file() {
    local file="$1"
    local path="$ROOT/$file"
    if [ ! -f "$path" ]; then
        echo "WARNING: $file not found, skipping"
        return
    fi
    local hits
    hits=$(grep -niE "$FORBIDDEN" "$path" 2>/dev/null \
        | grep -viE "$EXEMPT" || true)
    if [ -n "$hits" ]; then
        echo "VIOLATION in $file:"
        echo "$hits" | sed 's/^/  /'
        violations=$((violations + 1))
    fi
    # Case-sensitive check for wire message names in prose.
    local wire_hits
    wire_hits=$(grep -nE "$FORBIDDEN_WIRE_MSG" "$path" 2>/dev/null \
        | grep -vE "$EXEMPT" || true)
    if [ -n "$wire_hits" ]; then
        echo "WIRE-MSG VIOLATION in $file:"
        echo "$wire_hits" | sed 's/^/  /'
        violations=$((violations + 1))
    fi
}

check_app_file() {
    local file="$1"
    check_file "$file"

    local path="$ROOT/$file"
    if [ ! -f "$path" ]; then
        return
    fi

    local hits
    hits=$(grep -nE "$FORBIDDEN_INCLUDES" "$path" 2>/dev/null || true)
    if [ -n "$hits" ]; then
        echo "VIOLATION in $file:"
        echo "$hits" | sed 's/^/  /'
        violations=$((violations + 1))
    fi
}

echo "=== API Boundary Check ==="
echo ""
echo "Scanning stable headers..."
for f in "${STABLE_HEADERS[@]}"; do
    check_file "$f"
done

echo "Scanning stable examples..."
for f in "${STABLE_EXAMPLES[@]}"; do
    check_app_file "$f"
done

echo "Scanning stable docs..."
for f in "${STABLE_DOCS[@]}"; do
    check_file "$f"
done

echo ""
if [ "$violations" -gt 0 ]; then
    echo "FAILED: $violations file(s) contain wire-level terms."
    echo ""
    echo "Wire internals belong behind <moq/codec.h> or in src/."
    echo "Stable app surfaces must use semantic MoQ concepts only."
    exit 1
else
    echo "PASSED: No wire-level leakage in stable surfaces."
    exit 0
fi
