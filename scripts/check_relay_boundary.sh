#!/usr/bin/env bash
#
# Relay boundary guard.
#
# relay/ and tools/moq5-relay/ consume libmoq strictly through the PUBLIC
# session API and the managed adapter facades. This check fails the build if
# relay code reaches below that line:
#
#   1. Nothing under relay/ or tools/moq5-relay/ may include private or
#      wire-tooling
#      headers (transport bridge, wire codecs, session internals) or the
#      publisher/subscriber facades (they own session event polling; the
#      relay needs the raw event stream).
#   2. The sans-I/O relay core (relay/src/core/) additionally must not
#      include adapter, service-tier, threading, or socket headers — time and
#      I/O are inputs there, same discipline as libmoq core/.
#
# One exception, by file and header (ALLOWED below): the property codec
# translation primitive, relay/src/core/props_codec.c, re-encodes Object and
# Track Property integers between draft-16 (i) and draft-18/22 (vi64) for
# cross-draft forwarding, using libmoq's existing integer codecs from
# moq/vi64.h and moq/wire.h rather than a second implementation. Every other
# forbidden header stays forbidden in that file, and those two stay forbidden
# everywhere else.
#
# Exit 0 if clean, 1 if violations found.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# The guard's own self-test points it at a fixture tree.
if [ -n "${MOQ_RELAY_BOUNDARY_ROOT:-}" ]; then
    ROOT="$(cd "$MOQ_RELAY_BOUNDARY_ROOT" && pwd)"
fi
RELAY_LIBRARY_DIR="relay"
RELAY_TOOL_DIR="tools/moq5-relay"

if [ ! -d "$ROOT/$RELAY_LIBRARY_DIR" ] && [ ! -d "$ROOT/$RELAY_TOOL_DIR" ]; then
    echo "relay boundary: relay sources not present; nothing to check"
    exit 0
fi

failures=0

# Forwarding implementation belongs to the top-level relay component.  Keep
# the legacy tool-owned locations empty so a new source file cannot bypass the
# library build or its stricter core scan.
for legacy in core bind shard obs bench; do
    if [ -d "$ROOT/$RELAY_TOOL_DIR/$legacy" ] &&
       find "$ROOT/$RELAY_TOOL_DIR/$legacy" -type f \
           \( -name '*.c' -o -name '*.h' -o -name '*.cpp' -o -name '*.hpp' \) \
           -print -quit | grep -q .; then
        echo "relay boundary violation: forwarding source under $RELAY_TOOL_DIR/$legacy"
        failures=1
    fi
done

# Headers no relay code may include (private SPI, wire tooling, internals,
# and the event-owning facades).
FORBIDDEN_EVERYWHERE=(
    'moq/transport_bridge\.h'
    'moq/wire\.h'
    'moq/codec\.h'
    'moq/control\.h'
    'moq/control_d18\.h'
    'moq/kvp\.h'
    'moq/vi64\.h'
    'moq/buf\.h'
    'moq/publisher\.h'
    'moq/subscriber\.h'
    'session_internal\.h'
)

# The only permitted forbidden-header uses: "file<TAB>pattern", exact.
ALLOWED=(
    "relay/src/core/props_codec.c	moq/wire\\.h"
    "relay/src/core/props_codec.c	moq/vi64\\.h"
)

# Additionally forbidden in the sans-I/O relay core.
FORBIDDEN_IN_CORE=(
    'moq/endpoint\.h'
    'moq/media_receiver\.h'
    'moq/media_sender\.h'
    'moq/picoquic'
    'moq/mvfst'
    'moq/pico_wt'
    'moq/proxygen'
    'pthread\.h'
    'threads\.h'
    'sys/socket\.h'
    'netinet/'
)

scan() {
    local scope_dir="$1"
    local label="$2"
    shift 2
    local patterns=("$@")

    local files
    files="$(cd "$ROOT" && find "$scope_dir" -type f \
        \( -name '*.c' -o -name '*.h' -o -name '*.cpp' -o -name '*.hpp' \) \
        -print | sort)"
    [ -n "$files" ] || return 0

    local pat
    for pat in "${patterns[@]}"; do
        local hits
        hits="$(cd "$ROOT" && echo "$files" | \
            xargs grep -HnE "^[[:space:]]*#[[:space:]]*include[[:space:]]*[<\"].*${pat}" \
            2>/dev/null || true)"
        local allowed
        for allowed in "${ALLOWED[@]}"; do
            local afile="${allowed%%	*}" apat="${allowed#*	}"
            if [ "$apat" = "$pat" ] && [ -n "$hits" ]; then
                local kept="" line
                while IFS= read -r line; do
                    case "$line" in
                        "${afile}:"*) ;;                 # this file only
                        *) kept="${kept:+$kept
}$line" ;;
                    esac
                done <<< "$hits"
                hits="$kept"
            fi
        done
        if [ -n "$hits" ]; then
            echo "relay boundary violation ($label): forbidden include '${pat}':"
            echo "$hits"
            failures=1
        fi
    done
}

scan "$RELAY_LIBRARY_DIR"          "relay library" "${FORBIDDEN_EVERYWHERE[@]}"
scan "$RELAY_TOOL_DIR"             "relay tool"    "${FORBIDDEN_EVERYWHERE[@]}"
scan "$RELAY_LIBRARY_DIR/src/core" "sans-I/O core" "${FORBIDDEN_IN_CORE[@]}"

if [ "$failures" -ne 0 ]; then
    echo "relay boundary: FAIL"
    exit 1
fi

echo "relay boundary: clean"
exit 0
