#!/usr/bin/env bash
#
# Self-test of check_relay_boundary.sh against fixture trees: its one
# permitted exception (moq/vi64.h and moq/wire.h in relay/src/core/
# props_codec.c) passes, and every neighboring case still fails -- the same
# headers in any other relay or tool source, and every other forbidden
# header in that file.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GUARD="$SCRIPT_DIR/check_relay_boundary.sh"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/relay-boundary-test.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT
failures=0

# case NAME EXPECT(pass|fail) FILE INCLUDE...
case_run() {
    local name="$1" expect="$2" file="$3"
    shift 3
    local root="$WORK/$name"
    mkdir -p "$root/relay/src/core" "$root/tools/moq5-relay"
    printf '#include <moq/vi64.h>\n#include <moq/wire.h>\n' \
        > "$root/relay/src/core/props_codec.c"
    mkdir -p "$(dirname "$root/$file")"
    local inc
    for inc in "$@"; do printf '#include <%s>\n' "$inc" >> "$root/$file"; done
    local got=pass
    MOQ_RELAY_BOUNDARY_ROOT="$root" bash "$GUARD" > "$root.out" 2>&1 || got=fail
    if [ "$got" != "$expect" ]; then
        echo "FAIL $name: expected $expect, got $got"
        cat "$root.out"
        failures=$((failures + 1))
    else
        echo "ok   $name ($got)"
    fi
}

case_run allowed_pair            pass relay/src/core/props_codec.c
case_run vi64_elsewhere_core     fail relay/src/core/other.c moq/vi64.h
case_run wire_elsewhere_bind     fail relay/src/bind/x.c moq/wire.h
case_run wire_in_tool            fail tools/moq5-relay/main.c moq/wire.h
case_run lookalike_path_in_tool  fail tools/moq5-relay/relay/src/core/props_codec.c moq/vi64.h
case_run kvp_in_codec_file       fail relay/src/core/props_codec.c moq/kvp.h
case_run session_internal_here   fail relay/src/core/props_codec.c session_internal.h
case_run adapter_here            fail relay/src/core/props_codec.c moq/picoquic.h
case_run threading_here          fail relay/src/core/props_codec.c pthread.h
case_run socket_here             fail relay/src/core/props_codec.c sys/socket.h

if [ "$failures" -ne 0 ]; then
    echo "relay boundary self-test: FAIL ($failures)"
    exit 1
fi
echo "relay boundary self-test: clean"
