#!/usr/bin/env bash
# Driver adapter that lets moq-contribution-interop-runner start and score the
# libmoq contribution publisher (examples/service/media_send). Contract:
# ../moq-contribution-interop-runner/adapters/contract.schema.json (version 1).
#
# The options below describe media_send's command line, not MOQT expectations.
# Expected behavior comes only from the draft, via the runner's catalogs.
set -euo pipefail

fail() {
    printf 'moq5 adapter: %s\n' "$1" >&2
    exit 64
}

[[ "${MOQ_INTEROP_DRIVER_CONTRACT_VERSION:-}" == 1 ]] ||
    fail 'unsupported driver contract version'
request_file=${MOQ_INTEROP_DRIVER_REQUEST_FILE:-}
[[ -n "$request_file" && -r "$request_file" ]] || fail 'request file is unavailable'
publisher_bin=${MOQ5_MEDIA_SEND_BIN:-}
[[ -n "$publisher_bin" && -x "$publisher_bin" ]] ||
    fail 'MOQ5_MEDIA_SEND_BIN must name the media_send executable'

jq -e '
    .schema_version == 1 and
    (.draft == 18 or .draft == 21) and
    (.transport == "native_quic" or .transport == "webtransport") and
    (.run_id | type == "string" and length > 0) and
    (.scenario_id | type == "string" and length > 0) and
    (.endpoint | type == "string" and length > 0 and (contains("\n") | not)) and
    (.tls_ca | type == "string" and length > 0 and (contains("\n") | not)) and
    (.scenario_timeout_ms | type == "number" and . == floor and . >= 1 and . <= 3600000) and
    (.namespace_hex | type == "array" and length >= 1 and all(type == "string" and length > 0)) and
    (.track_name_hex | type == "string" and length > 0)
' "$request_file" >/dev/null || fail 'unsupported or malformed request'

draft=$(jq -r '.draft' "$request_file")
transport=$(jq -r '.transport' "$request_file")
endpoint=$(jq -r '.endpoint' "$request_file")
ca_cert=$(jq -r '.tls_ca' "$request_file")
timeout_ms=$(jq -r '.scenario_timeout_ms' "$request_file")
[[ -r "$ca_cert" ]] || fail 'TLS CA is unreadable'
if [[ "$transport" == webtransport ]]; then
    [[ "$endpoint" == https://* ]] || fail 'WebTransport requires an HTTPS endpoint'
else
    [[ "$endpoint" == moqt://* ]] || fail 'native QUIC requires a moqt endpoint'
fi

# Decode a lowercase hex string into printable ASCII, rejecting anything media_send
# cannot take on its command line unambiguously.
hex_to_text() {
    local hex=$1 text
    [[ "$hex" =~ ^([0-9a-f]{2})+$ ]] || fail 'malformed hex in request'
    text=$(printf '%b' "$(sed 's/../\\x&/g' <<<"$hex")")
    [[ "$text" =~ ^[[:print:]]+$ ]] || fail 'non-printable name in request'
    printf '%s' "$text"
}

namespace=
while IFS= read -r field_hex; do
    field=$(hex_to_text "$field_hex")
    [[ "$field" != */* ]] || fail 'namespace field contains a slash'
    namespace+="${namespace:+/}$field"
done < <(jq -r '.namespace_hex[]' "$request_file")
track=$(hex_to_text "$(jq -r '.track_name_hex' "$request_file")")

# The fixture is accepted but not used: media_send publishes placeholder access
# units. Say so in the adapter log so nobody reads the score as a media check.
printf 'moq5 adapter: scenario %s draft %s %s; media_send publishes placeholder media, fixture ignored\n' \
    "$(jq -r '.scenario_id' "$request_file")" "$draft" "$transport" >&2

timeout_seconds=$(((timeout_ms + 999) / 1000))

# Run "$@", sending SIGINT after $1 seconds; status 124 when the deadline fired,
# as timeout(1) reports it. macOS has no timeout(1) unless coreutils is installed
# (timeout, or gtimeout without the g prefix), so fall back to a watchdog. The
# background job starts with SIGINT ignored (no job control), which is fine for
# media_send: it installs its own SIGINT handler.
run_with_deadline() {
    local seconds=$1 pid watchdog status
    shift
    if command -v timeout >/dev/null 2>&1; then
        timeout --signal=INT "$seconds" "$@"
        return
    fi
    if command -v gtimeout >/dev/null 2>&1; then
        gtimeout --signal=INT "$seconds" "$@"
        return
    fi
    "$@" &
    pid=$!
    # The watchdog's own sleep is killed with it, so no stray process holds the
    # runner's pipes open after the publisher has exited.
    (
        trap 'kill "$nap" 2>/dev/null; exit 0' TERM
        sleep "$seconds" & nap=$!
        wait "$nap"
        kill -INT "$pid" 2>/dev/null
    ) >/dev/null 2>&1 &
    watchdog=$!
    wait "$pid"
    status=$?
    if kill -0 "$watchdog" 2>/dev/null; then
        kill -TERM "$watchdog" 2>/dev/null
        wait "$watchdog"
        return "$status"
    fi
    wait "$watchdog"
    return 124
}

# SIGINT makes media_send stop writing, drain and exit cleanly. Reaching the
# scenario timeout is the normal end of a context (the runner ends it too), so
# the deadline's status 124 is not a failure.
set +e
run_with_deadline "$timeout_seconds" "$publisher_bin" "$endpoint" "$namespace" "$track" \
    --draft "$draft" --ca "$ca_cert" --peer-close-ok
status=$?
set -e
((status == 124)) && exit 0
exit "$status"
