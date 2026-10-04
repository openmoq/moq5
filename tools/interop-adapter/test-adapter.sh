#!/usr/bin/env bash
# No-network contract test for run.sh: a capture stub stands in for media_send.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
adapter="$here/run.sh"
work=$(mktemp -d /tmp/moq5-adapter-contract.XXXXXX)
trap 'rm -rf -- "$work"' EXIT

cat >"$work/media send stub" <<'STUB'
#!/usr/bin/env bash
for arg in "$@"; do printf '<%s>' "$arg"; done
printf '\n'
STUB
chmod +x "$work/media send stub"
touch "$work/ca cert.pem"

make_request() {  # draft transport endpoint namespace-json track-hex timeout-ms
    jq -n --argjson draft "$1" --arg transport "$2" --arg endpoint "$3" \
        --argjson ns "$4" --arg track "$5" --argjson timeout "$6" \
        --arg ca "$work/ca cert.pem" '{
            schema_version: 1, run_id: "r1", scenario_id: "s1", endpoint: $endpoint,
            draft: $draft, transport: $transport, namespace_hex: $ns,
            track_name_hex: $track, fixture: "", tls_ca: $ca, log_dir: "/tmp",
            scenario_timeout_ms: $timeout, process_timeout_ms: 5000
        }' >"$work/request.json"
}

run_adapter() {
    MOQ5_MEDIA_SEND_BIN="$work/media send stub" MOQ_INTEROP_DRIVER_CONTRACT_VERSION=1 \
        MOQ_INTEROP_DRIVER_REQUEST_FILE="$work/request.json" "$adapter" 2>/dev/null
}

expect_refused() {  # description
    local status=0
    run_adapter >/dev/null || status=$?
    [[ $status -eq 64 ]] || { echo "FAIL: $1 (status $status, want 64)"; exit 1; }
}

# Native QUIC, draft 18: argument mapping, hex decoding, multi-field namespace.
make_request 18 native_quic moqt://127.0.0.1:4443/moq '["6d65646961"]' 766964655f31 2500
out=$(run_adapter)
[[ "$out" == "<moqt://127.0.0.1:4443/moq><media><vide_1><--draft><18><--ca><$work/ca cert.pem>" ]] ||
    { echo "FAIL: draft 18 native mapping: $out"; exit 1; }

# WebTransport, draft 21, two namespace fields joined by a slash.
make_request 21 webtransport https://127.0.0.1:4443/moq '["6d65646961","6c697665"]' 766964655f31 1000
out=$(run_adapter)
[[ "$out" == "<https://127.0.0.1:4443/moq><media/live><vide_1><--draft><21><--ca><$work/ca cert.pem>" ]] ||
    { echo "FAIL: draft 21 webtransport mapping: $out"; exit 1; }

# Refusals (exit 64, publisher never started).
make_request 16 native_quic moqt://127.0.0.1:4443/moq '["6d65646961"]' 766964655f31 1000
expect_refused 'draft 16 is not a runner draft'
make_request 18 native_quic https://127.0.0.1:4443/moq '["6d65646961"]' 766964655f31 1000
expect_refused 'native QUIC with an https endpoint'
make_request 18 webtransport moqt://127.0.0.1:4443/moq '["6d65646961"]' 766964655f31 1000
expect_refused 'WebTransport with a moqt endpoint'
make_request 18 native_quic moqt://127.0.0.1:4443/moq '["6d65646961"]' 7a 1000
out=$(run_adapter); [[ "$out" == *"<z>"* ]] || { echo "FAIL: one-byte track name"; exit 1; }
make_request 18 native_quic moqt://127.0.0.1:4443/moq '["6d65646961"]' 0001 1000
expect_refused 'non-printable track name'
make_request 18 native_quic moqt://127.0.0.1:4443/moq '["6d2f65"]' 766964655f31 1000
expect_refused 'namespace field containing a slash'
make_request 18 native_quic moqt://127.0.0.1:4443/moq '["6d65646961"]' 766964655f3 1000
expect_refused 'odd-length hex'
make_request 18 native_quic moqt://127.0.0.1:4443/moq '["6d65646961"]' 766964655f31 0
expect_refused 'zero timeout'

status=0
MOQ5_MEDIA_SEND_BIN="$work/media send stub" MOQ_INTEROP_DRIVER_CONTRACT_VERSION=2 \
    MOQ_INTEROP_DRIVER_REQUEST_FILE="$work/request.json" "$adapter" >/dev/null 2>&1 || status=$?
[[ $status -eq 64 ]] || { echo "FAIL: wrong contract version accepted"; exit 1; }

echo "moq5 adapter contract test: PASS"
