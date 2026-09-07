#!/usr/bin/env bash
# The verify and measure compositions refuse a JSON-logging serve by name,
# before any resource exists (nothing on stdout, exit 2, one diagnostic),
# while their capacity subcommand is unaffected by the logging section.
#
# Arguments: the moq-relay-measure and moq-relay-verify binaries.
set -u
fail=0
work=$(mktemp -d "${TMPDIR:-/tmp}/moqr-logref.XXXXXX")
trap 'rm -rf "$work"' EXIT
cat > "$work/json.json" <<'JSON'
{"listener":{"port":4433,"versions":[18],"lanes":2,"cert":"nope.pem","key":"nope.pem"},
 "logging":{"format":"json"}}
JSON
unset MOQR_VERIFY_BIND_MAX_OPEN_SUBGROUPS MOQR_VERIFY_SESSION_MAX_OPEN_SUBGROUPS
for bin in "$@"; do
    name=$(basename "$bin")
    "$bin" serve --config "$work/json.json" >"$work/out" 2>"$work/err"
    rc=$?
    if [ "$rc" -ne 2 ]; then
        echo "FAIL: $name serve with logging.format=json exited $rc, expected 2"; fail=1
    fi
    if [ -s "$work/out" ]; then
        echo "FAIL: $name wrote to stdout before refusing:"; cat "$work/out"; fail=1
    fi
    if ! grep -q 'logging.format: json is not available in the verify and measure builds' "$work/err"; then
        echo "FAIL: $name did not name the refusal:"; cat "$work/err"; fail=1
    fi
    if [ "$(wc -l < "$work/err")" -ne 1 ]; then
        echo "FAIL: $name printed more than the one diagnostic:"; cat "$work/err"; fail=1
    fi
    "$bin" capacity --config "$work/json.json" >"$work/out" 2>"$work/err"
    rc=$?
    if [ "$rc" -ne 0 ] || ! grep -q '^relay-state allocation-request ceiling: ' "$work/out"; then
        echo "FAIL: $name capacity is affected by the logging section (rc $rc):"; cat "$work/out" "$work/err"; fail=1
    fi
done
[ "$fail" -eq 0 ] || exit 1
echo "PASS: the verify and measure builds refuse a JSON serve before any resource and leave capacity alone"
