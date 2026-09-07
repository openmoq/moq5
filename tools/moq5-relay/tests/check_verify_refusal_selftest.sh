#!/usr/bin/env bash
# What counts as the verify seam's single-lane refusal.
#
# The classifier decides what a real run proved, so a broad one turns an
# unrelated failure into evidence that a guard still holds. Every case here is
# a captured status and message with a known verdict. No binary is run and no
# listener is started.
#
# The second half drives the REAL gate against substitute verify tools, so the
# classifier is checked where it is actually used, not only in isolation.
set -u
gate=${1:?usage: check_verify_refusal_selftest.sh <check_dual_capacity.sh> <moq5-relay> [<moq-relay-verify>]}
relay=${2:?usage: check_verify_refusal_selftest.sh <check_dual_capacity.sh> <moq5-relay> [<moq-relay-verify>]}
real_verify=${3:-}

. "$(dirname "$0")/verify_refusal.sh"

fail=0
cases=0
expect() {  # <label> <status> <output> <want: yes|no>
    cases=$((cases + 1))
    if verify_refused "$2" "$3"; then got=yes; else got=no; fi
    if [ "$got" != "$4" ]; then
        echo "FAIL: $1: classified as '$got', wanted '$4'"
        fail=1
    fi
}

expect_cap() {  # <label> <status> <output> <want: yes|no>
    cases=$((cases + 1))
    if verify_no_wt_support "$2" "$3"; then got=yes; else got=no; fi
    if [ "$got" != "$4" ]; then
        echo "FAIL: $1: classified as '$got', wanted '$4'"
        fail=1
    fi
}

REAL="$MOQR_SINGLE_LANE_REFUSAL"

# -- the refusal itself ------------------------------------------------------
expect "the real refusal" 2 "$REAL" yes
expect "the refusal among other output" 2 "some earlier line
$REAL" yes

# -- the two reproductions from the review -----------------------------------
# A different diagnostic from the same tool is a different failure, whatever
# status it carries. Accepting these is what let an unrelated configuration
# error stand in for the single-lane guard.
WRONG='moq-relay-verify: unrelated injected configuration failure'
expect "the wrong diagnostic at status 1" 1 "$WRONG" no
expect "the wrong diagnostic at status 2" 2 "$WRONG" no

# -- status and message must agree -------------------------------------------
expect "the right message at status 1" 1 "$REAL" no
expect "the right message at status 0" 0 "$REAL" no
expect "the right message at status 127" 127 "$REAL" no
expect "the right status with no message" 2 "" no
expect "the right status with a capacity report" 2 \
    "  cli runtime: 5816 bytes (snapshot rows, plus the serve context at lanes > 1; included above)" no

# -- a successful report is never a refusal ----------------------------------
# The capacity report describes a term as applying "at lanes > 1"; scanning the
# text for that phrase is what made a passing run read as a refusal.
expect "a successful capacity report" 0 \
    "relay-state allocation-request ceiling: 1616263241 bytes
  cli runtime: 399861 bytes (snapshot rows, plus the serve context at lanes > 1; included above)" no

# -- the message is matched literally, not as a pattern ----------------------
# Its own '*' and '.' must spell themselves.
expect "a glob-expanded near miss" 2 \
    'moq-relay-verify: MOQR_VERIFY_XX_MAX_OPEN_SUBGROUPS requires a multi-shard relay: listener.lanes + webtransport.lanes > 1' no
expect "a dot-substituted near miss" 2 \
    'moq-relay-verify: MOQR_VERIFY_*_MAX_OPEN_SUBGROUPS requires a multi-shard relay: listenerXlanes + webtransport.lanes > 1' no
expect "the refusal with trailing text on its line" 2 "$REAL and more" no
expect "the refusal as a substring of a longer line" 2 "note: $REAL" no

# -- the capability refusal is a DIFFERENT contract ---------------------------
# A build without WebTransport support refuses any webtransport object, for
# every command. That answer must never be mistaken for the single-lane
# misclassification, and vice versa: two named contracts, no overlap.
NOWT="$MOQR_NO_WT_REFUSAL"
expect_cap "the capability refusal is recognised"          2 "$NOWT" yes
expect_cap "the capability refusal at status 1 is not it"  1 "$NOWT" no
expect_cap "the capability refusal at status 0 is not it"  0 "$NOWT" no
expect_cap "the single-lane refusal is not a capability refusal" 2 "$REAL" no
expect_cap "a capability refusal with trailing text is not it" \
    2 "$NOWT and more" no
expect     "the capability refusal is not a single-lane refusal" 2 "$NOWT" no

# -- the gate itself, against substitute verify tools ------------------------
# The classifier is checked where it is used. Each substitute answers the two
# configs the gate builds; only the last two must let the gate pass.
work=$(mktemp -d); trap 'rm -rf "$work"' EXIT
mk() {  # <name> <body>
    printf '#!/usr/bin/env bash\n%s\n' "$2" > "$work/$1"
    chmod +x "$work/$1"
}
# accepts dual, and refuses raw1 with the WRONG diagnostic -- the review's
# reproduction, at each of the two statuses it used
for st in 1 2; do
    mk "wrong_verify_rc$st" '
case "$*" in
  *raw1.json*) echo "moq-relay-verify: unrelated injected configuration failure" >&2; exit '"$st"';;
  *) echo "  cli runtime: 5816 bytes (snapshot rows, plus the serve context at lanes > 1; included above)"; exit 0;;
esac'
done
# the right message at the wrong status
mk right_msg_wrong_rc '
case "$*" in
  *raw1.json*) echo "'"$REAL"'" >&2; exit 1;;
  *) echo "  cli runtime: 5816 bytes"; exit 0;;
esac'
# the right message with a successful status
mk right_msg_rc0 '
case "$*" in
  *raw1.json*) echo "'"$REAL"'" >&2; exit 0;;
  *) echo "  cli runtime: 5816 bytes"; exit 0;;
esac'
# refuses everything, including the dual config
mk always_refuse '
echo "'"$REAL"'" >&2; exit 2'
# accepts everything, including a genuinely single-lane relay
mk always_accept '
echo "  cli runtime: 5816 bytes (snapshot rows, plus the serve context at lanes > 1; included above)"; exit 0'
# a WT-less verify binary: it refuses the dual config for CAPABILITY, and the
# single-lane config for lane count. Both arms must be satisfied.
mk nowt_verify '
case "$*" in
  *raw1.json*) echo "'"$REAL"'" >&2; exit 2;;
  *) echo "'"$NOWT"'" >&2; exit 2;;
esac'
# the contract, answered correctly
mk correct_verify '
case "$*" in
  *raw1.json*) echo "'"$REAL"'" >&2; exit 2;;
  *) echo "  cli runtime: 5816 bytes (snapshot rows, plus the serve context at lanes > 1; included above)"; exit 0;;
esac'

gate_case() {  # <label> <verify> <want: pass|fail>
    cases=$((cases + 1))
    if "$BASH" "$gate" "$relay" "$2" >"$work/out.log" 2>&1; then
        got=pass
    else
        got=fail
    fi
    if [ "$got" != "$3" ]; then
        echo "FAIL: gate with $1: $got, wanted $3"
        sed 's/^/    /' "$work/out.log" | head -6
        fail=1
    fi
}
: "${BASH:=bash}"
gate_case "the wrong diagnostic at status 1" "$work/wrong_verify_rc1" fail
gate_case "the wrong diagnostic at status 2" "$work/wrong_verify_rc2" fail
gate_case "the right message at the wrong status" "$work/right_msg_wrong_rc" fail
gate_case "the right message at status 0" "$work/right_msg_rc0" fail
gate_case "a tool that refuses everything" "$work/always_refuse" fail
gate_case "a tool that accepts everything" "$work/always_accept" fail
gate_case "a correct substitute" "$work/correct_verify" pass
gate_case "a WebTransport-less verify binary" "$work/nowt_verify" pass

# the real binary is the positive control
if [ -n "$real_verify" ] && [ -x "$real_verify" ]; then
    gate_case "the real verify binary" "$real_verify" pass
else
    echo "FAIL: the real verify binary was not supplied; the positive control"\
         "cannot run"
    fail=1
fi

if [ "$fail" -ne 0 ]; then
    echo "FAILED: verify-refusal classifier selftest"
    exit 1
fi
echo "PASS: $cases verify-refusal cases"
