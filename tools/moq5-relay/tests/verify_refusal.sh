# The verify seam's single-lane refusal, and how to recognise it.
#
# This decides whether a run was THAT refusal or something else, so its own
# mistakes are invisible to the run it judges: a classifier that accepts any
# failure would read an unrelated configuration error — or a crash — as proof
# the seam still guards the single-lane case, and a classifier that accepts any
# `moq-relay-verify:` line would accept the wrong diagnostic entirely.
#
# Sourced by check_dual_capacity.sh and driven directly by
# check_verify_refusal_selftest.sh. Defines only; starts nothing.

# Emitted by cmd_verify's single-lane guard, verbatim, on stderr with status 2.
MOQR_SINGLE_LANE_REFUSAL='moq-relay-verify: MOQR_VERIFY_*_MAX_OPEN_SUBGROUPS requires a multi-shard relay: listener.lanes + webtransport.lanes > 1'
MOQR_SINGLE_LANE_STATUS=2

# The other named refusal a WT-less build can give for a dual configuration:
# it cannot serve a webtransport object at all, and says so for EVERY command.
# This is a different contract from the single-lane one, kept separate so
# neither can stand in for the other.
MOQR_NO_WT_REFUSAL='config error: this build has no WebTransport listener support; a configured webtransport object cannot be served'

# verify_no_wt_support <status> <combined output> -> 0 when this is THAT refusal.
verify_no_wt_support() {
    [ "$1" -eq "$MOQR_SINGLE_LANE_STATUS" ] || return 1
    printf '%s\n' "$2" | grep -qxF "$MOQR_NO_WT_REFUSAL" || return 1
    return 0
}

# verify_refused <status> <combined output> -> 0 when this is that refusal.
#
# Both halves are required. A correct message with the wrong status is not this
# refusal, and neither is the right status carrying a different message: the
# message names the guard, and the status is how a caller distinguishes it from
# an ordinary failure. The message is matched as a whole line and as a FIXED
# string, so its `*` and `.` cannot match anything they do not spell.
verify_refused() {
    [ "$1" -eq "$MOQR_SINGLE_LANE_STATUS" ] || return 1
    printf '%s\n' "$2" | grep -qxF "$MOQR_SINGLE_LANE_REFUSAL" || return 1
    return 0
}
