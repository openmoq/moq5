#!/usr/bin/env bash
# The coordinator's self-pipe protocol, pinned on executable source order:
#
#   1. each coordinator turn drains the pipe BEFORE it reads any state (raw
#      facade health, signal latches, broker) and waits only AFTER the
#      coordinator work of the turn: a notification posted after the drain
#      stays for the wait;
#   2. a row is published before its notification (relay_publish_row);
#   3. the dump and stop handlers latch before they notify.
#
# The behavioural rows (relay_coord_seam, relay_coord_wake) drive the pipe
# and the serve; this pins the order in cmd_serve_lanes itself, which those
# rows cannot reorder. Comments are stripped first.
#
# Argument 1 is cli/main.c.
set -u
src="${1:-}"
[ -r "$src" ] || { echo "FAIL: usage: $0 <cli/main.c>" >&2; exit 2; }
failures=0
fail() { echo "FAIL: $1" >&2; failures=$((failures + 1)); }

code=$(sed -e 's://.*::' "$src" | tr '\n' '\001' |
       sed -e 's:/\*[^\*]*\*\+\([^/\*][^\*]*\*\+\)*/: :g' | tr '\001' '\n')

body() {
    printf '%s\n' "$code" |
        awk -v f="^$1\\\\(" 'BEGIN{on=0} $0 ~ f {on=1} on {print} on && /^}/ {exit}'
}
first() {   # first line of $2 in body $1, or 0
    printf '%s\n' "$1" | grep -n -F -- "$2" | head -1 | cut -d: -f1
}
last() {
    printf '%s\n' "$1" | grep -n -F -- "$2" | tail -1 | cut -d: -f1
}
before() {  # $1 body, $2 earlier needle, $3 later needle, $4 what
    a=$(first "$1" "$2"); b=$(first "$1" "$3")
    if [ -z "$a" ] || [ -z "$b" ]; then
        fail "$4: '$2' or '$3' not found"
    elif [ "$a" -ge "$b" ]; then
        fail "$4: '$2' (line $a) must come before '$3' (line $b)"
    fi
}

lanes=$(body cmd_serve_lanes)
[ -n "$lanes" ] || { echo "FAIL: cmd_serve_lanes not found" >&2; exit 1; }
before "$lanes" 'coord_wake_init()' 'serve_install_signals(' \
    "the pipe exists before the handlers"
before "$lanes" 'coord_wake_drain()' 'relay_raw_poll(ctx)' \
    "drain before the health check"
before "$lanes" 'coord_wake_drain()' 'g_signal_metrics_pending' \
    "drain before the signal latch"
before "$lanes" 'coord_wake_drain()' 'moqr_broker_busy(' \
    "drain before the broker"
w=$(last "$lanes" 'coord_wake_wait_until(')
d=$(last "$lanes" 'wake_after_release)')
if [ -z "$w" ] || [ -z "$d" ] || [ "$w" -le "$d" ]; then
    fail "the wait must follow the turn's coordinator work"
fi
# Retirement after every lane has joined on the normal path.
before "$lanes" 'relay_raw_stop(ctx);   ' 'coord_wake_report()' \
    "retire after the lanes join"

pub=$(body relay_publish_row)
before "$pub" 'moqr_cli_snapshot_publish(' 'coord_wake_notify()' \
    "publish before notify"

for h in on_signal on_dump_signal; do
    hb=$(body "$h")
    a=$(first "$hb" 'atomic_'); n=$(first "$hb" 'coord_wake_notify()')
    if [ -z "$a" ] || [ -z "$n" ] || [ "$a" -ge "$n" ]; then
        fail "$h must latch before it notifies"
    fi
done

if [ "$failures" -ne 0 ]; then
    echo "FAIL: $failures coordinator wake ordering violation(s)" >&2
    exit 1
fi
echo "PASS: coordinator wake ordering"
