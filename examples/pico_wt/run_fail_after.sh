#!/usr/bin/env bash
# One failure exit of moq_pico_wt_client. --fail-after STEP must end the run
# with exit status 1 after printing "fail-after: STEP reached" (the client
# prints it only once STEP succeeded and the failure was asked for), within
# a bound, with no other fail-after marker and no sanitizer or runtime-error
# text in its output (the scan looks for "Sanitizer" and "runtime error",
# not for every kind of diagnostic). In a sanitizer build a cleanup path that
# misses something STEP made shows up as such a report. The client runs in
# its own process group, and a run past the bound is killed with the group.
#   run_fail_after.sh <client> <step> [bound seconds, default 20]
#   run_fail_after.sh --self-test     the judgement on synthetic clients
judge() {   # <client> <step> <bound>
    local client=$1 step=$2 bound=$3 out rc pid i
    out=$(mktemp)
    set -m
    "$client" --port 14459 --insecure-skip-verify --fail-after "$step" > "$out" 2>&1 &
    pid=$!
    set +m
    for ((i = 0; i < bound * 10; i++)); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.1
    done
    if kill -0 "$pid" 2>/dev/null; then
        kill -KILL -- "-$pid" 2>/dev/null; wait "$pid" 2>/dev/null
        cat "$out"; rm -f "$out"
        echo "FAIL: --fail-after $step still running after ${bound}s"; return 1
    fi
    wait "$pid"; rc=$?
    cat "$out"
    local markers wanted
    markers=$(grep -c '^fail-after: ' "$out")
    wanted=$(grep -cx "fail-after: $step reached" "$out")
    if grep -qE 'Sanitizer|runtime error' "$out"; then
        rm -f "$out"; echo "FAIL: sanitizer report after --fail-after $step"; return 1
    fi
    rm -f "$out"
    if [ "$wanted" -ne 1 ] || [ "$markers" -ne 1 ]; then
        echo "FAIL: --fail-after $step did not report reaching $step"; return 1
    fi
    if [ "$rc" -ne 1 ]; then
        echo "FAIL: exit status $rc after --fail-after $step, want 1"; return 1
    fi
    echo "PASS: --fail-after $step reached $step, exits 1, no sanitizer report"
}

self_test() {
    local d fails=0
    d=$(mktemp -d)
    fake() { printf '#!/usr/bin/env bash\n%s\n' "$2" > "$d/$1"; chmod +x "$d/$1"; }
    fake bogus 'exit 1'
    fake wrong 'echo "fail-after: prepare reached" >&2; exit 1'
    fake right 'for a; do s=$a; done; echo "fail-after: $s reached" >&2; exit 1'
    fake twice 'echo "fail-after: prepare reached" >&2; echo "fail-after: connect reached" >&2; exit 1'
    fake zero 'echo "fail-after: connect reached" >&2; exit 0'
    fake crash 'echo "fail-after: connect reached" >&2; kill -SEGV $$'
    fake hang "echo 'fail-after: connect reached' >&2; sleep 30 & echo \$! > '$d/hang.child'; wait"
    fake asan 'echo "fail-after: connect reached" >&2; echo "==1==ERROR: LeakSanitizer: detected memory leaks" >&2; exit 1'
    expect() {   # <want 0|1> <client> <bound>
        judge "$d/$2" connect "$3" > /dev/null 2>&1; local got=$?
        if [ "$got" -ne "$1" ]; then echo "self-test FAIL: $2 judged $got, want $1"; fails=$((fails + 1)); fi
    }
    expect 1 bogus 5; expect 1 wrong 5; expect 0 right 5; expect 1 twice 5
    expect 1 zero 5; expect 1 crash 5
    # A hang is judged at its bound, and the client's own child goes with it.
    local start=$SECONDS
    expect 1 hang 1
    if [ $((SECONDS - start)) -gt 4 ]; then
        echo "self-test FAIL: the hang was judged after $((SECONDS - start))s, bound 1s"
        fails=$((fails + 1))
    fi
    expect 1 asan 5
    if [ ! -s "$d/hang.child" ]; then
        echo "self-test FAIL: the hang client recorded no child"; fails=$((fails + 1))
    elif kill -0 "$(cat "$d/hang.child")" 2>/dev/null; then
        kill -KILL "$(cat "$d/hang.child")"
        echo "self-test FAIL: the hang client's child outlived the bound"; fails=$((fails + 1))
    fi
    rm -rf "$d"
    [ "$fails" -eq 0 ] && echo "self-test: ok" || echo "self-test: FAIL"
    return $((fails != 0))
}

if [ "${1:-}" = --self-test ]; then self_test; exit $?; fi
judge "$1" "$2" "${3:-20}"
