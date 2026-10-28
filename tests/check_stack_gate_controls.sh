#!/bin/bash
set -euo pipefail
export LC_ALL=C
[[ $# -ge 3 && $# -le 4 && ( $# -eq 3 || $4 == --arm ) ]] || exit 2
root=$(cd "$1" && pwd -P)
cc=$2
mkdir -p "$(dirname "$3")"
out="$(cd "$(dirname "$3")" && pwd -P)/$(basename "$3")"
mkdir "$out"
source "$root/tests/stack_gate_support.sh"

# Synthetic inventory records exercise the parser without relying on a target
# to emit a particular qualifier. They do not replace the compiler controls.
inventory_case() {
    local label=$1 expected=$2 dir rc
    shift 2
    dir="$out/inventory-$label"
    mkdir "$dir"
    : > "$dir/fixture.su"
    if [[ $# -gt 0 ]]; then printf '%s\n' "$@" > "$dir/fixture.su"; fi
    if stack_usage_validate "$dir/fixture.su" > "$dir/stdout.log" 2> "$dir/diagnostics.log"; then
        rc=0
    else
        rc=$?
    fi
    printf '%s\n' "$rc" > "$dir/status.log"
    test "$rc" -eq "$expected" && test ! -s "$dir/stdout.log" ||
        stack_fail "control.inventory-$label" "rc=$rc or unexpected stdout"
    if test "$expected" -eq 0; then
        test ! -s "$dir/diagnostics.log" || stack_fail "control.inventory-$label" 'unexpected diagnostic'
    else
        awk '
            index($0,"UNEXPECTED[stack.inventory]: ")!=1 {bad=1}
            END {exit (bad || NR==0)}
        ' "$dir/diagnostics.log" || stack_fail "control.inventory-$label" 'wrong rejection mechanism'
    fi
    printf 'PASS[control.inventory-%s]: expected rc=%s\n' "$label" "$expected"
}

inventory_case static 0 $'fixture.c:1:1:owner\t32\tstatic'
inventory_case mixed-bounded 0 \
    $'session.c:4803:14:moq_session_poll_actions_ex\t32\tdynamic,bounded' \
    $'session.c:4813:14:moq_core_internal_virtual_poll\t48\tdynamic,bounded' \
    $'fixture.c:1:1:owner\t64\tstatic'
inventory_case unbounded 1 $'fixture.c:1:1:owner\t32\tdynamic'
inventory_case bounded-alone 1 $'fixture.c:1:1:owner\t32\tbounded'
inventory_case contradictory 1 $'fixture.c:1:1:owner\t32\tstatic,bounded'
inventory_case repeated-qualifier 1 $'fixture.c:1:1:owner\t32\tdynamic,bounded,bounded'
inventory_case unknown-qualifier 1 $'fixture.c:1:1:owner\t32\tdynamic,bounded,unknown'
inventory_case missing-qualifier 1 $'fixture.c:1:1:owner\t32'
inventory_case extra-field 1 $'fixture.c:1:1:owner\t32\tdynamic,bounded\textra'
inventory_case negative-size 1 $'fixture.c:1:1:owner\t-32\tdynamic,bounded'
inventory_case nonnumeric-size 1 $'fixture.c:1:1:owner\t32x\tdynamic,bounded'
inventory_case missing-owner 1 $'fixture.c:1:1:\t32\tdynamic,bounded'
inventory_case duplicate-owner 1 \
    $'fixture.c:1:1:owner\t32\tstatic' $'fixture.c:2:1:owner\t48\tdynamic,bounded'
inventory_case empty 1

# The same exact-owner path and unchanged 1024-byte threshold apply to both
# finite forms. The fixture symbol record is deliberately synthetic too.
for qualifier in static dynamic,bounded; do
    for bytes in 1023 1024; do
        dir="$out/limit-$qualifier-$bytes"
        mkdir "$dir"
        printf 'fixture.c:1:1:moq_stack_fixture\t%s\t%s\n' "$bytes" "$qualifier" > "$dir/fixture.su"
        printf 'moq_stack_fixture T 0 1\n' > "$dir/fixture-symbols.log"
        stack_usage_validate "$dir/fixture.su"
        expected=0
        if test "$bytes" -eq 1024; then expected=1; fi
        (
            out="$dir"; symbol_prefix=; gate_failed=0
            stack_owner fixture moq_stack_fixture
            test "$measured_frame" -eq "$bytes" && test "$measured_qualifier" = "$qualifier" ||
                stack_fail control.bounded-limit 'measurement lost its size or qualifier'
            stack_limit
            test "$gate_failed" -eq "$expected"
        ) > "$dir/replay.log" 2> "$dir/replay.diagnostics.log" ||
            stack_fail control.bounded-limit "$qualifier/$bytes did not preserve the frame limit"
        test ! -s "$dir/replay.diagnostics.log" || stack_fail control.bounded-limit 'unexpected diagnostic'
        awk -v bytes="$bytes" -v qualifier="$qualifier" -v expected="$expected" '
            BEGIN {
                measure="MEASURE[moq_stack_fixture]: " bytes " bytes"
                if (qualifier=="dynamic,bounded") measure=measure " upper bound (dynamic,bounded)"
                measure=measure "; exact emitted owner moq_stack_fixture"
                failure="FAIL[frame.moq_stack_fixture]: 1024 bytes exceeds1023 bytes"
            }
            $0==measure {measured++; next}
            $0==failure {failed++; next}
            {bad=1}
            END {exit (bad || measured!=1 || failed!=expected)}
        ' "$dir/replay.log" || stack_fail control.bounded-limit 'wrong measurement or rejection'
        printf 'PASS[control.limit-%s-%s]: exact owner; limit result=%s\n' "$qualifier" "$bytes" "$expected"
    done
done

for gate in d22_setup session_goaway; do
    unit=profile
    if [[ $gate == session_goaway ]]; then unit=session; fi
    for control in missing compile_failure; do
        fixture="$root/tests/stack_gate_mechanism_control.c"
        expected=owner.missing
        if [[ $control == compile_failure ]]; then
            fixture="$root/tests/stack_gate_compile_failure.c"
            expected="$unit.compile"
        fi
        label="$gate-$control"
        command=(env "MEASURE_TU=$fixture" bash "$root/tests/check_${gate}_stack.sh" "$root" "$cc" "$out/$label")
        if [[ ${4:-} == --arm ]]; then command+=(--arm); fi
        printf '%q ' "${command[@]}" > "$out/$label.command.log"
        printf '\n' >> "$out/$label.command.log"
        if "${command[@]}" > "$out/$label.stdout.log" 2> "$out/$label.diagnostics.log"; then
            rc=0
        else
            rc=$?
        fi
        printf '%s\n' "$rc" > "$out/$label.status.log"
        test "$rc" -eq 1 && test ! -s "$out/$label.diagnostics.log" || {
            printf 'FAIL[control.%s]: rc=%s or unexpected gate diagnostic\n' "$label" "$rc"
            exit 1
        }
        awk -v tag="FAIL[$expected]:" '
            index($0,"FAIL[")==1 {n++; if(index($0,tag)!=1) bad=1}
            /^MEASURE\[moq_frame_probe\]:/ {positive++}
            /^PASS\[/ {bad=1}
            END {exit (bad || n!=1 || positive!=1)}
        ' "$out/$label.stdout.log" || { printf 'FAIL[control.%s]: wrong failure mechanism\n' "$label"; exit 1; }
        if [[ $control == compile_failure ]]; then
            awk '
                /:[0-9]+(:[0-9]+)?: error:/ {
                    n++; if(!index($0,"stack-gate deliberate compile-failure control")) bad=1
                }
                /warning:|note:/ {bad=1}
                END {exit (bad || n!=1)}
            ' "$out/$label/$unit.diagnostics.log" || { printf 'FAIL[control.%s]: wrong deliberate diagnostic\n' "$label"; exit 1; }
            test "$(< "$out/$label/$unit.status.log")" -ne 0
        else
            test "$(< "$out/$label/$unit.status.log")" -eq 0
        fi
        printf 'PASS[control.%s]: live positive; exact %s rejection\n' "$label" "$expected"
    done
done
