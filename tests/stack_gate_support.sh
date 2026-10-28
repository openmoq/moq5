#!/bin/bash
# Shared only by the two bounded, compile-only frame gates.
set -euo pipefail
export LC_ALL=C

stack_fail() { printf 'FAIL[%s]: %s\n' "$1" "$2"; exit 1; }

stack_init() {
    [[ $# -ge 3 && $# -le 4 && ( $# -eq 3 || $4 == --arm ) ]] ||
        stack_fail usage 'ROOT CONFIGURED_CC NEW_OUTPUT_DIR [--arm]'
    root=$(cd "$1" && pwd -P)
    cc=$2
    mkdir -p "$(dirname "$3")"
    out="$(cd "$(dirname "$3")" && pwd -P)/$(basename "$3")"
    mkdir "$out" || stack_fail output.exists 'fresh output directory required'
    df -Pk "$out" > "$out/headroom.log"
    local available
    available=$(awk 'NR==2 {print $4}' "$out/headroom.log")
    [[ $available =~ ^[0-9]+$ ]] && test "$available" -ge 1048576 ||
        stack_fail resource.guard 'less than 1 GiB available; no compile admitted'
    command -v "$cc" > "$out/compiler-path.log" || stack_fail compiler.missing "$cc"
    "$cc" --version > "$out/compiler.log" 2>&1 || stack_fail compiler.version "$cc"
    "$cc" -dumpmachine > "$out/compiler-target.log" 2>&1 || stack_fail compiler.target "$cc"
    local version
    version=$(< "$out/compiler.log")
    if [[ $version == *clang* ]]; then
        compiler_kind=clang
    elif [[ $version == *gcc* || $version == *GCC* ]]; then
        compiler_kind=gcc
    else
        stack_fail compiler.incapable 'no declared measurement mechanism'
    fi
    flags=(-std=c11 -O1 -g -Wall -Wextra -Wpedantic -fstack-usage)
    nm_tool=${NM:-nm}
    if [[ ${4:-} == --arm ]]; then
        [[ $compiler_kind == gcc ]] || stack_fail compiler.arm 'explicit GCC required'
        flags+=(-mcpu=cortex-m33 -mthumb)
        nm_tool=${NM:-$(dirname "$(< "$out/compiler-path.log")")/arm-none-eabi-nm}
    fi
    command -v "$nm_tool" > "$out/nm-path.log" || stack_fail mechanism.nm "$nm_tool"
    symbol_prefix=
    if [[ $(< "$out/compiler-target.log") == *darwin* ]]; then symbol_prefix=_; fi
    gate_failed=0
}

stack_run() {
    local label=$1 rc
    shift
    printf '%q ' "$@" > "$out/$label.command.log"
    printf '\n' >> "$out/$label.command.log"
    if "$@" > "$out/$label.stdout.log" 2> "$out/$label.diagnostics.log"; then
        rc=0
    else
        rc=$?
    fi
    printf '%s\n' "$rc" > "$out/$label.status.log"
    test "$rc" -eq 0 || stack_fail "$label.compile" "command failed rc=$rc; no absence conclusion"
    test ! -s "$out/$label.stdout.log" || stack_fail diagnostic.unexpected "$label stdout"
}

stack_clang_diagnostics() {
    # Admit only intentional positive frame warnings or exact inline remarks,
    # with their complete source/caret context. All other output fails closed.
    awk -v mode="$1" '
        BEGIN {
            q=sprintf("%c",39); ident="[A-Za-z_][A-Za-z_0-9]*"
            frame="^.+:[0-9]+:[0-9]+: warning: stack frame size \\([0-9]+\\) exceeds limit \\(1023\\) in " q "moq_frame_probe" q " \\[-Wframe-larger-than\\]$"
            inline="^.+:[0-9]+:[0-9]+: remark: " q ident q " inlined into " q ident q " with \\(cost=-?[0-9]+, threshold=[0-9]+\\) at callsite " ident ":[0-9]+:[0-9]+; \\[-Rpass=inline\\]$"
        }
        function reject(line) {
            print "UNEXPECTED[clang.diagnostic]: " line > "/dev/stderr"; bad=1
        }
        function finish() {
            if (context && (source!=1 || caret!=1)) reject("incomplete source/caret context")
            context=source=caret=0
        }
        /^[[:space:]]*$/ {next}
        /^In file included from .+:[0-9]+:$/ {
            finish(); if (mode!="inline" || summary) reject($0); includes++; next
        }
        $0 ~ frame {
            finish()
            size=$0; sub(/^.*stack frame size \(/,"",size); sub(/\).*$/,"",size)
            if (mode!="positive" || summary || ++warnings!=1 || size+0<4096) reject($0)
            printf "FRAME\tmoq_frame_probe\t%s\n",size
            context=1; includes=0; next
        }
        $0 ~ inline {
            finish(); if (mode!="inline" || summary) reject($0)
            split($0, quoted, q)
            printf "INLINE\t%s\t%s\n",quoted[2],quoted[4]
            context=1; includes=0; next
        }
        /^[[:space:]]*[0-9]+[[:space:]]+\|/ {
            if (!context || caret || ++source!=1) reject($0); next
        }
        /^[[:space:]]*\|[[:space:]]*[~^]+[[:space:]]*$/ {
            if (!context || !source || ++caret!=1) reject($0); next
        }
        /^[0-9]+ warnings? generated\.$/ {
            finish()
            if (mode!="positive" || warnings!=1 || includes || ++summary!=1 || $0+0!=warnings) reject($0)
            next
        }
        {reject($0)}
        END {
            finish()
            if (includes || (mode=="positive" && (warnings!=1 || summary!=1))) reject("missing diagnostic/summary")
            exit bad
        }
    ' "$2"
}

stack_usage_validate() {
    # GCC -fstack-usage: dynamic,bounded includes compile-time-bounded
    # adjustments in the function body (including outgoing call arguments).
    # Its byte count is an upper bound, not evidence of an unbounded VLA.
    # https://gcc.gnu.org/onlinedocs/gcc-14.3.0/gcc/Developer-Options.html#index-fstack-usage
    awk -F '\t' '
        {
            name=$1; sub(/^.*:/,"",name)
            if (NF!=3 || name=="" || ++seen[name]!=1 || $2!~/^[0-9]+$/ ||
                ($3!="static" && $3!="dynamic,bounded")) {
                print "UNEXPECTED[stack.inventory]: " $0 > "/dev/stderr"; bad=1
            }
        }
        END {
            if (NR==0) {
                print "UNEXPECTED[stack.inventory]: empty inventory" > "/dev/stderr"; bad=1
            }
            exit bad
        }
    ' "$1"
}

stack_inventory() {
    local unit=$1
    test -s "$out/$unit.o" && test -s "$out/$unit.su" ||
        stack_fail measurement.missing "$unit object or stack-usage inventory absent"
    stack_usage_validate "$out/$unit.su" ||
        stack_fail measurement.invalid "$unit malformed/ambiguous/unbounded inventory"
    printf '%q ' "$nm_tool" -P "$out/$unit.o" > "$out/$unit-nm.command.log"
    printf '\n' >> "$out/$unit-nm.command.log"
    local rc
    if "$nm_tool" -P "$out/$unit.o" > "$out/$unit-symbols.log" 2> "$out/$unit-nm.diagnostics.log"; then rc=0; else rc=$?; fi
    printf '%s\n' "$rc" > "$out/$unit-nm.status.log"
    test "$rc" -eq 0 && test ! -s "$out/$unit-nm.diagnostics.log" ||
        stack_fail mechanism.symbols "$unit failed/diagnostic symbol inventory"
    awk 'NF<2 || $2!~/^[A-Za-z?]$/ {bad=1} END {exit bad}' "$out/$unit-symbols.log" ||
        stack_fail mechanism.symbols "$unit unexpected nm output"
}

stack_compile() {
    local unit=$1 source=$2
    local extra=(-Werror)
    if [[ $compiler_kind == clang ]]; then
        extra+=(-Rpass=inline)
    else
        extra+=("-fdump-ipa-inline=$out/$unit-inline.log")
    fi
    stack_run "$unit" "$cc" "${flags[@]}" "${extra[@]}" "-I$root/core/include" -c "$source" -o "$out/$unit.o"
    if [[ $compiler_kind == clang ]]; then
        stack_clang_diagnostics inline "$out/$unit.diagnostics.log" > "$out/$unit-inline.tsv" ||
            stack_fail diagnostic.unexpected "$unit"
    else
        test ! -s "$out/$unit.diagnostics.log" || stack_fail diagnostic.unexpected "$unit"
    fi
    stack_inventory "$unit"
}

stack_symbol_count() {
    awk -v name="$symbol_prefix$2" '$1==name && $2~/^[tT]$/ {n++} END {print n+0}' "$out/$1-symbols.log"
}

stack_frame() {
    awk -F '\t' -v name="$2" '
        {owner=$1; sub(/^.*:/,"",owner)}
        owner==name {n++; size=$2; qualifier=$3}
        END {if(n!=1) exit 1; printf "%s\t%s\n",size,qualifier}
    ' "$out/$1.su"
}

stack_inline_count() {
    local unit=$1 child=$2 parent=$3
    if [[ $compiler_kind == clang ]]; then
        awk -F '\t' -v child="$child" -v parent="$parent" '
            $1=="INLINE" && $2==child && $3==parent {n++} END {print n+0}
        ' "$out/$unit-inline.tsv"
    else
        awk -v child="$child" -v parent="$parent" '
            $0 ~ ("^  Function " child "/[0-9]+ is inline copy in " parent "/[0-9]+$") {n++}
            END {print n+0}
        ' "$out/$unit-inline.log"
    fi
}

stack_inline() {
    test "$(stack_inline_count "$@")" -eq 1
}

stack_owner() {
    local unit=$1 child=$2 parent=${3:-} emitted_parent=${4:-${3:-}} count su_count relation_count record
    count=$(stack_symbol_count "$unit" "$child")
    su_count=$(awk -F '\t' -v name="$child" '{owner=$1; sub(/^.*:/,"",owner); if(owner==name)n++} END {print n+0}' "$out/$unit.su")
    measured_owner=$child
    if test "$count" -eq 0 && test "$su_count" -eq 0; then
        test -n "$parent" ||
            stack_fail owner.missing "$child absent; no exact inline relation"
        relation_count=$(stack_inline_count "$unit" "$child" "$parent")
        if test "$relation_count" -eq 1; then
            if [[ $parent != "$emitted_parent" ]]; then
                stack_inline "$unit" "$parent" "$emitted_parent" ||
                    stack_fail owner.missing "$parent lacks exact emitted-owner relation"
            fi
            printf 'INLINE[%s]: %s; emitted owner %s\n' "$child" "$parent" "$emitted_parent"
        elif test "$relation_count" -eq 0 && [[ $parent != "$emitted_parent" ]] &&
             stack_inline "$unit" "$child" "$emitted_parent" &&
             stack_inline "$unit" "$parent" "$emitted_parent"; then
            # GCC names the final emitted owner of a nested inline copy.
            printf 'INLINE[%s]: compiler owner %s; intermediate %s in same owner\n' \
                "$child" "$emitted_parent" "$parent"
        else
            stack_fail owner.missing "$child lacks unique direct or flattened inline proof"
        fi
        measured_owner=$emitted_parent
    else
        test "$count" -eq 1 && test "$su_count" -eq 1 ||
            stack_fail owner.invalid "$child missing/duplicated symbol or .su entry"
    fi
    test "$(stack_symbol_count "$unit" "$measured_owner")" -eq 1 ||
        stack_fail owner.missing "$measured_owner not emitted exactly once"
    record=$(stack_frame "$unit" "$measured_owner") ||
        stack_fail owner.missing "$measured_owner lacks unique bounded frame"
    IFS=$'\t' read -r measured_frame measured_qualifier <<< "$record"
    if [[ $measured_qualifier == dynamic,bounded ]]; then
        printf 'MEASURE[%s]: %s bytes upper bound (dynamic,bounded); exact emitted owner %s\n' \
            "$child" "$measured_frame" "$measured_owner"
    else
        printf 'MEASURE[%s]: %s bytes; exact emitted owner %s\n' "$child" "$measured_frame" "$measured_owner"
    fi
}

stack_owner_controls() {
    local unit=$1 child=$2 parent=$3 emitted=$4 subject mutation label dir rc
    [[ $compiler_kind == gcc && $parent != "$emitted" ]] &&
        stack_inline "$unit" "$child" "$emitted" &&
        stack_inline "$unit" "$parent" "$emitted" ||
        stack_fail control.evidence 'exact flattened compiler relations required'
    mkdir "$out/owner-controls" || stack_fail control.output 'fresh control directory required'
    for subject in "$child" "$parent" owner-symbol owner-frame; do
        for mutation in missing misnamed duplicate; do
            if [[ $subject == owner-* && $mutation == misnamed ]]; then continue; fi
            label="$subject-$mutation"
            dir="$out/owner-controls/$label"
            mkdir "$dir"
            cp "$out/$unit-symbols.log" "$out/$unit.su" "$out/$unit-inline.log" "$dir/"
            if [[ $subject == owner-symbol ]]; then
                awk -v name="$symbol_prefix$emitted" -v mutation="$mutation" '
                    $1==name && $2~/^[tT]$/ {
                        n++; if(mutation=="duplicate") {print; print}; next
                    }
                    {print}
                    END {exit (n!=1)}
                ' "$out/$unit-symbols.log" > "$dir/$unit-symbols.log" ||
                    stack_fail control.evidence "$label needs one real symbol"
            elif [[ $subject == owner-frame ]]; then
                awk -F '\t' -v name="$emitted" -v mutation="$mutation" '
                    {owner=$1; sub(/^.*:/,"",owner)}
                    owner==name {
                        n++; if(mutation=="duplicate") {print; print}; next
                    }
                    {print}
                    END {exit (n!=1)}
                ' "$out/$unit.su" > "$dir/$unit.su" ||
                    stack_fail control.evidence "$label needs one real frame"
            else
                awk -v child="$subject" -v owner="$emitted" -v mutation="$mutation" '
                    $0 ~ ("^  Function " child "/[0-9]+ is inline copy in " owner "/[0-9]+$") {
                        n++
                        if(mutation=="duplicate") {print; print}
                        if(mutation=="misnamed") {
                            sub(" in " owner "/", " in moq_wrong_frame_owner/"); print
                        }
                        next
                    }
                    {print}
                    END {exit (n!=1)}
                ' "$out/$unit-inline.log" > "$dir/$unit-inline.log" ||
                    stack_fail control.evidence "$label needs one real relation"
            fi
            if (out="$dir"; stack_owner "$unit" "$child" "$parent" "$emitted") \
                    > "$dir/replay.log" 2> "$dir/replay.diagnostics.log"; then rc=0; else rc=$?; fi
            printf '%s\n' "$rc" > "$dir/replay.status.log"
            test "$rc" -eq 1 && test ! -s "$dir/replay.diagnostics.log" ||
                stack_fail "control.$label" "rc=$rc or unexpected diagnostic"
            awk '
                /^FAIL\[/ {n++; if(index($0,"FAIL[owner.missing]:")!=1) bad=1}
                /^MEASURE\[|^PASS\[/ {bad=1}
                END {exit (bad || n!=1)}
            ' "$dir/replay.log" || stack_fail "control.$label" 'wrong rejection mechanism'
            printf 'PASS[control.%s]: exact owner.missing rejection\n' "$label"
        done
    done
}

stack_limit() {
    if test "$measured_frame" -ge 1024; then
        printf 'FAIL[frame.%s]: %s bytes exceeds1023 bytes\n' "$measured_owner" "$measured_frame"
        gate_failed=1
    fi
}

stack_positive() {
    local probe=${POSITIVE_CONTROL:-$root/tests/stack_positive_control.c}
    if [[ $compiler_kind == clang ]]; then
        stack_run positive "$cc" "${flags[@]}" -Wframe-larger-than=1023 -c "$probe" -o "$out/positive.o"
        stack_clang_diagnostics positive "$out/positive.diagnostics.log" > "$out/positive-inventory.tsv" ||
            stack_fail diagnostic.unexpected positive
        stack_inventory positive
    else
        stack_compile positive "$probe"
    fi
    stack_owner positive moq_frame_probe
    test "$measured_frame" -ge 4096 || stack_fail mechanism.dead 'live positive frame below4096 bytes'
    if [[ $compiler_kind == clang ]]; then
        local warned
        warned=$(awk -F '\t' '$1=="FRAME" {print $3}' "$out/positive-inventory.tsv")
        test "$warned" -eq "$measured_frame" || stack_fail mechanism.frame 'warning/.su disagreement'
    fi
}
