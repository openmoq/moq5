#!/usr/bin/env bash
# No relay executable may name the same static archive twice on its link line.
#
# The relay libraries form one linear chain, each PUBLIC-linking the next:
#
#     moqr-admin -> relay-obs -> relay-shard -> relay-bind -> relay-core -> moq::core
#
# so naming a lower member beside a higher one adds nothing to the link
# closure -- but the generator must then emit that archive twice (once where
# it was named, once where the closure requires it), and the linker reports
# `ignoring duplicate libraries`. A target names the HIGHEST chain member it
# needs and lets the closure supply the rest.
#
# The check reads the link commands the generator actually emits, so it holds
# for what is really passed to the linker rather than for what the
# declarations look like. Supported generators, named explicitly because an
# unrecognised one is refused rather than reported as clean:
#
#   Unix Makefiles  one link.txt per target, holding the whole shell command.
#   Ninja           the command is recovered from ninja itself
#                   (`-t commands`), so ninja's own escaping is undone by the
#                   tool that wrote it.
#
# Each command is split into arguments by `xargs`, which implements real
# quoting rules: an archive path containing spaces is quoted by CMake, and a
# plain word split would drop it from the check.
#
# NOTHING IS EVER COUNTED AS INSPECTED ON THE STRENGTH OF AN ABSENCE. A
# command that cannot be read, is empty, cannot be parsed, or carries an
# argument shape this check does not model (a `@response` file) fails by
# name; so does a tool invocation that exits nonzero, however plausible its
# output looked. A command with no archives at all is legitimate and IS
# inspected -- some relay executables link none.
#
# Portability: POSIX-ish Bash, no construct newer than the macOS system shell
# (3.2). An empty array is never expanded, which is an unbound variable there
# under `set -u`.
#
# Argument 1 is a configured build directory.
set -u
build="${1:-}"
[ -n "$build" ] && [ -d "$build" ] || {
    echo "FAIL: usage: $0 <build-dir>" >&2; exit 1; }
cache="$build/CMakeCache.txt"
[ -r "$cache" ] || { echo "FAIL: no CMakeCache.txt in $build" >&2; exit 1; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/moqr-linkgate-run.XXXXXX") || {
    echo "FAIL: could not create a working directory" >&2; exit 1; }
trap 'rm -rf "$tmp"' EXIT

cacheval() { sed -n "s|^$1:[^=]*=||p" "$cache" | head -1; }

fail=0
checked=0
reldir="tools/moq5-relay"

# Split one link command into arguments, or say exactly why it could not be.
# On success the arguments are in "$tmp/args", one per line, and there is at
# least one.
parse_cmd() {   # <target> <command string>
    local tgt="$1" cmd="$2"
    case "$cmd" in
        *[![:space:]]*) ;;
        *) echo "FAIL: $tgt: the link command is empty"
           fail=1
           return 1 ;;
    esac
    if ! printf '%s' "$cmd" | xargs -n1 > "$tmp/args" 2>/dev/null; then
        echo "FAIL: $tgt: the link command could not be parsed as arguments"
        fail=1
        return 1
    fi
    if [ ! -s "$tmp/args" ]; then
        echo "FAIL: $tgt: the link command parsed to no arguments"
        fail=1
        return 1
    fi
    return 0
}

check_cmd() {   # <target> <command string>
    local tgt="$1" rsp dup
    parse_cmd "$tgt" "$2" || return 0
    rsp=$(grep -m1 '^@' "$tmp/args" || true)
    if [ -n "$rsp" ]; then
        echo "FAIL: $tgt: the link command uses a response file ($rsp);" \
             "this check does not read one"
        fail=1
        return 0
    fi
    checked=$((checked + 1))
    dup=$(grep '\.a$' "$tmp/args" | sed 's|.*/||' | sort | uniq -d | tr '\n' ' ')
    if [ -n "${dup// /}" ]; then
        echo "FAIL: $tgt links a duplicated archive: $dup"
        fail=1
    fi
}

# A tool invocation is evidence only once its exit status says so.
tool_failed() {   # <label> <status> <stderr file>
    echo "FAIL: $1 exited $2; its output is not evidence"
    sed -n '1,2p' "$3" | sed 's/^/  /'
}

generator=$(cacheval CMAKE_GENERATOR)
case "$generator" in
"Unix Makefiles")
    while IFS= read -r lt; do
        # archive creation (ar/ranlib), not a link
        grep -qE '\bar[[:space:]]+(qc|cr|rcs)\b' "$lt" && continue
        tgt=$(basename "$(dirname "$lt")"); tgt="${tgt%.dir}"
        if ! cmd=$(cat "$lt" 2>/dev/null); then
            echo "FAIL: $tgt: its link command file could not be read ($lt)"
            fail=1
            continue
        fi
        check_cmd "$tgt" "$cmd"
    done < <(find "$build/$reldir" -name link.txt 2>/dev/null | sort)
    ;;
Ninja)
    ninja=$(cacheval CMAKE_MAKE_PROGRAM)
    [ -x "$ninja" ] || {
        echo "FAIL: the Ninja generator is configured but CMAKE_MAKE_PROGRAM" \
             "($ninja) is not executable" >&2; exit 1; }

    # 1. every relay executable ninja knows how to link
    "$ninja" -C "$build" -t targets all > "$tmp/targets" 2> "$tmp/targets.err"
    rc=$?
    if [ "$rc" -ne 0 ]; then
        tool_failed "ninja -t targets" "$rc" "$tmp/targets.err"
        exit 1
    fi
    sed -n "s|^\($reldir/[^:]*\): C\(XX\)\{0,1\}_EXECUTABLE_LINKER.*|\1|p" \
        "$tmp/targets" | sort -u > "$tmp/want"
    if [ ! -s "$tmp/want" ]; then
        echo "FAIL: ninja lists no relay executable link statement in $build" >&2
        exit 1
    fi
    want=()
    while IFS= read -r t; do
        [ -n "$t" ] && want+=("$t")
    done < "$tmp/want"
    # never expand an empty array: unbound under set -u on Bash 3.2
    if [ "${#want[@]}" -eq 0 ]; then
        echo "FAIL: ninja lists no relay executable link statement in $build" >&2
        exit 1
    fi

    # 2. their commands, matched back by the -o argument, so a link statement
    #    that yields no command is reported rather than skipped
    "$ninja" -C "$build" -t commands "${want[@]}" > "$tmp/cmds" 2> "$tmp/cmds.err"
    rc=$?
    if [ "$rc" -ne 0 ]; then
        tool_failed "ninja -t commands" "$rc" "$tmp/cmds.err"
        exit 1
    fi
    : > "$tmp/seen"
    while IFS= read -r cmd; do
        parse_cmd "one ninja command" "$cmd" >/dev/null 2>&1 || continue
        out=$(awk 'prev == "-o" { print; exit } { prev = $0 }' "$tmp/args")
        [ -n "$out" ] || continue
        grep -Fxq "$out" "$tmp/want" || continue
        check_cmd "${out##*/}" "$cmd"
        printf '%s\n' "$out" >> "$tmp/seen"
    done < "$tmp/cmds"
    while IFS= read -r t; do
        if ! grep -Fxq "$t" "$tmp/seen"; then
            echo "FAIL: ${t##*/}: ninja produced no usable link command for it"
            fail=1
        fi
    done < "$tmp/want"
    ;;
*)
    echo "FAIL: unsupported generator '$generator'; this check reads the link" \
         "commands of Unix Makefiles and Ninja only" >&2
    exit 1
    ;;
esac

if [ "$checked" -eq 0 ]; then
    echo "FAIL: no relay link command found under $build" >&2
    exit 1
fi
if [ "$fail" -ne 0 ]; then
    echo "FAIL: $checked relay link command(s) inspected; see the failures above"
    exit 1
fi
echo "PASS: $checked relay link commands ($generator) name each archive once"
