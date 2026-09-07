#!/usr/bin/env bash
# The link-hygiene gate, exercised against link commands whose verdict is
# known in advance.
#
# The gate reads generated link commands, so its own defects are invisible to
# the relay build: a command it fails to interpret simply never reports
# anything. The cases below therefore include the shapes that could pass
# unexamined -- an archive path in quotes (which CMake writes whenever the
# path contains a space), a command that cannot be parsed at all, and an
# argument shape the gate does not model -- and each is required to FAIL
# rather than to be silently clean.
#
# The last cases are not hand-written text: CMake configures a small project
# under a directory whose name contains a space, once with the redundant link
# edge and once without, for every generator the gate claims to support. The
# generator writes the quoting; nothing here imitates it.
#
# The gate is always run through THIS script's own interpreter, never through
# its shebang: the two differ on macOS (system Bash 3.2 versus a newer one on
# PATH), and a gate that only works on the newer one would otherwise be
# reported as clean by a selftest running under the older one.
#
# Argument 1 is the gate script, 2 the cmake binary, 3 (optional) ninja.
set -u
gate="${1:?usage: $0 <check_link_hygiene.sh> <cmake> [ninja]}"
cmake_bin="${2:?usage: $0 <check_link_hygiene.sh> <cmake> [ninja]}"
ninja_bin="${3:-}"
[ -r "$gate" ] || { echo "FAIL: cannot read the gate script ($gate)" >&2; exit 1; }
# The interpreter under test: this script's own, passed explicitly to the gate.
sh_under_test="${BASH:-/bin/bash}"
[ -x "$sh_under_test" ] || {
    echo "FAIL: the running interpreter ($sh_under_test) is not executable" >&2
    exit 1; }
echo "interpreter under test: $sh_under_test ${BASH_VERSION:-unknown}"
[ -x "$cmake_bin" ] || { echo "FAIL: cmake ($cmake_bin) is not executable" >&2; exit 1; }

work=$(mktemp -d "${TMPDIR:-/tmp}/moqr-linkgate.XXXXXX")
trap 'rm -rf "$work"' EXIT
fail=0
cases=0

# Run the gate on a build directory and require an exact verdict.
#   expect_fail <name> <dir> <substring the output must contain>
#   expect_pass <name> <dir>
expect_fail() {
    local name="$1" dir="$2" want="$3" out rc
    cases=$((cases + 1))
    out=$("$sh_under_test" "$gate" "$dir" 2>&1); rc=$?
    if [ "$rc" -eq 0 ]; then
        echo "FAIL: $name: the gate PASSED a link line it must refuse"
        printf '  %s\n' "$out"
        fail=1
    elif ! printf '%s' "$out" | grep -qF "$want"; then
        echo "FAIL: $name: refused, but not for the stated reason (want '$want')"
        printf '  %s\n' "$out"
        fail=1
    fi
}
expect_pass() {
    local name="$1" dir="$2" out rc
    cases=$((cases + 1))
    out=$("$sh_under_test" "$gate" "$dir" 2>&1); rc=$?
    if [ "$rc" -ne 0 ]; then
        echo "FAIL: $name: the gate refused a clean link line"
        printf '  %s\n' "$out"
        fail=1
    fi
}

# -- hand-built commands: one link.txt, one known verdict -------------------
mk() {   # <dir> <generator> <command line>
    local d="$work/$1/tools/moq5-relay/CMakeFiles/moq5-relay.dir"
    mkdir -p "$d"
    printf 'CMAKE_GENERATOR:INTERNAL=%s\n' "$2" > "$work/$1/CMakeCache.txt"
    printf '%s\n' "$3" > "$d/link.txt"
}

mk plain "Unix Makefiles" \
   '/usr/bin/cc main.c.o -o moq5-relay /tmp/core/libmoq-core.a /tmp/core/libmoq-core.a'
expect_fail "plain duplicate" "$work/plain" "links a duplicated archive"

# The reviewed gap: the same duplicate with the paths quoted, as CMake writes
# them when a path contains a space.
mk quoted "Unix Makefiles" \
   '/usr/bin/cc main.c.o -o moq5-relay "/tmp/core space/libmoq-core.a" "/tmp/core space/libmoq-core.a"'
expect_fail "quoted duplicate" "$work/quoted" "links a duplicated archive"

mk quoted_ok "Unix Makefiles" \
   '/usr/bin/cc main.c.o -o moq5-relay "/tmp/core space/libmoq-core.a" "/tmp/core space/libmoq-relay-core.a"'
expect_pass "quoted non-duplicate control" "$work/quoted_ok"

mk unparsable "Unix Makefiles" \
   '/usr/bin/cc main.c.o -o moq5-relay "/tmp/core space/libmoq-core.a'
expect_fail "unterminated quote" "$work/unparsable" "could not be parsed"

mk response "Unix Makefiles" \
   '/usr/bin/cc main.c.o -o moq5-relay @CMakeFiles/moq5-relay.rsp'
expect_fail "response file" "$work/response" "response file"

# An absent command is not a clean one: xargs succeeds on empty input, so the
# command itself has to be shown to exist before anything is called inspected.
mk emptycmd "Unix Makefiles" ''
expect_fail "empty link command" "$work/emptycmd" "the link command is empty"

mk blankcmd "Unix Makefiles" '   '
expect_fail "whitespace-only link command" "$work/blankcmd" "the link command is empty"

# A command with no archive at all is legitimate -- some relay executables
# link none -- and must be inspected, not refused.
mk noarchive "Unix Makefiles" '/usr/bin/cc main.c.o -o moq5-relay -lm'
expect_pass "archive-free command" "$work/noarchive"

mk unreadable "Unix Makefiles" \
   '/usr/bin/cc main.c.o -o moq5-relay /tmp/libmoq-core.a /tmp/libmoq-core.a'
chmod 000 "$work/unreadable/tools/moq5-relay/CMakeFiles/moq5-relay.dir/link.txt"
if [ "$(id -u)" = "0" ]; then
    echo "NOTE: running as root; the unreadable-input case did NOT run"
else
    expect_fail "unreadable link command file" "$work/unreadable" "could not be read"
fi

mk unknown_gen "Green Hills MULTI" \
   '/usr/bin/cc main.c.o -o moq5-relay /tmp/core/libmoq-core.a'
expect_fail "unsupported generator" "$work/unknown_gen" "unsupported generator"

mkdir -p "$work/nocache/tools/moq5-relay"
expect_fail "no CMakeCache" "$work/nocache" "no CMakeCache.txt"

mkdir -p "$work/empty"
printf 'CMAKE_GENERATOR:INTERNAL=Unix Makefiles\n' > "$work/empty/CMakeCache.txt"
expect_fail "no link command at all" "$work/empty" "no relay link command"

cases=$((cases + 1))
if "$sh_under_test" "$gate" >/dev/null 2>&1; then
    echo "FAIL: the gate accepted a missing build directory"
    fail=1
fi

# -- a ninja whose tools fail: plausible output, nonzero exit ---------------
# Each tool invocation is guarded separately, so each fixture fails exactly
# one of them and leaves the other clean. The command emitted by the failing
# fixtures is itself clean: the only reason to refuse is the exit status.
mkninja() {   # <dir> <targets rc> <commands rc> <command line>
    local d="$work/$1"
    mkdir -p "$d"
    cat > "$d/fake-ninja" <<EOF
#!/bin/sh
# invoked as: ninja -C <build> -t <tool> [targets...]
case "\$4" in
    targets)  printf '%s\n' 'tools/moq5-relay/moq5-relay: C_EXECUTABLE_LINKER__moq5-relay_Debug'
              exit $2 ;;
    commands) printf '%s\n' '$4'
              exit $3 ;;
esac
exit 9
EOF
    chmod +x "$d/fake-ninja"
    {
        printf 'CMAKE_GENERATOR:INTERNAL=Ninja\n'
        printf 'CMAKE_MAKE_PROGRAM:FILEPATH=%s/fake-ninja\n' "$d"
    } > "$d/CMakeCache.txt"
}

clean_cmd='/usr/bin/cc main.c.o -o tools/moq5-relay/moq5-relay /tmp/libmoq-core.a'
dup_cmd='/usr/bin/cc main.c.o -o tools/moq5-relay/moq5-relay /tmp/libmoq-core.a /tmp/libmoq-core.a'

mkninja ninja_targets_fail 1 0 "$clean_cmd"
expect_fail "ninja -t targets exits nonzero" "$work/ninja_targets_fail" \
            "ninja -t targets exited 1"

mkninja ninja_commands_fail 0 1 "$clean_cmd"
expect_fail "ninja -t commands exits nonzero" "$work/ninja_commands_fail" \
            "ninja -t commands exited 1"

mkninja ninja_both_fail 1 1 "$clean_cmd"
expect_fail "both ninja tools exit nonzero" "$work/ninja_both_fail" \
            "exited 1"

# the all-zero controls: the same path must really inspect the command
mkninja ninja_ok_dup 0 0 "$dup_cmd"
expect_fail "ninja tools succeed, duplicate archive" "$work/ninja_ok_dup" \
            "links a duplicated archive"

mkninja ninja_ok_clean 0 0 "$clean_cmd"
expect_pass "ninja tools succeed, clean command" "$work/ninja_ok_clean"

# -- CMake-generated commands, with a space in an archive path --------------
# CMake writes link arguments relative to the build directory, so a space in
# the build path alone never reaches an archive argument: the libraries live
# behind a directory whose own name has a space, which is what makes the
# generator quote them. fixdup then names the lower library beside the higher
# one that already brings it -- the real defect shape, written by the real
# generator.
src="$work/fix ture/src"
mkdir -p "$src/tools/moq5-relay/lib dir"
cat > "$src/CMakeLists.txt" <<'EOF'
cmake_minimum_required(VERSION 3.20)
project(link_gate_fixture C)
option(FIXTURE_REDUNDANT_EDGE "name the lower chain member as well" OFF)
add_subdirectory(tools/moq5-relay)
EOF
cat > "$src/tools/moq5-relay/CMakeLists.txt" <<'EOF'
add_subdirectory("lib dir")
add_executable(moq5-relay main.c)
if(FIXTURE_REDUNDANT_EDGE)
    target_link_libraries(moq5-relay PRIVATE fixlow fixhigh)
else()
    target_link_libraries(moq5-relay PRIVATE fixhigh)
endif()
EOF
cat > "$src/tools/moq5-relay/lib dir/CMakeLists.txt" <<'EOF'
add_library(fixlow STATIC low.c)
add_library(fixhigh STATIC high.c)
target_link_libraries(fixhigh PUBLIC fixlow)
EOF
printf 'int low(void){return 1;}\n'  > "$src/tools/moq5-relay/lib dir/low.c"
printf 'int high(void){return 2;}\n' > "$src/tools/moq5-relay/lib dir/high.c"
printf 'int main(void){return 0;}\n' > "$src/tools/moq5-relay/main.c"

# The link command as the generator would hand it to a shell.
generated_command() {   # <build dir> <generator>
    if [ "$2" = "Ninja" ]; then
        "$ninja_bin" -C "$1" -t commands tools/moq5-relay/moq5-relay 2>/dev/null |
            tail -1
    else
        cat "$1/tools/moq5-relay/CMakeFiles/moq5-relay.dir/link.txt" 2>/dev/null
    fi
}

gen_case() {   # <generator> <dir suffix> <extra cmake args...>
    local gen="$1" tag="$2"; shift 2
    local bdup="$work/fix ture/build dup $tag" bok="$work/fix ture/build ok $tag"
    if ! "$cmake_bin" -S "$src" -B "$bdup" -G "$gen" "$@" \
            -DFIXTURE_REDUNDANT_EDGE=ON >"$work/cfg.log" 2>&1; then
        echo "FAIL: $gen: configuring the redundant fixture failed"
        tail -3 "$work/cfg.log"
        fail=1
        return
    fi
    if ! "$cmake_bin" -S "$src" -B "$bok" -G "$gen" "$@" \
            -DFIXTURE_REDUNDANT_EDGE=OFF >>"$work/cfg.log" 2>&1; then
        echo "FAIL: $gen: configuring the control fixture failed"
        fail=1
        return
    fi
    # The point of the fixture is a QUOTED archive argument. If the generator
    # stopped quoting, the reviewed gap would go uncovered here, so this is a
    # failure of the case rather than a remark.
    cases=$((cases + 1))
    local quoted
    quoted=$(generated_command "$bdup" "$gen" | grep -o '"[^"]*\.a"' | wc -l | tr -d ' ')
    if [ "${quoted:-0}" -lt 1 ]; then
        echo "FAIL: $gen: the generated link command quotes no archive path,"\
             "so the quoted-argument case is not being exercised"
        fail=1
    fi
    expect_fail "$gen generated, redundant edge" "$bdup" "links a duplicated archive"
    expect_pass "$gen generated, control" "$bok"
}

gen_case "Unix Makefiles" make
# Argument 3 must really be ninja: this tree's own make program is whatever
# generator built it, and handing that to -G Ninja would fail for a reason
# that has nothing to do with the gate.
if [ -n "$ninja_bin" ] && [ -x "$ninja_bin" ] &&
   "$ninja_bin" --version >/dev/null 2>&1 &&
   case "${ninja_bin##*/}" in *ninja*) true ;; *) false ;; esac; then
    gen_case "Ninja" ninja "-DCMAKE_MAKE_PROGRAM=$ninja_bin"
else
    echo "NOTE: no usable ninja was supplied (${ninja_bin:-none}); the Ninja"\
         "generated-command cases did NOT run. The Ninja build trees are"\
         "still gated by relay_link_hygiene itself."
fi

if [ "$fail" -ne 0 ]; then
    echo "FAIL: $cases link-gate cases, see the failures above"
    exit 1
fi
echo "PASS: $cases link-gate cases, including quoted, unparsable and generated commands"
