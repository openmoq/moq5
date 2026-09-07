#!/usr/bin/env bash
# When the WebTransport create-boundary test target exists at all.
#
# A test target is a build-option decision, not a source decision: with
# MOQ_BUILD_TESTS=OFF a downstream builds the command and nothing else, and a
# test executable that still lands in the default ALL build makes that build
# depend on a header and a test harness the downstream never asked for. Only
# the generated build system can settle this -- the CMake source reads the same
# either way -- so this configures real trees and looks at what came out.
#
# Configure only: nothing is compiled, no binary runs, no listener starts.
set -u
usage='usage: check_wt_cfg_target_gating.sh <source-root> <cmake> <wtquic_DIR>'\
' <msquic_DIR> [<compiler>] [<prefix-list>] [<openssl-root>]'
srcroot=${1:?$usage}
cmake_bin=${2:?$usage}
# The package directories the PARENT already resolved. A caller may have found
# its dependencies through a search list, through explicit package directories
# with no search list at all, or through a list of several prefixes -- so the
# resolved directories are the one input that means the same thing in every
# case. A search list is not an install prefix and is not path-tested here.
wtquic_dir=${3:-}
msquic_dir=${4:-}
cc=${5:-}
# Supplemental only, forwarded verbatim so CMake still reads its ";" as list
# separators. Never treated as a single pathname.
prefix_list=${6:-}
openssl_root=${7:-}

fail=0
note() { printf '  %s\n' "$*"; }
work=$(mktemp -d); trap 'rm -rf "$work"' EXIT

# The target under test, and a control that must keep its own gating.
target=test_relay_wt_cfg
testname=relay_wt_cfg

configure() {  # <dir> <tests: ON|OFF> <facade: yes|no>
    local dir=$1 tests=$2 facade=$3
    local args=(-S "$srcroot" -B "$dir" -G "Unix Makefiles"
                -DCMAKE_BUILD_TYPE=Debug
                -DMOQ_BUILD_RELAY=ON
                "-DMOQ_BUILD_TESTS=$tests"
                -DMOQ_BUILD_ADAPTER_MSQUIC=ON
                -DMOQ_BUILD_MSQUIC_MANAGED=ON)
    if [ -n "$cc" ]; then args+=("-DCMAKE_C_COMPILER=$cc"); fi
    # The raw listener needs MsQuic in EVERY case, so the selected package
    # travels with all of them: a child left to search on its own could bind a
    # different installation than the parent chose, or none at all.
    if [ -n "$msquic_dir" ]; then args+=("-Dmsquic_DIR=$msquic_dir"); fi
    if [ -n "$prefix_list" ]; then
        args+=("-DCMAKE_PREFIX_PATH=$prefix_list")
    fi
    if [ -n "$openssl_root" ]; then
        args+=("-DOPENSSL_ROOT_DIR=$openssl_root")
    fi
    if [ "$facade" = yes ]; then
        args+=(-DMOQ_BUILD_ADAPTER_WTQUIC=ON
               -DMOQ_BUILD_WTQUIC_MSQUIC_MANAGED=ON
               "-Dwtquic_DIR=$wtquic_dir")
    fi
    "$cmake_bin" "${args[@]}" > "$dir.log" 2>&1
}

# Did the child bind the packages this gate selected, rather than finding some
# other installation?
cache_says() {  # <dir> <var> <want>
    local got
    got=$(grep -m1 "^$2:" "$1/CMakeCache.txt" 2>/dev/null | sed 's/^[^=]*=//')
    [ "$got" = "$3" ]
}

# Is the target in the generated build system at all, and is its test
# registered? Both are read from generated files, never from the CMake source.
target_present() {  # <dir> -> 0 when the target was generated
    grep -q "/${target}\.dir\$" "$1/CMakeFiles/TargetDirectories.txt" 2>/dev/null
}
in_default_all() {  # <dir> -> 0 when ALL depends on it
    grep -q "all: .*CMakeFiles/${target}\.dir/all" \
        "$1/CMakeFiles/Makefile2" 2>/dev/null
}
test_registered() {  # <dir> -> 0 when the ctest name exists
    grep -qs "add_test(\[=\[${testname}\]=\]" \
        "$1/tools/moq5-relay/CTestTestfile.cmake"
}

case_check() {  # <label> <tests> <facade> <want: yes|no>
    local dir="$work/$1"
    if ! configure "$dir" "$2" "$3"; then
        echo "FAIL: $1: configure failed"
        tail -5 "$dir.log" | sed 's/^/    /'
        fail=1
        return
    fi
    local got=no
    target_present "$dir" && got=yes
    if [ "$got" != "$4" ]; then
        echo "FAIL: $1 (tests=$2 facade=$3): target $got, wanted $4"
        fail=1
    fi
    if [ "$4" = no ]; then
        # a target that is absent cannot be in ALL or registered either
        if in_default_all "$dir"; then
            echo "FAIL: $1: the target is still in the default ALL build"
            fail=1
        fi
        if test_registered "$dir"; then
            echo "FAIL: $1: the test is still registered"
            fail=1
        fi
    else
        in_default_all "$dir" || {
            echo "FAIL: $1: the target exists but ALL does not build it"
            fail=1; }
        test_registered "$dir" || {
            echo "FAIL: $1: the target exists but its test is not registered"
            fail=1; }
    fi
}

# The facade-present cases need the WebTransport package the parent resolved.
# This test is only registered where the parent found that facade, so a missing
# or unusable directory here is a registration defect, not a missing dependency.
if [ -z "$wtquic_dir" ] || [ ! -d "$wtquic_dir" ] ||
   [ ! -f "$wtquic_dir/wtquicConfig.cmake" ]; then
    echo "FAIL: the resolved wtquic package directory is required; got"\
         "'${wtquic_dir}'. The facade-present cases cannot run without it."
    exit 1
fi
if [ -n "$msquic_dir" ] && [ ! -d "$msquic_dir" ]; then
    echo "FAIL: the resolved msquic package directory does not exist:"\
         "'${msquic_dir}'"
    exit 1
fi

case_check tests_on_facade_yes  ON  yes yes
case_check tests_off_facade_yes OFF yes no
case_check tests_on_facade_no   ON  no  no
case_check tests_off_facade_no  OFF no  no
[ "$fail" -eq 0 ] && note "target gating: present only with tests ON and the facade available"

# The children used the packages this gate selected, not whatever they found.
for d in tests_on_facade_yes tests_off_facade_yes; do
    cache_says "$work/$d" wtquic_DIR "$wtquic_dir" || {
        echo "FAIL: $d did not bind the selected wtquic package"
        fail=1; }
done
if [ -n "$openssl_root" ]; then
    for d in tests_on_facade_yes tests_off_facade_yes; do
        cache_says "$work/$d" OPENSSL_ROOT_DIR "$openssl_root" || {
            echo "FAIL: $d did not bind the selected OpenSSL root"
            fail=1; }
    done
fi
if [ -n "$msquic_dir" ]; then
    for d in tests_on_facade_yes tests_on_facade_no; do
        cache_says "$work/$d" msquic_DIR "$msquic_dir" || {
            echo "FAIL: $d did not bind the selected msquic package"
            fail=1; }
    done
fi
[ "$fail" -eq 0 ] && note "packages: children bound the selected dependency inputs"

# The production step itself is NOT a test artefact: with tests off the command
# still gains its WebTransport listener wherever the facade exists.
if grep -q "cli/wtcfg\.c" "$work/tests_off_facade_yes/tools/moq5-relay/CMakeFiles/moq5-relay.dir/build.make" 2>/dev/null; then
    note "production: cli/wtcfg.c stays in the command with tests off"
else
    echo "FAIL: cli/wtcfg.c left the command when tests were disabled"
    fail=1
fi

[ "$fail" -eq 0 ] && echo "PASS: wt_cfg target gating"
exit "$fail"
