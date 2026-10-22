#!/usr/bin/env bash
# The gates' own controls, run as a row: inputs that must fail the
# diagnostic classifier and inputs that must pass it; the SDK link-mode
# selection on static-only, shared-only and dual prefixes; the exact
# link-search assertion against a prefix-of-path, an outside directory and
# the real directory; a failing helper failing a row; a row stating the
# SDK's link mode to its builds with the opposite mode inherited; and, with
# real cargo runs, a build script's printed diagnostic and a package step's
# warning failing their gates beside clean runs that pass.
#
#   gate_controls.sh <sdk-prefix> <work-dir>
set -euo pipefail
# A command substitution fails with the first command that fails in it
# (bash 4.4 and later; older shells keep the helpers' explicit returns).
shopt -s inherit_errexit 2>/dev/null || true
HERE=$(cd "$(dirname "$0")/.." && pwd)
PREFIX=${1:?sdk prefix}; WORK=${2:?work dir}
# shellcheck source=sdk_paths.sh
. "$HERE/scripts/sdk_paths.sh"
# shellcheck source=cargo_step.sh
. "$HERE/scripts/cargo_step.sh"
rm -rf "$WORK"; mkdir -p "$WORK"
CLASSIFY="$HERE/scripts/diagnostics.sh"
failures=0
check() {   # check <label> <expected: pass|fail> <command...>
    local label=$1 expected=$2; shift 2
    local rc=0
    "$@" > "$WORK/$label.out" 2>&1 || rc=$?
    if { [ "$expected" = pass ] && [ "$rc" = 0 ]; } || { [ "$expected" = fail ] && [ "$rc" != 0 ]; }; then
        echo "ok   $label ($expected, rc=$rc)"
    else
        echo "FAIL $label (expected $expected, rc=$rc; see $WORK/$label.out)"
        failures=$((failures + 1))
    fi
}

# -- the classifier ------------------------------------------------------
# One registry compiler-message record, its primary span in the crate's own
# source: registry <crate> <version> <level> <code|-> <message> [<file> <line>]
registry() {
    local code='null' file=${6:-src/lib.rs} line=${7:-10}
    [ "$4" != - ] && code="{\"code\":\"$4\",\"explanation\":null}"
    printf '{"reason":"compiler-message","package_id":"registry+https://github.com/rust-lang/crates.io-index#%s@%s","target":{"kind":["lib"],"name":"%s"},"message":{"message":"%s","code":%s,"level":"%s","spans":[{"file_name":"/home/ci/.cargo/registry/src/index.crates.io-0000000000000000/%s-%s/%s","line_start":%s,"is_primary":true,"expansion":null}],"rendered":"%s: %s\\n"}}\n' \
        "$1" "$2" "$1" "$5" "$code" "$3" "$1" "$2" "$file" "$line" "$3" "$5"
}
local_record() {   # one path-package compiler-message record: local_record <name> <level> <code> <message>
    printf '{"reason":"compiler-message","package_id":"path+file:///work/%s#%s@0.1.0","target":{"kind":["lib"],"name":"%s"},"message":{"message":"%s","code":{"code":"%s","explanation":null},"level":"%s","spans":[{"file_name":"src/lib.rs","line_start":3,"is_primary":true,"expansion":null}],"rendered":"%s: %s\\n"}}\n' \
        "$1" "$1" "$1" "$4" "$3" "$2" "$2" "$4"
}
ALLOW=$WORK/allow
printf '# one exact entry\nshlex 2.0.1 src/lib.rs:10 unexpected_cfgs\n' > "$ALLOW"
# A clean build: the allowed registry warning at its location, cargo's
# roll-ups, a -vv echo line.
{ registry shlex 2.0.1 warning unexpected_cfgs "unexpected cfg condition name"; echo '[moq5-sys 0.1.0] cargo:rustc-link-search=native=/x'; } > "$WORK/clean.json"
printf 'warning: `shlex` (lib) generated 1 warning\n    Finished `dev` profile in 1.0s\n' > "$WORK/clean.err"
check classifier-clean pass bash "$CLASSIFY" "$WORK/clean.json" "$WORK/clean.err" "$WORK/clean.inv" "$ALLOW"
# A manifest warning: unlocated, cargo's own, this build's.
: > "$WORK/manifest.json"
printf 'warning: /work/Cargo.toml: unused manifest key: package.unexpected-key\n    Finished `dev` profile in 0.01s\n' > "$WORK/manifest.err"
check classifier-manifest-warning fail bash "$CLASSIFY" "$WORK/manifest.json" "$WORK/manifest.err" "$WORK/manifest.inv" "$ALLOW"
# A build-script warning of this build's own package.
: > "$WORK/build-script.json"
printf 'warning: moq5-sys@0.1.0: deliberate review warning from build.rs\n    Finished `dev` profile in 0.01s\n' > "$WORK/build-script.err"
check classifier-own-build-script-warning fail bash "$CLASSIFY" "$WORK/build-script.json" "$WORK/build-script.err" "$WORK/build-script.inv" "$ALLOW"
# A registry crate's build-script warning not in the allowlist.
registry shlex 2.0.1 warning unexpected_cfgs "unexpected cfg condition name" > "$WORK/dep-build-script.json"
printf 'warning: shlex@2.0.1: deliberate dependency build-script warning\n' > "$WORK/dep-build-script.err"
check classifier-dependency-build-script-warning fail bash "$CLASSIFY" "$WORK/dep-build-script.json" "$WORK/dep-build-script.err" "$WORK/dep-build-script.inv" "$ALLOW"
# Another lint at the allowed location.
registry shlex 2.0.1 warning deliberate_review_lint "deliberately unapproved new dependency diagnostic" > "$WORK/novel.json"
: > "$WORK/novel.err"
check classifier-novel-dependency-lint fail bash "$CLASSIFY" "$WORK/novel.json" "$WORK/novel.err" "$WORK/novel.inv" "$ALLOW"
# The allowed lint at another location of the same crate and version.
registry shlex 2.0.1 warning unexpected_cfgs "unexpected cfg condition name" src/lib.rs 11 > "$WORK/other-location.json"
: > "$WORK/other-location.err"
check classifier-allowed-lint-elsewhere fail bash "$CLASSIFY" "$WORK/other-location.json" "$WORK/other-location.err" "$WORK/other-location.inv" "$ALLOW"
# The allowed lint and location in another version of the crate.
registry shlex 2.0.2 warning unexpected_cfgs "unexpected cfg condition name" > "$WORK/other-version.json"
: > "$WORK/other-version.err"
check classifier-allowed-lint-other-version fail bash "$CLASSIFY" "$WORK/other-version.json" "$WORK/other-version.err" "$WORK/other-version.inv" "$ALLOW"
# A diagnostic of an allowed crate without a lint code, not allowed by text.
registry shlex 2.0.1 warning - "a codeless dependency diagnostic" > "$WORK/codeless.json"
: > "$WORK/codeless.err"
check classifier-codeless-dependency-diagnostic fail bash "$CLASSIFY" "$WORK/codeless.json" "$WORK/codeless.err" "$WORK/codeless.inv" "$ALLOW"
# This build's own warning.
local_record moq5 warning unused_variables "unused variable: x" > "$WORK/own.json"
: > "$WORK/own.err"
check classifier-own-warning fail bash "$CLASSIFY" "$WORK/own.json" "$WORK/own.err" "$WORK/own.inv" "$ALLOW"
# An error at exactly the allowed identity: never waivable.
registry shlex 2.0.1 error unexpected_cfgs "an error" > "$WORK/error.json"
printf 'error: could not compile `shlex` (lib) due to 1 previous error\n' > "$WORK/error.err"
check classifier-dependency-error fail bash "$CLASSIFY" "$WORK/error.json" "$WORK/error.err" "$WORK/error.inv" "$ALLOW"
# An unlocated line from another tool on stderr.
: > "$WORK/tool.json"
printf 'warning: something a tool printed\n' > "$WORK/tool.err"
check classifier-unlocated-tool-warning fail bash "$CLASSIFY" "$WORK/tool.json" "$WORK/tool.err" "$WORK/tool.inv" "$ALLOW"
# A crate-wide or lint-wide entry cannot be written: the allowlist is refused.
printf 'shlex 2.0.1 unexpected_cfgs\n' > "$WORK/allow-wide"
check classifier-refuses-location-free-entry fail bash "$CLASSIFY" "$WORK/clean.json" "$WORK/clean.err" "$WORK/wide.inv" "$WORK/allow-wide"
grep -q 'no crate-wide or location-free entry' "$WORK/classifier-refuses-location-free-entry.out" &&
    echo "ok   classifier-refuses-location-free-entry-with-the-reason" ||
    { echo "FAIL classifier-refuses-location-free-entry-with-the-reason"; failures=$((failures + 1)); }
# The inventory records the allowed diagnostic of the clean case with its
# location, and the summary counts it as an accepted upstream warning.
grep -q -x -F "$(printf '1\tallowed\tregistry\tshlex\t2.0.1\tsrc/lib.rs:10\tunexpected_cfgs')" "$WORK/clean.inv" &&
    grep -q '1 accepted upstream warning' "$WORK/classifier-clean.out" &&
    echo "ok   classifier-inventory-lists-allowed" ||
    { echo "FAIL classifier-inventory-lists-allowed"; failures=$((failures + 1)); }

# Cargo's roll-ups of test builds are summaries, not diagnostics; a linker
# message beside them is one.
printf '%s\n' 'warning: `moq5-fake-tests` (lib test) generated 1 warning (1 duplicate)' \
    'warning: `moq5-fake-tests` (test "layout") generated 1 warning (1 duplicate)' \
    'warning: `moq5-fake-tests` (build script) generated 1 warning' > "$WORK/rollups.err"
: > "$WORK/rollups.json"
check classifier-test-rollups-pass pass bash "$CLASSIFY" "$WORK/rollups.json" "$WORK/rollups.err" "$WORK/rollups.inv" "$ALLOW"
printf '%s\n' 'warning: linker stderr: /usr/bin/ld: libclang_rt.asan-aarch64.so: the use of `tmpnam_r'"'"' is dangerous' >> "$WORK/rollups.err"
check classifier-linker-message-fails fail bash "$CLASSIFY" "$WORK/rollups.json" "$WORK/rollups.err" "$WORK/rollups-linker.inv" "$ALLOW"

# What a build script prints, as cargo -vv echoes it on either stream.
: > "$WORK/no-records.json"
printf '%s\n' "[moq5-sys 0.1.0] warning: unknown warning option '-Wx' [-Wunknown-warning-option]" \
    "[moq5-sys 0.1.0] clang diag: warning: unknown warning option '-Wx' [-Wunknown-warning-option]" > "$WORK/echo-clang.err"
check classifier-build-output-clang-warning fail bash "$CLASSIFY" "$WORK/no-records.json" "$WORK/echo-clang.err" "$WORK/echo-clang.inv" "$ALLOW"
printf '%s\n' '[moq5-sys 0.1.0] clang diag: /x/include/moq/types.h:3:1: warning: a located diagnostic [-Wfoo]' > "$WORK/echo-located.err"
check classifier-build-output-located-diagnostic fail bash "$CLASSIFY" "$WORK/no-records.json" "$WORK/echo-located.err" "$WORK/echo-located.inv" "$ALLOW"
printf '%s\n' '[moq5-sys 0.1.0] warning: a diagnostic printed to stdout' > "$WORK/echo-stdout.json"
: > "$WORK/echo-stdout.err"
check classifier-build-output-on-stdout fail bash "$CLASSIFY" "$WORK/echo-stdout.json" "$WORK/echo-stdout.err" "$WORK/echo-stdout.inv" "$ALLOW"
printf '%s\n' '[libc 0.2.191] cargo:rustc-check-cfg=cfg(libc_deny_warnings)' '[moq5-sys 0.1.0] cargo:rerun-if-env-changed=X' > "$WORK/echo-directives.json"
: > "$WORK/echo-directives.err"
check classifier-build-output-directives-pass pass bash "$CLASSIFY" "$WORK/echo-directives.json" "$WORK/echo-directives.err" "$WORK/echo-directives.inv" "$ALLOW"

# -- the link mode -------------------------------------------------------
mode_of() {   # mode_of <label> <expected> <library files...>: a prefix with those files
    local label=$1 expected=$2; shift 2
    local prefix=$WORK/prefix-$label
    mkdir -p "$prefix/lib/pkgconfig"; : > "$prefix/lib/pkgconfig/libmoq-service.pc"
    local f; for f in "$@"; do : > "$prefix/lib/$f"; done
    local got; got=$(moq5_sdk_link_mode "$prefix" 2>/dev/null || echo error)
    if [ "$got" = "$expected" ]; then echo "ok   link-mode-$label ($got)"; else
        echo "FAIL link-mode-$label (expected $expected, got $got)"; failures=$((failures + 1)); fi
}
mode_of static-only static libmoq-service.a
mode_of shared-dylib shared libmoq-service.dylib
mode_of shared-so shared libmoq-service.so
mode_of dual shared libmoq-service.a libmoq-service.dylib
mode_of none error
# Exporting clears the opposite variable and names the mode.
( export LIBMOQ_SERVICE_DYNAMIC=1; moq5_sdk_export_link_mode "$WORK/prefix-static-only"
  [ "${LIBMOQ_SERVICE_STATIC:-}" = 1 ] && [ -z "${LIBMOQ_SERVICE_DYNAMIC:-}" ] &&
    [ "$MOQ5_SDK_LINK_MODE" = static ] ) &&
    echo "ok   link-mode-export-clears-opposite" ||
    { echo "FAIL link-mode-export-clears-opposite"; failures=$((failures + 1)); }

# -- the exact link-search assertion, against the real SDK ------------------
libdir=$(moq5_sdk_libdir "$PREFIX")
printf '[moq5-sys 0.1.0] cargo:rustc-link-search=native=%s\n' "$libdir" > "$WORK/search-exact.log"
check link-search-exact pass moq5_sdk_assert_link_search "$WORK/search-exact.log" "$PREFIX"
printf '[moq5-sys 0.1.0] cargo:rustc-link-search=native=%s\n' "$(dirname "$libdir")" > "$WORK/search-parent.log"
check link-search-prefix-of-path fail moq5_sdk_assert_link_search "$WORK/search-parent.log" "$PREFIX"
printf '[moq5-sys 0.1.0] cargo:rustc-link-search=native=%s\n[moq5-sys 0.1.0] cargo:rustc-link-search=native=%s\n' "$libdir" "$WORK" > "$WORK/search-outside.log"
check link-search-outside-closure fail moq5_sdk_assert_link_search "$WORK/search-outside.log" "$PREFIX"
: > "$WORK/search-none.log"
check link-search-no-line fail moq5_sdk_assert_link_search "$WORK/search-none.log" "$PREFIX"

# -- a failing helper fails a row ------------------------------------------
check row-fails-on-missing-sdk fail env MOQ5_RUST_SDK=$WORK/no-such-prefix MOQ5_RUST_WORK=$WORK/row-work \
    bash "$HERE/scripts/ctest_row.sh" fmt
grep -q 'no libmoq-service.pc under' "$WORK/row-fails-on-missing-sdk.out" && echo "ok   row-fails-with-the-helper's-reason" ||
    { echo "FAIL row-fails-with-the-helper's-reason"; failures=$((failures + 1)); }

# -- the row states the SDK's link mode to the builds it runs ---------------
# ctest_row.sh exports the mode in its own shell, which every cargo run of
# the row inherits, and clears the opposite variable even when it was
# inherited: the call site, not only the helper.
row_mode=$(moq5_sdk_link_mode "$PREFIX")
case $row_mode in
    static) inherited=LIBMOQ_SERVICE_DYNAMIC=1; want="(LIBMOQ_SERVICE_STATIC=1, LIBMOQ_SERVICE_DYNAMIC=unset)" ;;
    shared) inherited=LIBMOQ_SERVICE_STATIC=1; want="(LIBMOQ_SERVICE_STATIC=unset, LIBMOQ_SERVICE_DYNAMIC=1)" ;;
esac
check row-runs-with-the-opposite-mode-inherited pass env "$inherited" "MOQ5_RUST_SDK=$PREFIX" \
    "MOQ5_RUST_WORK=$WORK/row-mode-work" bash "$HERE/scripts/ctest_row.sh" fmt
grep -q -F "link mode $row_mode $want" "$WORK/row-runs-with-the-opposite-mode-inherited.out" &&
    echo "ok   row-states-the-link-mode ($row_mode $want, $inherited inherited)" ||
    { echo "FAIL row-states-the-link-mode (expected: link mode $row_mode $want)"; failures=$((failures + 1)); }

# -- real cargo runs through the same gates ------------------------------
# moq5-sys checked against the SDK with cargo -vv, its build script rerun
# each time: clean, and with an unknown clang warning option, which libclang
# and bindgen print from the build script and cargo exits 0 on.
REAL_ALLOW=$HERE/scripts/dependency_diagnostics.allow
sys_target=$(dirname "$WORK")/gate-controls-sys-target
sys_check() {   # sys_check <name> [an extra clang argument]
    local name=$1 extra=${BINDGEN_EXTRA_CLANG_ARGS:-}
    if [ -n "${2:-}" ]; then extra="${extra:+$extra }$2"; fi
    CARGO_TARGET_DIR=$sys_target "${CARGO:-cargo}" clean --manifest-path "$HERE/Cargo.toml" -p moq5-sys \
        > /dev/null 2>&1
    BINDGEN_EXTRA_CLANG_ARGS=$extra MOQ_PREFIX=$PREFIX CARGO_TARGET_DIR=$sys_target \
        "${CARGO:-cargo}" check --manifest-path "$HERE/Cargo.toml" -p moq5-sys --locked -vv \
        --message-format=json > "$WORK/$name.json" 2> "$WORK/$name.err"
}
sys_check sys-clean
check build-script-clean-passes pass bash "$CLASSIFY" "$WORK/sys-clean.json" "$WORK/sys-clean.err" "$WORK/sys-clean.inv" "$REAL_ALLOW"
sys_check sys-clang-warning -Wmoq5-gate-control-unknown-option
check build-script-clang-warning-fails fail bash "$CLASSIFY" "$WORK/sys-clang-warning.json" "$WORK/sys-clang-warning.err" "$WORK/sys-clang-warning.inv" "$REAL_ALLOW"
grep -q -F "build-output:warning: unknown warning option '-Wmoq5-gate-control-unknown-option'" "$WORK/sys-clang-warning.inv" &&
    echo "ok   build-script-clang-warning-is-inventoried-as-own" ||
    { echo "FAIL build-script-clang-warning-is-inventoried-as-own"; failures=$((failures + 1)); }
# The package recipe's step (scripts/cargo_step.sh) on a package without
# metadata, whose cargo package warning cargo exits 0 on, and on the real
# crate.
probe=$WORK/package-probe
mkdir -p "$probe/src"
printf '[package]\nname = "moq5-package-warning-probe"\nversion = "0.0.0"\nedition = "2021"\n\n[workspace]\n' \
    > "$probe/Cargo.toml"
echo 'pub fn probe() {}' > "$probe/src/lib.rs"
check package-step-refuses-a-package-warning fail moq5_cargo_step "$WORK" probe-list "$WORK/probe-list.out" \
    env CARGO_TARGET_DIR="$probe/target" "${CARGO:-cargo}" package --manifest-path "$probe/Cargo.toml" --allow-dirty --list
grep -q 'manifest has no description' "$WORK/probe-list.diagnostics.txt" &&
    echo "ok   package-step-inventories-the-warning" ||
    { echo "FAIL package-step-inventories-the-warning"; failures=$((failures + 1)); }
check package-step-passes-the-real-crate pass moq5_cargo_step "$WORK" sys-list "$WORK/sys-list.out" \
    env CARGO_TARGET_DIR="$WORK/package-target" "${CARGO:-cargo}" package --manifest-path "$HERE/Cargo.toml" \
    -p moq5-sys --allow-dirty --list

[ "$failures" = 0 ] || { echo "gate_controls.sh: $failures control(s) failed" >&2; exit 1; }
echo "gate_controls.sh: OK ($WORK)"
