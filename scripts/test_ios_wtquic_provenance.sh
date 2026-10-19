#!/usr/bin/env bash
#
# The iOS build's wtquic provenance rule, checked with
# MOQ_IOS_PREFLIGHT_ONLY=1 (nothing compiles). This verifies the WT
# checkout's provenance only -- not the other dependencies, the OpenSSL
# prefix, or linkability:
#   - a clean checkout at the exact pin, found through WTQUIC_DEPS_DIR, is
#     PINNED, with no warning line;
#   - the same checkout with an untracked file is refused as DIRTY, with no
#     warning line;
#   - a checkout at another commit is refused as a pin mismatch, with no
#     warning line;
#   - an explicit MOQ_WTQUIC_SOURCE_DIR -- even the clean pin itself -- stays
#     an UNVERIFIED override, printing exactly its four WARNING lines and no
#     other warning.
# Warning lines are matched case-insensitively. The classifier itself has
# controls: an output carrying one unexpected warning line fails each kind.
# MOQ_IOS_JOBS=1 keeps the preflight independent of the host CPU query.
#
# Args: 1 a clean checkout at the pin (scripts/setup_wtquic_deps.sh with
#       WTQ_FETCH_ONLY=1), 2 picoquic source dir, 3 picotls source dir.
set -euo pipefail
PIN_SRC="$1"; PQ="$2"; PT="$3"
here=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
fails=0

# The override's exact warning set (the script's four lines, pin elided).
override_warnings() {
    printf '%s\n' \
        '[build_ios_xcframework] WARNING: MOQ_WTQUIC_SOURCE_DIR override in use -- the wtquic' \
        '[build_ios_xcframework] WARNING: sources are NOT verified against the dependency pin' \
        '[build_ios_xcframework] WARNING: (PIN); the artifact is a local development' \
        '[build_ios_xcframework] WARNING: build, not a pinned release build.'
}

# classify <expect: pinned|unverified|refused> <refusal text|-> <rc> <output>
# Returns 0 when the run is exactly the expected outcome with exactly the
# expected warning lines; prints the reason otherwise.
classify() {
    local expect=$1 reason=$2 rc=$3 out=$4 got warnings want_w
    if [ "$rc" -ne 0 ]; then got=refused
    elif grep -q "preflight: wtquic PINNED" <<<"$out"; then got=pinned
    elif grep -q "preflight: wtquic UNVERIFIED" <<<"$out"; then got=unverified
    else got=unknown
    fi
    [ "$got" = "$expect" ] || { echo "outcome $got, want $expect"; return 1; }
    warnings=$(grep -i warning <<<"$out" || true)
    case $expect in
        unverified)
            want_w=$(override_warnings)
            # Compare with the pin elided from the third line.
            local seen
            seen=$(sed -E 's/\(([0-9a-f]{40})\)/(PIN)/' <<<"$warnings")
            [ "$seen" = "$want_w" ] || { echo "warning lines differ from the override's exact set"; return 1; }
            ;;
        *)
            [ -z "$warnings" ] || { echo "unexpected warning line(s): $warnings"; return 1; }
            ;;
    esac
    if [ "$expect" = refused ]; then
        grep -q "$reason" <<<"$out" || { echo "refused, but not for: $reason"; return 1; }
    fi
    return 0
}

run() {   # name expect reason env...
    local name=$1 expect=$2 reason=$3; shift 3
    local out rc=0 why
    out=$(env -u MOQ_WTQUIC_SOURCE_DIR -u WTQUIC_DEPS_DIR \
              MOQ_IOS_PREFLIGHT_ONLY=1 MOQ_IOS_JOBS=1 \
              MOQ_PICOQUIC_SOURCE_DIR="$PQ" MOQ_PICOTLS_SOURCE_DIR="$PT" "$@" \
              bash "$here/build_ios_xcframework.sh" 2>&1) || rc=$?
    if why=$(classify "$expect" "$reason" "$rc" "$out"); then
        echo "PASS: $name ($expect, rc=$rc, warning_lines_ci=$(grep -ci warning <<<"$out" || true))"
    else
        echo "FAIL: $name: $why (rc=$rc)"
        printf '%s\n' "$out" | tail -6
        fails=$((fails + 1))
    fi
}

# The classifier's own controls: one unexpected warning line fails each kind.
selftest() {   # name expect reason rc output
    if classify "$2" "$3" "$4" "$5" > /dev/null; then
        echo "FAIL: $1: an unexpected warning line was accepted"; fails=$((fails + 1))
    else
        echo "PASS: $1 (unexpected warning rejected)"
    fi
}
pin=$(git -C "$PIN_SRC" rev-parse HEAD)
selftest classify_pinned_extra_warning pinned - 0 \
    "$(printf '%s\n' "[build_ios_xcframework] preflight: wtquic PINNED $pin at /x" 'cmake: Warning: something else')"
selftest classify_refused_extra_warning refused "is DIRTY" 1 \
    "$(printf '%s\n' '[build_ios_xcframework] ERROR: wtquic pinned checkout is DIRTY' 'warning: unrelated')"
selftest classify_override_extra_warning unverified - 0 \
    "$(override_warnings | sed "s/(PIN)/($pin)/"; printf '%s\n' 'WARNING: an extra one' "[build_ios_xcframework] preflight: wtquic UNVERIFIED override at /x")"

mkdir -p "$work/clean" "$work/dirty" "$work/other/wtquic"
cp -R "$PIN_SRC" "$work/clean/wtquic"
cp -R "$PIN_SRC" "$work/dirty/wtquic"
touch "$work/dirty/wtquic/untracked-file"
git -C "$work/other/wtquic" init -q
git -C "$work/other/wtquic" -c user.email=t@example.invalid -c user.name=t \
    commit -q --allow-empty -m other
run pinned_clean     pinned     -              WTQUIC_DEPS_DIR="$work/clean"
run pinned_dirty     refused    "is DIRTY"     WTQUIC_DEPS_DIR="$work/dirty"
run pinned_mismatch  refused    "pin mismatch" WTQUIC_DEPS_DIR="$work/other"
run override_clean   unverified -              MOQ_WTQUIC_SOURCE_DIR="$work/clean/wtquic"
if [ "$fails" -eq 0 ]; then echo "PASS: ios wtquic provenance"; else echo "FAIL: $fails"; exit 1; fi
