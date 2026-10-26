#!/bin/sh
# Non-vacuity evidence for the 16 KiB ELF alignment gate.
#
# Every case is built by copying the REAL packaged images into a task-owned
# scratch stage and patching their program headers there. The durable stage is
# never written to, so this suite needs no mutation lock: it only reads.
#
# Patching is the only honest way to build case 2. A relink cannot produce an
# image whose FIRST LOAD is correctly aligned while a LATER one is not, because
# the linker gives every LOAD the same page size -- so the byte pattern is
# written deliberately.
#
# Each case also records what the SUPERSEDED treatment did with the same bytes.
# The topology checker used to emit only a NOTE, read from the FIRST LOAD row;
# showing that each fixture would have passed under it is what makes the new
# gate a change rather than a restatement.
set -u
LC_ALL=C; export LC_ALL

here=$(cd "$(dirname "$0")" && pwd) || { echo "cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd)        || { echo "cannot resolve bindings/kotlin" >&2; exit 2; }
. "$here/lib-declaration.sh"
. "$here/lib-elf.sh"

for t in awk sed grep cp rm mkdir dd od printf mktemp find chmod; do
    command -v "$t" >/dev/null 2>&1 || { echo "selftest-jni-alignment: required tool not found: $t" >&2; exit 2; }
done

dep="$root/.native"
STAGE="$dep/_topology"
GATE="$here/check-jni-alignment.sh"
ABIS="arm64-v8a x86_64"
[ -r "$GATE" ] || { echo "selftest-jni-alignment: no gate at $GATE" >&2; exit 2; }
[ -d "$STAGE" ] || { echo "selftest-jni-alignment: no durable stage at $STAGE -- run build-jni-topology.sh first" >&2; exit 2; }

work=$(mktemp -d "${TMPDIR:-/tmp}/moq5-align-selftest-XXXXXXXX") \
    || { echo "selftest-jni-alignment: cannot create a task-owned scratch" >&2; exit 2; }
# shellcheck disable=SC2329  # invoked from the traps below
cleanup() { rm -rf -- "$work"; }
trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

kills=0; misses=0; cases=0
kill_() { cases=$((cases + 1)); kills=$((kills + 1)); echo "KILL[$1]: $2"; }
miss_() { cases=$((cases + 1)); misses=$((misses + 1)); echo "MISS[$1]: $2"; }

# The pinned readelf, resolved exactly as the gate resolves it.
_p="$dep/arm64-v8a/provenance.properties"
_n=$(props_count "$_p" ndk.cc) || { echo "cannot read the pinned toolchain" >&2; exit 2; }
[ "$_n" = "1" ] || { echo "the Phase 0.2 provenance declares no single ndk.cc" >&2; exit 2; }
_cc=$(props_value "$_p" ndk.cc) || { echo "cannot read the pinned toolchain" >&2; exit 2; }
READELF="$(dirname "$_cc")/llvm-readelf"
[ -x "$READELF" ] || { echo "the pinned readelf is not executable: $READELF" >&2; exit 2; }

# mkfix <name> -- a scratch stage holding copies of the real images; echoes it
mkfix() {
    _fx="$work/$1"
    mkdir -p "$_fx" || return 1
    for _a in $ABIS; do
        mkdir -p "$_fx/$_a/package" "$_fx/$_a/diagnostic" || return 1
        for _i in package diagnostic; do
            cp "$STAGE/$_a/$_i/libmoq5_jni.so" "$_fx/$_a/$_i/libmoq5_jni.so" || return 1
        done
    done
    printf '%s' "$_fx"
}

aligns_of() { "$READELF" --wide -l "$1" 2>/dev/null | awk '$1 == "LOAD" { printf "%s ", $NF }'; }

# what the SUPERSEDED treatment saw: the first LOAD row only, reported as a NOTE
superseded_verdict() {
    _sv=$("$READELF" --wide -l "$1" 2>/dev/null | awk '$1 == "LOAD" { print $NF; exit }')
    [ -n "$_sv" ] || { printf 'no observation'; return; }
    printf 'NOTE first LOAD %s (never a failure)' "$_sv"
}

# expect_gate_fail <case> <fixture> <substring>
expect_gate_fail() {
    _c=$1; _fx=$2; _sub=$3
    if sh "$GATE" --readelf "$READELF" "$_fx" > "$work/out" 2>&1; then
        miss_ "$_c" "the gate passed an image it must reject"
        return
    fi
    if grep -q -- "$_sub" "$work/out"; then
        kill_ "$_c" "$(grep -m1 -- "$_sub" "$work/out" | cut -c1-150)"
    else
        miss_ "$_c" "the gate failed, but not for the expected reason: $(grep -m1 '^FAIL' "$work/out" | cut -c1-150)"
    fi
}

echo "== the unmodified images pass, so every mutant below is attributable =="
base=$(mkfix base) || { echo "cannot build the baseline fixture" >&2; exit 2; }
if sh "$GATE" --readelf "$READELF" "$base" > "$work/base.out" 2>&1; then
    echo "baseline: $(grep -c '^PASS' "$work/base.out") image(s) pass; arm64-v8a package aligns [$(aligns_of "$base/arm64-v8a/package/libmoq5_jni.so")]"
else
    echo "selftest-jni-alignment: the unmodified images do not pass the gate:" >&2
    grep '^FAIL' "$work/base.out" >&2
    exit 1
fi

echo "== RED 1: every LOAD at 4 KiB =="
f1=$(mkfix red1) || { echo "cannot build fixture 1" >&2; exit 2; }
_t="$f1/arm64-v8a/package/libmoq5_jni.so"
for _i in $(elf_load_indices "$_t"); do
    elf_set_ph_align "$_t" "$_i" 4096 || { echo "cannot patch fixture 1" >&2; exit 2; }
done
echo "   aligns now [$(aligns_of "$_t")]; superseded treatment would have said: $(superseded_verdict "$_t")"
expect_gate_fail red1-all-4k "$f1" "below the required 2\*\*14"

echo "== RED 2: the FIRST LOAD is correct and a LATER one is not =="
f2=$(mkfix red2) || { echo "cannot build fixture 2" >&2; exit 2; }
_t="$f2/arm64-v8a/package/libmoq5_jni.so"
_second=$(elf_load_indices "$_t" | sed -n '2p')
[ -n "$_second" ] || { echo "the image has fewer than two LOAD segments; fixture 2 is not constructible" >&2; exit 2; }
elf_set_ph_align "$_t" "$_second" 4096 || { echo "cannot patch fixture 2" >&2; exit 2; }
_first=$(aligns_of "$_t" | awk '{ print $1 }')
[ "$_first" = "0x4000" ] || { echo "fixture 2 did not preserve a correct first LOAD (got $_first)" >&2; exit 2; }
echo "   aligns now [$(aligns_of "$_t")]; superseded treatment would have said: $(superseded_verdict "$_t")"
expect_gate_fail red2-later-load-4k "$f2" "below the required 2\*\*14"

echo "== RED 3: the program-header producer fails =="
f3=$(mkfix red3) || { echo "cannot build fixture 3" >&2; exit 2; }
# A producer that cannot run must be a named failure, never zero LOAD rows
# silently satisfying the rule.
cat > "$work/broken-readelf" <<'BROKEN'
#!/bin/sh
echo "llvm-readelf: error: unable to read program headers" >&2
exit 1
BROKEN
chmod +x "$work/broken-readelf" || { echo "cannot prepare the broken producer" >&2; exit 2; }
if sh "$GATE" --readelf "$work/broken-readelf" "$f3" > "$work/out3" 2>&1; then
    miss_ red3-producer-failure "a failing program-header producer was treated as aligned"
elif grep -q 'producer failed' "$work/out3" && grep -q 'UNMEASURED' "$work/out3"; then
    kill_ red3-producer-failure "$(grep -m1 'producer failed' "$work/out3" | cut -c1-150)"
else
    miss_ red3-producer-failure "failed, but not as a producer failure: $(grep -m1 '^FAIL' "$work/out3" | cut -c1-150)"
fi

echo "== RED 4: an image reporting no LOAD segments is not vacuously aligned =="
f4=$(mkfix red4) || { echo "cannot build fixture 4" >&2; exit 2; }
cat > "$work/empty-readelf" <<'EMPTY'
#!/bin/sh
echo "Program Headers:"
echo "  Type           Offset   VirtAddr           PhysAddr           FileSiz  MemSiz   Flg Align"
exit 0
EMPTY
chmod +x "$work/empty-readelf" || { echo "cannot prepare the empty producer" >&2; exit 2; }
if sh "$GATE" --readelf "$work/empty-readelf" "$f4" > "$work/out4" 2>&1; then
    miss_ red4-no-load-rows "an image with no LOAD rows passed the gate"
elif grep -q 'no LOAD segments at all' "$work/out4"; then
    kill_ red4-no-load-rows "$(grep -m1 'no LOAD segments at all' "$work/out4" | cut -c1-150)"
else
    miss_ red4-no-load-rows "failed, but not on the missing rows: $(grep -m1 '^FAIL' "$work/out4" | cut -c1-150)"
fi

echo "== RED 5: the DIAGNOSTIC twin is gated too, not only the package =="
f5=$(mkfix red5) || { echo "cannot build fixture 5" >&2; exit 2; }
_t="$f5/x86_64/diagnostic/libmoq5_jni.so"
for _i in $(elf_load_indices "$_t"); do
    elf_set_ph_align "$_t" "$_i" 4096 || { echo "cannot patch fixture 5" >&2; exit 2; }
done
expect_gate_fail red5-diagnostic-image "$f5" "x86_64.diagnostic.alignment"

echo "== RED 6: a missing image is a failure, not an absent measurement =="
f6=$(mkfix red6) || { echo "cannot build fixture 6" >&2; exit 2; }
rm -f "$f6/x86_64/package/libmoq5_jni.so" || { echo "cannot remove the image" >&2; exit 2; }
expect_gate_fail red6-missing-image "$f6" "no such image"

printf '\nalignment self-test: %s kill(s), %s miss(es), %s case(s)\n' "$kills" "$misses" "$cases"
[ "$misses" -eq 0 ] || exit 1
exit 0
