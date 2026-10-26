#!/bin/sh
# Self-test for check-jni-endpoint-preflight.sh.
#
# It drives the SHIPPING gate over synthetic trees, so what is proven is the
# gate itself and not a second copy of its logic. Two properties make it a
# ledger rather than a pile of cases:
#
#  - the expected case names are DECLARED independently below, each must run
#    exactly once, and a case that disappears is a failure rather than a
#    smaller green count;
#  - every case declares its COMPLETE expected FAIL inventory with
#    multiplicity, so a case cannot be satisfied by a gate that failed for an
#    unrelated reason as well.
#
# Its origin: an earlier gate suppressed `nm` stderr and let `grep -c` report
# zero, so an `nm` that could not run read as "this image links no LibMoQ" and
# the whole gate exited 0 with every row PASS. The same shape was then found in
# the hash path -- a hasher printing a valid digest and exiting 7 -- because the
# status observed was the parser's. Both are killed here permanently.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
case $0 in
    */*) here=$(cd "${0%/*}" && pwd) ;;
    *)   here=$(pwd) ;;
esac
GATE="$here/check-jni-endpoint-preflight.sh"
[ -x "$GATE" ] || { echo "$_self: gate not executable: $GATE" >&2; exit 2; }
for t in cc sed grep awk sort comm; do
    command -v "$t" >/dev/null 2>&1 || { echo "$_self: required tool not found: $t" >&2; exit 2; }
done
[ -r "$here/lib-hash.sh" ] || { echo "$_self: missing lib-hash.sh" >&2; exit 2; }
# shellcheck source=lib-hash.sh
. "$here/lib-hash.sh"

W=$(mktemp -d "${TMPDIR:-/tmp}/moq5-eppf-selftest-XXXXXXXX") || exit 2
# shellcheck disable=SC2329
cleanup() { rm -rf -- "$W"; }
trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

fails=0
ran=""
ok()  { ran="$ran $1"; printf 'PASS[selftest.%s]: %s\n' "$1" "$2"; }
bad() { ran="$ran $1"; fails=$((fails + 1)); printf 'FAIL[selftest.%s]: %s\n' "$1" "$2"; }

# ---- the declared ledger ----------------------------------------------------
EXPECTED_CASES='complete-real
complete-synthetic-with-extra-row
authority-endpoint-spelled-row
authority-plain-spelled-row
authority-descriptor-drift
broken-host-nm
noisy-host-nm
broken-android-nm
core-only-host
undefined-reference-host
host-duplicate-symbol
android-duplicate-symbol
host-foreign-receipt
host-stale-source
host-no-receipt
host-receipt-duplicate-key
host-receipt-extra-key
host-receipt-lookalike-key
host-receipt-missing-image-row
android-missing-member
android-one-abi
android-stale-image
android-no-digest
android-duplicate-digest-key
android-abi-mismatch
android-toolchain-derived
android-toolchain-missing-nm
android-ndk-version-mismatch
hasher-valid-then-nonzero
hasher-zero-with-diagnostic
hasher-malformed-output
hasher-absent'

BRIDGE_IMAGES='bridge_product moq5_jni registry_roundtrip bridge_compatible
bridge_epoch_high bridge_epoch_low bridge_features_absent bridge_features_extra
bridge_features_wide bridge_omit_one bridge_bad_descriptor
bridge_registration_fails trust_delegate'

EP='moq_endpoint_connect moq_endpoint_get_snapshot moq_endpoint_wait
moq_endpoint_stop moq_endpoint_wake moq_endpoint_destroy'

# ---- fixture helpers --------------------------------------------------------
# A synthetic nm: prints the prepared table beside the image it is asked about.
cat > "$W/nm-synth" <<'EOS'
#!/bin/sh
[ -f "$1.syms" ] || { echo "no synthetic table for $1" >&2; exit 3; }
cat "$1.syms"
EOS
chmod +x "$W/nm-synth"
printf '#!/bin/sh\nexit 7\n' > "$W/nm-broken"; chmod +x "$W/nm-broken"
printf '#!/bin/sh\necho "warning: degraded" >&2\nexit 0\n' > "$W/nm-noisy"; chmod +x "$W/nm-noisy"

# syms_defined <file> <name...>
syms_defined() {
    _f=$1; shift
    : > "$_f"
    _a=1000
    for _s in "$@"; do
        printf '%016x T %s\n' "$_a" "$_s" >> "$_f"
        _a=$((_a + 16))
    done
}

write_abi_header() {
    # $1 dir, $2 extra row spec ("" | "name|descriptor") , $3 drift(0|1)
    _h="$1/jni/moq5_abi.h"
    {
        printf '#define MOQ5_ABI_METHODS(X)   \\\n'
        printf '    X(nativeAbiEpoch, "()I")  \\\n'
        if [ "${3:-0}" -eq 1 ]; then
            printf '    X(nativeFeatureBits, "()I") \\\n'
        else
            printf '    X(nativeFeatureBits, "()J") \\\n'
        fi
        if [ -n "${2:-}" ]; then
            printf '    X(nativeLibmoqVersion, "()I") \\\n'
            printf '    X(%s, "%s")\n' "${2%%|*}" "${2##*|}"
        else
            printf '    X(nativeLibmoqVersion, "()I")\n'
        fi
        printf '\n'
    } > "$_h"
}

write_host_receipt() {
    _r=$1
    _p="$_r/.native/_bridge/bridge.provenance"
    {
        printf 'producer=build-bridge-testdsos.sh\n'
        printf 'cc=/usr/bin/cc\n'
        printf 'jni.include=/nonexistent/include\n'
        printf 'openssl.prefix=/nonexistent/openssl\n'
        printf 'source.moq5_jni.c.sha256=%s\n' "$(moq_hash_file "$_r/jni/moq5_jni.c" "$W")"
    } > "$_p" || return 1
    for _i in $BRIDGE_IMAGES; do
        _f="$_r/.native/_bridge/lib$_i.dylib"
        [ -f "$_f" ] || : > "$_f"
        printf 'image.lib%s.dylib.sha256=%s\n' "$_i" "$(moq_hash_file "$_f" "$W")" >> "$_p" \
            || return 1
    done
    return 0
}

# make_tree <dir> <extra-row|""> [real|synth]
make_tree() {
    _r=$1; _extra=${2:-}; _kind=${3:-synth}
    mkdir -p "$_r/jni" "$_r/.native/_bridge" || return 1
    printf 'int moq5_stub(void) { return 0; }\n' > "$_r/jni/moq5_jni.c" || return 1
    write_abi_header "$_r" "$_extra" 0 || return 1

    if [ "$_kind" = real ]; then
        : > "$W/real.c"
        for _s in $EP; do printf 'int %s(void) { return 0; }\n' "$_s" >> "$W/real.c"; done
        cc -shared -fPIC -Wall -Wextra -Wpedantic -Werror -std=c11 \
           -o "$_r/.native/_bridge/libmoq5_jni.dylib" "$W/real.c" 2> "$W/cc.err" \
            || { echo "$_self: fixture compile failed: $(sed -n '1p' "$W/cc.err")" >&2; return 1; }
        [ -s "$W/cc.err" ] && {
            echo "$_self: fixture compile emitted a diagnostic: $(sed -n '1p' "$W/cc.err")" >&2
            return 1
        }
    else
        printf 'synthetic-host-image\n' > "$_r/.native/_bridge/libmoq5_jni.dylib"
        # shellcheck disable=SC2086
        syms_defined "$_r/.native/_bridge/libmoq5_jni.dylib.syms" $EP
    fi
    write_host_receipt "$_r" || return 1

    for _abi in arm64-v8a x86_64; do
        _d="$_r/.native/_topology/$_abi"
        mkdir -p "$_d/diagnostic" || return 1
        printf 'synthetic-%s-image\n' "$_abi" > "$_d/diagnostic/libmoq5_jni.so"
        # shellcheck disable=SC2086
        syms_defined "$_d/diagnostic/libmoq5_jni.so.syms" $EP
        {
            printf 'abi=%s\n' "$_abi"
            printf 'ndk.version=28.2.13676358\n'
            printf 'ndk.cc=/fake/ndk/28.2.13676358/toolchains/llvm/prebuilt/x/bin/clang\n'
            printf 'diagnostic.sha256=%s\n' \
                "$(moq_hash_file "$_d/diagnostic/libmoq5_jni.so" "$W")"
        } > "$_d/topology.provenance" || return 1
    done
    return 0
}

# ---- the runner -------------------------------------------------------------
# run_case <name> <dir> <expected-exit> <expected-FAIL-rows...> <-- description last
run_case() {
    _n=$1; _d=$2; _x=$3; _want=$4; _desc=$5
    MOQ5_EP_PREFLIGHT_ROOT="$_d" \
    MOQ5_EP_PREFLIGHT_ANDROID_NM="${CASE_ANM-$W/nm-synth}" \
    MOQ5_EP_PREFLIGHT_HOST_NM="${CASE_HNM-$W/nm-synth}" \
    PATH="${CASE_PATH-$PATH}" \
        "$GATE" > "$W/out.$_n" 2>&1
    _got=$?
    printf '%s\n' "$_want" | tr ' ' '\n' | grep -v '^$' | sort > "$W/want.$_n"
    sed -n 's/^FAIL\[\([^]]*\)\].*/\1/p' "$W/out.$_n" | sort > "$W/have.$_n"
    if [ "$_got" -ne "$_x" ]; then
        bad "$_n" "$_desc: exit $_got, expected $_x"
        sed -n '1,10p' "$W/out.$_n" | sed 's/^/    /'
        return
    fi
    if ! cmp -s "$W/want.$_n" "$W/have.$_n"; then
        bad "$_n" "$_desc: failure inventory differs (want: $(tr '\n' ' ' < "$W/want.$_n")| have: $(tr '\n' ' ' < "$W/have.$_n"))"
        return
    fi
    ok "$_n" "$_desc"
}

# ---- cases ------------------------------------------------------------------
make_tree "$W/real" "" real || exit 2
CASE_HNM="nm" run_case complete-real "$W/real" 0 "" \
    "a real compiled image with the six entry points passes, read by the real nm"
unset CASE_HNM

make_tree "$W/full" "nativeEndpointConnect|()J" || exit 2
run_case complete-synthetic-with-extra-row "$W/full" 0 "" \
    "an added ABI row is permitted once the host substrate is complete"

# -- F3: the invariant is the AUTHORITY, not a spelling ----------------------
mk_incomplete() {
    _r=$1; _extra=${2:-}
    make_tree "$_r" "$_extra" || return 1
    : > "$_r/.native/_bridge/libmoq5_jni.dylib.syms"
    return 0
}
mk_incomplete "$W/rowep" "nativeEndpointConnect|()J" || exit 2
run_case authority-endpoint-spelled-row "$W/rowep" 1 "endpoint-preflight.reachable" \
    "an added row spelled with Endpoint is refused while the substrate is incomplete"

mk_incomplete "$W/rowplain" "nativeConnect|()J" || exit 2
run_case authority-plain-spelled-row "$W/rowplain" 1 "endpoint-preflight.reachable" \
    "an added row whose name omits Endpoint is refused just the same"

make_tree "$W/drift" "" || exit 2
: > "$W/drift/.native/_bridge/libmoq5_jni.dylib.syms"
write_abi_header "$W/drift" "" 1
run_case authority-descriptor-drift "$W/drift" 1 "endpoint-preflight.reachable" \
    "a descriptor that drifts from the signed authority is refused"

# -- tool status and diagnostics ---------------------------------------------
CASE_HNM="$W/nm-broken" run_case broken-host-nm "$W/full" 1 \
    "endpoint-preflight.host.substrate endpoint-preflight.reachable" \
    "an nm that exits nonzero is named, and an unknown substrate fails the invariant closed"
unset CASE_HNM
CASE_HNM="$W/nm-noisy" run_case noisy-host-nm "$W/full" 1 \
    "endpoint-preflight.host.substrate endpoint-preflight.reachable" \
    "an nm diagnostic on a zero exit is not discarded, and the invariant stays closed"
unset CASE_HNM
CASE_ANM="$W/nm-broken" run_case broken-android-nm "$W/full" 1 \
    "endpoint-preflight.android.arm64-v8a endpoint-preflight.android.x86_64" \
    "a broken Android nm fails both ABIs by name"
unset CASE_ANM

# -- identity, not counts -----------------------------------------------------
mk_incomplete "$W/coreonly" "nativeConnect|()J" || exit 2
syms_defined "$W/coreonly/.native/_bridge/libmoq5_jni.dylib.syms" \
    moq_session_create moq_rcbuf_wrap moq_endpoint_state
run_case core-only-host "$W/coreonly" 1 "endpoint-preflight.reachable" \
    "unrelated moq_* definitions do not satisfy the endpoint six"

mk_incomplete "$W/undef" "nativeConnect|()J" || exit 2
{
    printf '                 U moq_endpoint_connect\n'
    printf '                 U moq_endpoint_get_snapshot\n'
    printf '                 U moq_endpoint_wait\n'
    printf '                 U moq_endpoint_stop\n'
    printf '                 U moq_endpoint_wake\n'
    printf '                 U moq_endpoint_destroy\n'
} > "$W/undef/.native/_bridge/libmoq5_jni.dylib.syms"
run_case undefined-reference-host "$W/undef" 1 "endpoint-preflight.reachable" \
    "an unresolved reference to every entry point is not a definition"

make_tree "$W/hdup" "" || exit 2
# shellcheck disable=SC2086
syms_defined "$W/hdup/.native/_bridge/libmoq5_jni.dylib.syms" $EP moq_endpoint_wake
run_case host-duplicate-symbol "$W/hdup" 1 "endpoint-preflight.host.substrate" \
    "a duplicated endpoint entry point in the host image is named"

make_tree "$W/adup" "" || exit 2
# shellcheck disable=SC2086
syms_defined "$W/adup/.native/_topology/arm64-v8a/diagnostic/libmoq5_jni.so.syms" \
    $EP moq_endpoint_connect
run_case android-duplicate-symbol "$W/adup" 1 "endpoint-preflight.android.arm64-v8a" \
    "a duplicated or aliased entry point in one ABI is named"

# -- receipt exactness --------------------------------------------------------
make_tree "$W/foreign" "" || exit 2
sed 's/^image.libmoq5_jni.dylib.sha256=.*/image.libmoq5_jni.dylib.sha256=0000000000000000000000000000000000000000000000000000000000000000/' \
    "$W/foreign/.native/_bridge/bridge.provenance" > "$W/foreign/.native/_bridge/p.n"
mv "$W/foreign/.native/_bridge/p.n" "$W/foreign/.native/_bridge/bridge.provenance"
run_case host-foreign-receipt "$W/foreign" 1 "endpoint-preflight.host.provenance" \
    "a current image paired with a foreign receipt is refused"

make_tree "$W/stalesrc" "" || exit 2
printf 'int moq5_stub(void) { return 1; }\n' > "$W/stalesrc/jni/moq5_jni.c"
run_case host-stale-source "$W/stalesrc" 1 "endpoint-preflight.host.provenance" \
    "a receipt describing different source than the current moq5_jni.c is refused"

make_tree "$W/norcpt" "" || exit 2
rm -f "$W/norcpt/.native/_bridge/bridge.provenance"
run_case host-no-receipt "$W/norcpt" 1 "endpoint-preflight.host.provenance" \
    "a host image with no producer receipt is refused"

make_tree "$W/dupkey" "" || exit 2
printf 'producer=build-bridge-testdsos.sh\n' >> "$W/dupkey/.native/_bridge/bridge.provenance"
run_case host-receipt-duplicate-key "$W/dupkey" 1 "endpoint-preflight.host.provenance" \
    "a duplicated receipt key is refused rather than first-match-wins"

make_tree "$W/extrakey" "" || exit 2
printf 'unexpected.key=1\n' >> "$W/extrakey/.native/_bridge/bridge.provenance"
run_case host-receipt-extra-key "$W/extrakey" 1 "endpoint-preflight.host.provenance" \
    "a receipt row outside the declared schema is refused"

make_tree "$W/lookalike" "" || exit 2
sed 's/^producer=/pXoducer=/' "$W/lookalike/.native/_bridge/bridge.provenance" \
    > "$W/lookalike/.native/_bridge/p.n"
mv "$W/lookalike/.native/_bridge/p.n" "$W/lookalike/.native/_bridge/bridge.provenance"
run_case host-receipt-lookalike-key "$W/lookalike" 1 "endpoint-preflight.host.provenance" \
    "a regex-lookalike key does not satisfy a literal key requirement"

make_tree "$W/missimg" "" || exit 2
grep -v '^image.libbridge_omit_one.dylib.sha256=' \
    "$W/missimg/.native/_bridge/bridge.provenance" > "$W/missimg/.native/_bridge/p.n"
mv "$W/missimg/.native/_bridge/p.n" "$W/missimg/.native/_bridge/bridge.provenance"
run_case host-receipt-missing-image-row "$W/missimg" 1 "endpoint-preflight.host.provenance" \
    "a dropped image row is visible against the complete declared set"

# -- Android per-ABI ----------------------------------------------------------
make_tree "$W/amiss" "" || exit 2
syms_defined "$W/amiss/.native/_topology/x86_64/diagnostic/libmoq5_jni.so.syms" \
    moq_endpoint_connect moq_endpoint_get_snapshot moq_endpoint_wait \
    moq_endpoint_stop moq_endpoint_wake
run_case android-missing-member "$W/amiss" 1 "endpoint-preflight.android.x86_64" \
    "a missing entry point in one ABI is a named per-ABI failure"

make_tree "$W/aone" "" || exit 2
rm -rf "$W/aone/.native/_topology/x86_64"
run_case android-one-abi "$W/aone" 1 "endpoint-preflight.android.x86_64" \
    "success in one ABI does not excuse the other"

make_tree "$W/astale" "" || exit 2
printf 'abi=arm64-v8a\nndk.version=28.2.13676358\nndk.cc=/fake/ndk/28.2.13676358/toolchains/llvm/prebuilt/x/bin/clang\ndiagnostic.sha256=0000000000000000000000000000000000000000000000000000000000000000\n' \
    > "$W/astale/.native/_topology/arm64-v8a/topology.provenance"
run_case android-stale-image "$W/astale" 1 "endpoint-preflight.android.arm64-v8a" \
    "an Android image that is not the one its producer recorded is refused"

make_tree "$W/anodig" "" || exit 2
grep -v '^diagnostic.sha256=' "$W/anodig/.native/_topology/arm64-v8a/topology.provenance" \
    > "$W/anodig/.native/_topology/arm64-v8a/p.n"
mv "$W/anodig/.native/_topology/arm64-v8a/p.n" \
   "$W/anodig/.native/_topology/arm64-v8a/topology.provenance"
run_case android-no-digest "$W/anodig" 1 "endpoint-preflight.android.arm64-v8a" \
    "an Android receipt without a diagnostic digest is refused"

make_tree "$W/adupkey" "" || exit 2
printf 'diagnostic.sha256=0000000000000000000000000000000000000000000000000000000000000000\n' \
    >> "$W/adupkey/.native/_topology/arm64-v8a/topology.provenance"
run_case android-duplicate-digest-key "$W/adupkey" 1 "endpoint-preflight.android.arm64-v8a" \
    "a duplicated diagnostic digest key is refused"

make_tree "$W/aabi" "" || exit 2
sed 's/^abi=arm64-v8a/abi=x86_64/' \
    "$W/aabi/.native/_topology/arm64-v8a/topology.provenance" > "$W/aabi/p.n"
mv "$W/aabi/p.n" "$W/aabi/.native/_topology/arm64-v8a/topology.provenance"
run_case android-abi-mismatch "$W/aabi" 1 "endpoint-preflight.android.arm64-v8a" \
    "a receipt from another ABI cannot be consulted for this one"

# -- the toolchain the receipt names, not the first one found ----------------
make_tree "$W/atc" "" || exit 2
_tcbin="$W/atc/ndk/28.2.13676358/toolchains/llvm/prebuilt/x/bin"
mkdir -p "$_tcbin"
cp "$W/nm-synth" "$_tcbin/llvm-nm"
for _abi in arm64-v8a x86_64; do
    sed "s|^ndk.cc=.*|ndk.cc=$_tcbin/clang|" \
        "$W/atc/.native/_topology/$_abi/topology.provenance" > "$W/atc/p.n"
    mv "$W/atc/p.n" "$W/atc/.native/_topology/$_abi/topology.provenance"
done
CASE_ANM="" run_case android-toolchain-derived "$W/atc" 0 "" \
    "each ABI is read with the llvm-nm beside its own recorded compiler"
unset CASE_ANM

rm -f "$_tcbin/llvm-nm"
CASE_ANM="" run_case android-toolchain-missing-nm "$W/atc" 1 \
    "endpoint-preflight.android.arm64-v8a endpoint-preflight.android.x86_64" \
    "a recorded toolchain with no llvm-nm beside it is a named failure"
unset CASE_ANM

make_tree "$W/andkv" "" || exit 2
for _abi in arm64-v8a x86_64; do
    sed 's|^ndk.cc=.*|ndk.cc=/fake/ndk/99.9.9/toolchains/llvm/prebuilt/x/bin/clang|' \
        "$W/andkv/.native/_topology/$_abi/topology.provenance" > "$W/andkv/p.n"
    mv "$W/andkv/p.n" "$W/andkv/.native/_topology/$_abi/topology.provenance"
done
CASE_ANM="" run_case android-ndk-version-mismatch "$W/andkv" 1 \
    "endpoint-preflight.android.arm64-v8a endpoint-preflight.android.x86_64" \
    "a compiler that does not belong to the recorded NDK version is refused"
unset CASE_ANM

# -- the hasher itself --------------------------------------------------------
HASHFAILS="endpoint-preflight.android.arm64-v8a endpoint-preflight.android.x86_64 endpoint-preflight.host.provenance"
mkdir -p "$W/hbin"
_real_shasum=$(command -v shasum || true)
printf '#!/bin/sh\n%s "$@"\nexit 7\n' "${_real_shasum:-true}" > "$W/hbin/sha256sum"
cp "$W/hbin/sha256sum" "$W/hbin/shasum"
chmod +x "$W/hbin/sha256sum" "$W/hbin/shasum"
make_tree "$W/hash1" "" || exit 2
CASE_PATH="$W/hbin:$PATH" run_case hasher-valid-then-nonzero "$W/hash1" 1 "$HASHFAILS" \
    "a hasher that prints a valid digest and exits nonzero cannot pass"
unset CASE_PATH

printf '#!/bin/sh\necho "warn: degraded" >&2\n%s "$@"\n' "${_real_shasum:-true}" \
    > "$W/hbin/sha256sum"
cp "$W/hbin/sha256sum" "$W/hbin/shasum"; chmod +x "$W/hbin/sha256sum" "$W/hbin/shasum"
CASE_PATH="$W/hbin:$PATH" run_case hasher-zero-with-diagnostic "$W/hash1" 1 "$HASHFAILS" \
    "a hasher diagnostic on a zero exit is not discarded"
unset CASE_PATH

printf '#!/bin/sh\necho "NOTAHASH  $3"\n' > "$W/hbin/sha256sum"
cp "$W/hbin/sha256sum" "$W/hbin/shasum"; chmod +x "$W/hbin/sha256sum" "$W/hbin/shasum"
CASE_PATH="$W/hbin:$PATH" run_case hasher-malformed-output "$W/hash1" 1 "$HASHFAILS" \
    "malformed hasher output is refused rather than parsed"
unset CASE_PATH

# A PATH carrying every tool the gate needs EXCEPT a hasher: the refusal must be
# named on any platform, not an environment abort.
mkdir -p "$W/nohash"
for _t in sed grep awk sort comm mktemp rm cat nm cmp tr; do
    _p=$(command -v "$_t" 2>/dev/null) && ln -sf "$_p" "$W/nohash/$_t"
done
make_tree "$W/hash2" "" || exit 2
CASE_PATH="$W/nohash" run_case hasher-absent "$W/hash2" 1 "endpoint-preflight.tooling" \
    "a missing SHA-256 tool is a named preflight failure"
unset CASE_PATH

# ---- the ledger -------------------------------------------------------------
printf '%s\n' "$EXPECTED_CASES" | sort > "$W/ledger.want"
printf '%s\n' "$ran" | tr ' ' '\n' | grep -v '^$' | sort > "$W/ledger.ran"
if ! cmp -s "$W/ledger.want" "$W/ledger.ran"; then
    fails=$((fails + 1))
    printf 'FAIL[selftest.ledger]: the executed cases are not the declared ledger; only in ledger: %s | only executed: %s\n' \
        "$(comm -23 "$W/ledger.want" "$W/ledger.ran" | tr '\n' ' ')" \
        "$(comm -13 "$W/ledger.want" "$W/ledger.ran" | tr '\n' ' ')"
else
    _n=$(awk 'END { print NR }' "$W/ledger.ran")
    printf 'PASS[selftest.ledger]: all %s declared cases ran exactly once\n' "$_n"
fi

if [ "$fails" -eq 0 ]; then
    printf '\njni endpoint preflight self-test: 0 failure(s)\n'
    exit 0
fi
printf '\njni endpoint preflight self-test: %s failure(s)\n' "$fails"
exit 1
