#!/bin/sh
# Builds the PRODUCT one-DSO topology stage for both ABIs: one self-contained
# libmoq5_jni.so per ABI, containing LibMoQ and the pinned picoquic/picotls/
# OpenSSL closure as PIC static archives, exporting only JNI_OnLoad.
#
# The stage it writes is the input to tools/check-jni-topology.sh, which audits
# it independently. This recipe declares what it did; it asserts nothing about
# whether the result is correct.
#
# PATH CONTRACT. The audited stage is written AT the given path -- by default
# exactly the path the checker audits by default -- never one level below it. A
# stage nested under the audited path would leave the checker inspecting an
# empty container, so the two directories the recipe owns are separated by name
# rather than by depth:
#
#   <dep>/_topology          the audited stage; its path manifest is exact
#   <dep>/_topology.build    the recipe's own container: ownership marker,
#                            CMake intermediate trees, scratch
#
# The container is a SIBLING because an intermediate build tree inside the stage
# would enter the stage's exact path manifest and break the `stage` oracle. Its
# path is what the provenance declares as `build.root`, bounded by the checker.
#
# The Phase 0.2 dependency tree under bindings/kotlin/.native is consumed
# READ-ONLY: the six dependency archives linked here are exactly the ones the
# checker pins by path and hash. Two Phase 0.2 outputs are pinned-only, and both
# are hash-declared under the `dependency.` key family and never enter the link:
#   * libssl.a -- picotls provides TLS 1.3 and OpenSSL is used solely as the
#     crypto provider, so the OpenSSL record layer is unreferenced;
#   * libpicoquic-log.a -- picoquic-core carries the qlog streaming functions
#     (picoquic_set_qlog among them), so nothing this image reaches is defined
#     only in the log archive.
#
# LibMoQ's own CMake contract compiles the picoquic adapters against picoquic's
# private headers, so it configures the pinned picoquic source tree as a
# subproject and builds picoquic archives of its own as a by-product. Those
# by-products are NOT linked -- the header revision is what the private-header
# contract requires to match, and both come from the same pinned checkout.
set -u
LC_ALL=C; export LC_ALL

here=$(cd "$(dirname "$0")" && pwd) || { echo "cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd)        || { echo "cannot resolve bindings/kotlin" >&2; exit 2; }
repo=$(cd "$root/../.." && pwd)     || { echo "cannot resolve the worktree root" >&2; exit 2; }
. "$here/lib-declaration.sh"
. "$here/lib-inputs.sh"
. "$here/lib-ptls-prefix.sh"

for t in awk sed grep sort tr find dirname basename cp cmp mkdir rm printf cmake ninja; do
    command -v "$t" >/dev/null 2>&1 || { echo "build-jni-topology: required tool not found: $t" >&2; exit 2; }
done
for f in decl_sha256 decl_is_sha256 props_count props_value inputs_digest; do
    command -v "$f" >/dev/null 2>&1 || { echo "build-jni-topology: required helper not available: $f" >&2; exit 2; }
done

usage() { echo "usage: $(basename "$0") [--force] [STAGE-DIR]" >&2; exit 2; }
FORCE=0
stage_arg=""
while [ $# -gt 0 ]; do
    case $1 in
        --force) [ "$FORCE" -eq 0 ] || { echo "duplicate --force" >&2; usage; }; FORCE=1; shift ;;
        -*) echo "unknown option: $1" >&2; usage ;;
        *) [ -z "$stage_arg" ] || { echo "more than one stage path given" >&2; usage; }
           stage_arg=$1; shift ;;
    esac
done

dep="$root/.native"
[ -d "$dep" ] || { echo "build-jni-topology: no Phase 0.2 dependency stage at $dep" >&2; exit 2; }
# The DEFAULT is the path the checker audits by default. Keeping the two
# defaults aligned is what stops the recipe and the checker drifting apart.
[ -n "$stage_arg" ] || stage_arg="$dep/_topology"
st=$stage_arg
build="$dep/_topology.build"

# A recipe that silently wrote <arg>/stage would produce exactly the mismatch
# ruling 1171 blocker 1 describes, so the shape is refused by name here as well
# as in the checker.
case $(basename "$st") in
    stage) echo "build-jni-topology: refusing to write an audited stage named 'stage': the checker audits the path it is given, not a directory nested below it" >&2; exit 2 ;;
esac

OWNER_MARK="$build/.moq5-topology-build"

# ---- the mutation lock -------------------------------------------------------
# This recipe is a stage WRITER: it deletes and rebuilds the durable stage, so it
# races a reader or the mutating product-contract suite exactly as they race each
# other. It therefore takes the same sibling lock, and takes it BEFORE removing
# or creating anything (chats/libmoq/1177).
#
# mkdir is the atomic primitive: it succeeds for exactly one caller. There is
# deliberately NO waiting and NO retry.
#
# A token is written so the lock has a uniform shape, but it is NEVER exported:
# nothing is admitted through this recipe's lock, because this recipe never
# audits the stage. A checker meeting it therefore refuses, which is correct.
LOCKDIR="$dep/_topology.lock"
LOCK_HELD=0
case $LOCKDIR in
    "$st"|"$st"/*)
        echo "build-jni-topology: refusing a lock path inside the audited stage: $LOCKDIR" >&2; exit 2 ;;
esac
if ! mkdir "$LOCKDIR" 2>/dev/null; then
    echo "build-jni-topology: the durable topology stage is locked for mutation ($LOCKDIR$([ -f "$LOCKDIR/owner" ] && printf ', held by %s' "$(cat "$LOCKDIR/owner" 2>/dev/null)")); refusing to rebuild it concurrently. Nothing was deleted or modified. Re-run when the holder has finished; if no such run is active, the lock is stale and may be removed." >&2
    exit 2
fi
LOCK_HELD=1
_lock_token=$(mktemp -u "moq5-lock-XXXXXXXXXXXX" | sed 's|.*/||')
printf '%s\n' "$_lock_token" > "$LOCKDIR/token" 2>/dev/null
printf 'build-jni-topology pid %s\n' "$$" > "$LOCKDIR/owner" 2>/dev/null
unlock() { [ "${LOCK_HELD:-0}" -eq 1 ] && rm -rf -- "$LOCKDIR"; LOCK_HELD=0; }

if [ -e "$st" ]; then
    # Only a stage this recipe owns may be replaced, and only on request.
    [ "$FORCE" -eq 1 ] || { echo "build-jni-topology: $st already exists; pass --force to rebuild it" >&2; unlock; exit 2; }
    [ -f "$OWNER_MARK" ] || { echo "build-jni-topology: $st exists but carries no ownership marker at $OWNER_MARK; refusing to remove a directory this recipe did not create" >&2; unlock; exit 2; }
    rm -rf -- "$st" || { echo "build-jni-topology: cannot replace $st" >&2; unlock; exit 2; }
fi
if [ -e "$build" ]; then
    rm -rf -- "$build" || { echo "build-jni-topology: cannot clear $build" >&2; unlock; exit 2; }
fi
mkdir -p "$st" "$build/.scratch" || { echo "build-jni-topology: cannot create the stage and its build container" >&2; unlock; exit 2; }
printf 'build-jni-topology\n' > "$OWNER_MARK" || { echo "cannot mark ownership" >&2; unlock; exit 2; }

die() {
    echo "build-jni-topology: FATAL: $*" >&2
    # Clean only paths this recipe created and still owns. A partial stage is
    # worse than none, so it is removed; the stage survives only when complete.
    if [ -f "$OWNER_MARK" ]; then rm -rf -- "$st"; fi
    unlock
    exit 1
}
trap 'die "interrupted"' HUP INT TERM

# A narrow, documented self-test seam. When MOQ5_TOPOLOGY_LOCK_PROBE names an
# executable, this recipe runs it here -- holding the lock, with a partial stage
# on disk -- and then terminates as it would on any failure: partial stage
# removed, lock released. It exists so a self-test can observe the lock while
# this recipe genuinely holds it, without sleeping or racing the real build. It
# grants no access: it runs only AFTER the lock was acquired, and the token is
# still never exported.
if [ -n "${MOQ5_TOPOLOGY_LOCK_PROBE-}" ]; then
    "$MOQ5_TOPOLOGY_LOCK_PROBE"; _prc=$?
    [ "$_prc" -eq 0 ] || die "the lock probe reported failure ($_prc)"
    if [ -f "$OWNER_MARK" ]; then rm -rf -- "$st"; fi
    unlock
    exit 0
fi

# hash_of <file> -- echo a validated SHA-256; RETURN non-zero on any failure.
#
# Every hash is consumed through this helper, and always in assignment form:
#
#     _h=$(hash_of "$f") || die "..."
#
# The status must be carried by the assignment, because a helper cannot abort
# its caller from inside a command substitution: `$( )` runs in a subshell, so
# an `exit` there ends the subshell and leaves the caller running with an empty
# value -- the same masked failure as the pattern this replaces,
# printf '...%s' "$(decl_sha256 f)", which observes printf's status rather than
# the hasher's (chats/libmoq/1171 blocker 2).
hash_of() {
    _hf=$1
    [ -f "$_hf" ] || return 1
    _hv=$(decl_sha256 "$_hf") || return 1
    decl_is_sha256 "$_hv" || return 1
    printf '%s' "$_hv"
}

# prov_of <properties-file> <key> -- echo the single declared value; RETURN
# non-zero otherwise. Zero, duplicate, malformed and unreadable values all fail
# here rather than becoming an empty string a later writer would record.
# Consumed in assignment form for the same reason as hash_of.
prov_of() {
    _pf=$1; _pk=$2
    # Exactly-one semantics, built on the accepted property primitives: property
    # loading is last-wins, so a duplicate would silently change the effective
    # value, and a missing key must not read as an empty one.
    _pc=$(props_count "$_pf" "$_pk") || return 1
    [ "$_pc" = "1" ] || return 1
    _pv=$(props_value "$_pf" "$_pk") || return 1
    [ -n "$_pv" ] || return 1
    printf '%s' "$_pv"
}

# step_diagnostics <receipt> -- the error lines of the step that just failed:
# what the receipt holds after its last `moq-topology:` command line. The
# failure path removes the partial stage, receipt included, so these lines are
# the only record of WHY a compile or link failed (a missing header, the exact
# unresolved definitions), not just that it did.
step_diagnostics() {
    awk '/^moq-topology: /{ n = 0; next } { l[++n] = $0 } END { for (i = 1; i <= n; i++) print l[i] }' "$1" \
        | grep -E 'error|undefined' | sed -n '1,20p' >&2
}

# The LINK closure: every one of these must contribute a member.
DEPS="libcrypto.a libpicotls-core.a libpicotls-openssl.a libpicotls-minicrypto.a libpicoquic-core.a libpicohttp-core.a"
# Pinned Phase 0.2 outputs that are hash-declared but never linked.
DEPS_PINNED="libssl.a libpicoquic-log.a"
# The LibMoQ archives the one-DSO closure requires. codec-signaling is among
# them because the JNI codec rows call its builder and formatter; it
# PUBLIC-links only core, so it adds nothing outside this closure.
MOQS="libmoq-core.a libmoq-service.a libmoq-msf.a libmoq-media-object.a libmoq-loc.a libmoq-cmaf.a libmoq-adapter-picoquic.a libmoq-adapter-picoquic-threaded.a libmoq-adapter-pico-wt.a libmoq-adapter-pico-wt-managed.a libmoq-codec-signaling.a"
TARGETS="moq-core moq-service moq-msf moq-media-object moq-loc moq-cmaf moq-adapter-picoquic moq-adapter-picoquic-threaded moq-adapter-pico-wt moq-adapter-pico-wt-managed moq-codec-signaling"
jni_src="$root/jni/moq5_jni.c"
[ -f "$jni_src" ] || die "the JNI substrate source is absent: $jni_src"

# The private handle registry is compiled INTO the shipped image. It exports
# nothing: the version script keeps JNI_OnLoad the only global, and the checker
# proves no registry or test-seam symbol is exported. MOQ5_REGISTRY_TESTING is
# never defined here, so the seams that drive allocation failure, near-wrap
# generations and lease scheduling are not in the Android objects at all.
reg_src="$root/jni/moq5_registry.c"
[ -f "$reg_src" ] || die "the handle registry source is absent: $reg_src"

for abi in arm64-v8a x86_64; do
    prov="$dep/$abi/provenance.properties"
    [ -f "$prov" ] || die "missing the accepted Phase 0.2 provenance at $prov"
    cc=$(prov_of "$prov" ndk.cc) || die "$abi: no single ndk.cc in $prov"
    api=$(prov_of "$prov" android.api) || die "$abi: no single android.api in $prov"
    ndkver=$(prov_of "$prov" ndk.version) || die "$abi: no single ndk.version in $prov"
    pqc=$(prov_of "$prov" picoquic.commit) || die "$abi: no single picoquic.commit in $prov"
    ptc=$(prov_of "$prov" picotls.commit) || die "$abi: no single picotls.commit in $prov"
    osc=$(prov_of "$prov" openssl.commit) || die "$abi: no single openssl.commit in $prov"
    [ -x "$cc" ] || die "$abi: the declared NDK compiler is not executable: $cc"
    bin=$(dirname "$cc") || die "cannot resolve the toolchain bin"
    ndk=$(cd "$bin/../../../../.." && pwd) || die "cannot resolve the NDK root"
    [ -f "$ndk/build/cmake/android.toolchain.cmake" ] \
        || die "$abi: the NDK toolchain file is not under the resolved NDK root: $ndk"
    case $abi in arm64-v8a) tri=aarch64-linux-android;; *) tri=x86_64-linux-android;; esac
    for d in $DEPS $DEPS_PINNED; do
        [ -f "$dep/$abi/lib/$d" ] || die "$abi: the pinned dependency archive is absent: $d"
    done

    a="$st/$abi"
    ai="$a/audit-inputs"
    mkdir -p "$a/package" "$a/diagnostic" "$a/licenses" "$ai" || die "mkdir"
    rcpt="$a/topology.log"
    : > "$rcpt" || die "cannot start the receipt"
    # The FINAL LINK runs here, inside the audited stage's retained inputs, so
    # the linked bytes stay stage-local evidence.
    printf 'moq-topology: cwd: %s\n' "$ai" >> "$rcpt"

    # --- LibMoQ archives, built with the NDK toolchain against the pinned deps.
    # Every out-of-sysroot path is passed explicitly: the Android toolchain
    # confines find_path/find_library to the sysroot, so a prefix alone is not
    # enough to locate picotls, OpenSSL or the picotls-OpenSSL headers.
    b="$build/$abi"
    # LibMoQ takes picotls from ONE prefix (MOQ_PICOTLS_PREFIX is the authority
    # for its headers and archives), while the Phase 0.2 tree keeps the headers
    # in the pinned checkout and the archives per ABI. So each ABI gets a staged
    # root of byte-identical copies (lib-ptls-prefix.sh), inside this recipe's
    # build root, with the copy mapping saved in the stage. The final link below
    # still reads the accepted archives themselves.
    pp="$b/ptls-prefix"
    mkdir -p "$b" || die "$abi: cannot create the build root"
    ptls_stage_prefix "$dep/_src/picotls/include" "$dep/$abi/lib" "$pp" "$a/ptls-prefix.manifest" \
        || die "$abi: cannot stage the picotls prefix"
    set -- \
        -S "$repo" -B "$b" -G Ninja \
        -DCMAKE_TOOLCHAIN_FILE="$ndk/build/cmake/android.toolchain.cmake" \
        -DANDROID_ABI="$abi" -DANDROID_PLATFORM="android-$api" \
        -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DBUILD_TESTING=OFF \
        -DMOQ_BUILD_TESTS=OFF -DMOQ_BUILD_EXAMPLES=OFF -DMOQ_BUILD_SIM=OFF \
        -DMOQ_BUILD_SERVICE=ON -DMOQ_BUILD_MSF=ON -DMOQ_BUILD_MEDIA_OBJECT=ON \
        -DMOQ_BUILD_LOC=ON -DMOQ_BUILD_CMAF=ON \
        -DMOQ_BUILD_CODEC_SIGNALING=ON \
        -DMOQ_BUILD_ADAPTER_PICOQUIC=ON -DMOQ_BUILD_PQ_THREADED=ON \
        -DMOQ_BUILD_ADAPTER_PICO_WT=ON -DMOQ_BUILD_PICO_WT_MANAGED=ON \
        -DMOQ_PICOQUIC_SOURCE_DIR="$dep/_src/picoquic" \
        -DMOQ_PICOTLS_PREFIX="$pp" \
        -DPTLS_INCLUDE_DIR="$pp/include" \
        -DMOQ_PTLS_OPENSSL_INCLUDE_DIR="$pp/include" \
        -DPTLS_CORE_LIBRARY="$pp/lib/libpicotls-core.a" \
        -DPTLS_OPENSSL_LIBRARY="$pp/lib/libpicotls-openssl.a" \
        -DPTLS_MINICRYPTO_LIBRARY="$pp/lib/libpicotls-minicrypto.a" \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
        -DOPENSSL_ROOT_DIR="$dep/_prefix/$abi" -DOPENSSL_USE_STATIC_LIBS=ON \
        -DOPENSSL_CRYPTO_LIBRARY="$dep/_prefix/$abi/lib/libcrypto.a" \
        -DOPENSSL_SSL_LIBRARY="$dep/_prefix/$abi/lib/libssl.a" \
        -DOPENSSL_INCLUDE_DIR="$dep/_prefix/$abi/include" \
        -DPICOQUIC_FETCH_PTLS=OFF -DPICOQUIC_FETCH_AEGIS=OFF
    printf 'moq-topology: configure: cmake %s\n' "$*" >> "$rcpt"
    cmake "$@" >> "$rcpt" 2>&1 || { sed -n '$p' "$rcpt" >&2; die "$abi: CMake configuration failed"; }
    printf 'moq-topology: build: cmake --build %s --target %s\n' "$b" "$TARGETS" >> "$rcpt"
    # shellcheck disable=SC2086  # TARGETS must split into separate arguments
    cmake --build "$b" --target $TARGETS -j4 >> "$rcpt" 2>&1 \
        || { sed -n '$p' "$rcpt" >&2; die "$abi: the LibMoQ archives did not build"; }

    for m in $MOQS; do
        _src=$(find "$b" -name "$m" -type f | sed -n '1p')
        [ -n "$_src" ] || die "$abi: the build produced no $m"
        cp "$_src" "$ai/$m" || die "$abi: cannot stage $m"
    done

    # --- the JNI substrate, then the one-DSO link.
    obj="$ai/moq5_jni.o"
    reg_obj="$ai/moq5_registry.o"
    # service/src carries the INTERNAL trust route header, which is not
    # installed; the OpenSSL headers serve the delegate's native identity
    # check, against the libcrypto archive the link already pins.
    set -- -c -fPIC -fvisibility=hidden -Wall -Wextra -Werror -std=c11 \
        -I"$repo/core/include" -I"$repo/service/include" -I"$repo/service/src" \
        -I"$dep/_prefix/$abi/include" \
        -I"$repo/media/msf/include" -I"$repo/media/object/include" \
        -I"$repo/media/loc/include" -I"$repo/media/cmaf/include" \
        -I"$repo/media/codec_signaling/include" \
        -I"$repo/adapters/picoquic/include" -I"$repo/adapters/pico_wt/include" \
        -I"$dep/_src/picoquic/loglib" -I"$dep/_src/picoquic/picoquic" \
        -I"$dep/_src/picoquic/picohttp" \
        -I"$pp/include" \
        -MJ "$a/jni-compile.json" \
        -o "$obj" "$jni_src"
    printf 'moq-topology: compile: %s %s\n' "$cc" "$*" >> "$rcpt"
    "$cc" "$@" >> "$rcpt" 2>&1 || { step_diagnostics "$rcpt"; die "$abi: the JNI substrate did not compile warning-free"; }
    # The compiler's own record of that compile (-MJ): what the checker judges
    # the JNI substrate's include search from.
    [ -s "$a/jni-compile.json" ] || die "$abi: the compiler wrote no record of the JNI compile"

    set -- -c -fPIC -fvisibility=hidden -Wall -Wextra -Werror -std=c11 \
        -o "$reg_obj" "$reg_src"
    printf 'moq-topology: compile: %s %s\n' "$cc" "$*" >> "$rcpt"
    "$cc" "$@" >> "$rcpt" 2>&1 \
        || { step_diagnostics "$rcpt"; die "$abi: the handle registry did not compile warning-free"; }

    vmap="$ai/ver.map"
    printf '{ global: JNI_OnLoad; local: *; };\n' > "$vmap" || die "cannot write the version script"
    rsp="$ai/link.rsp"
    : > "$rsp" || die "cannot write the link response file"
    for d in $DEPS; do printf '%s\n' "$dep/$abi/lib/$d" >> "$rsp" || die "cannot write $rsp"; done
    for m in $MOQS; do printf '%s\n' "$ai/$m" >> "$rsp" || die "cannot write $rsp"; done
    # A pinned-only output must never reach the linker. Asserted here as well as
    # in the checker, so the recipe cannot quietly produce what the checker then
    # has to catch.
    for d in $DEPS_PINNED; do
        if grep -q "/$d\$" "$rsp"; then die "$abi: the pinned-only archive $d entered the link response file"; fi
    done

    _link="$cc -shared -fPIC -fvisibility=hidden -Wall -Wextra -Werror -o $a/diagnostic/libmoq5_jni.so $obj $reg_obj -Wl,--version-script=$vmap -Wl,-Map,$a/link.map -Wl,-z,text -Wl,--no-undefined -Wl,--start-group @$rsp -Wl,--end-group"
    printf 'moq-topology: link: %s\n' "$_link" >> "$rcpt"
    ( cd "$ai" && "$cc" -shared -fPIC -fvisibility=hidden -Wall -Wextra -Werror \
        -o "$a/diagnostic/libmoq5_jni.so" "$obj" "$reg_obj" \
        -Wl,--version-script="$vmap" -Wl,-Map,"$a/link.map" \
        -Wl,-z,text -Wl,--no-undefined \
        -Wl,--start-group "@$rsp" -Wl,--end-group ) >> "$rcpt" 2>&1 \
        || { step_diagnostics "$rcpt"; die "$abi: the one-DSO link failed"; }
    printf 'moq-topology: link: done\n' >> "$rcpt"
    rm -f "$obj" "$reg_obj" "$vmap" || die "cannot tidy the link inputs"

    _tab=$(printf '\t')
    _rsph=$(hash_of "$rsp") || die "$abi: cannot hash the link response file"
    printf '%s%s%s\n' "$rsp" "$_tab" "$_rsph" > "$a/responses.manifest" \
        || die "cannot write the response-file declaration"

    cp "$a/diagnostic/libmoq5_jni.so" "$a/package/libmoq5_jni.so" || die "copy"
    "$bin/llvm-strip" --strip-debug --strip-unneeded "$a/package/libmoq5_jni.so" || die "strip"

    : > "$a/licenses.manifest" || die "cannot start the licence manifest"
    for pair in "libmoq:$repo/LICENSE.txt" \
                "picoquic:$dep/_src/picoquic/LICENSE" \
                "picotls:$dep/_src/picotls/README.md" \
                "openssl:$dep/_src/openssl/LICENSE.txt" \
                "cifra:$dep/_src/picotls/deps/cifra/COPYING" \
                "micro-ecc:$dep/_src/picotls/deps/micro-ecc/LICENSE.txt" \
                "moq-msf-json:$repo/media/msf/THIRD_PARTY.md"; do
        c=${pair%%:*}; src=${pair#*:}
        [ -f "$src" ] || die "the pinned licence source for $c is absent: $src"
        cp "$src" "$a/licenses/$c.LICENSE" || die "licence copy $c"
        _lh=$(hash_of "$a/licenses/$c.LICENSE") || die "$abi: cannot hash the $c licence"
        printf '%s\t%s\t%s\t%s\n' "$c" "$_lh" "licenses/$c.LICENSE" "$src" \
            >> "$a/licenses.manifest" || die "manifest write"
    done

    rt=$("$cc" --print-libgcc-file-name) || die "$abi: cannot query the compiler runtime archive"
    [ -f "$rt" ] || die "$abi: the compiler runtime archive does not exist: $rt"
    rth=$(hash_of "$rt") || die "$abi: cannot hash the compiler runtime archive"
    dg=$(inputs_digest "$repo" "$build/.scratch") \
        || die "$abi: cannot compute the LibMoQ input digest"
    decl_is_sha256 "$dg" || die "$abi: the LibMoQ input digest is not a SHA-256"
    # The EVIDENCE inventory travels beside it. It binds no artefact: it is
    # recorded so a host-tool change is still accounted for somewhere, without
    # invalidating a DSO no host tool can affect (ruling 1734).
    tg=$(tooling_digest "$repo" "$build/.scratch") \
        || die "$abi: cannot compute the tooling/evidence digest"
    decl_is_sha256 "$tg" || die "$abi: the tooling digest is not a SHA-256"
    # The manifest the artefacts are bound to is SAVED IN THE STAGE, not left
    # in this recipe's scratch container -- which this recipe itself clears, so
    # a stage whose manifest lived only there could not say what bound it
    # (chats/libmoq/1733, ruled in 1734).
    cp "$build/.scratch/libmoq-inputs.manifest" "$a/libmoq-inputs.manifest" \
        || die "$abi: cannot save the bound input manifest into the stage"
    cp "$build/.scratch/libmoq-tooling.manifest" "$a/libmoq-tooling.manifest" \
        || die "$abi: cannot save the tooling manifest into the stage"
    _mfh=$(hash_of "$a/libmoq-inputs.manifest") \
        || die "$abi: cannot hash the saved input manifest"
    [ "$_mfh" = "$dg" ] \
        || die "$abi: the saved input manifest hashes $_mfh, not the declared $dg"
    _tfh=$(hash_of "$a/libmoq-tooling.manifest") \
        || die "$abi: cannot hash the saved tooling manifest"
    [ "$_tfh" = "$tg" ] \
        || die "$abi: the saved tooling manifest hashes $_tfh, not the declared $tg"
    dsoh=$(hash_of "$a/package/libmoq5_jni.so") || die "$abi: cannot hash the packaged DSO"
    # The DIAGNOSTIC image needs its own recorded digest. It is the only one of
    # the two whose symbol table survives stripping, so it is the image any gate
    # judging the linked substrate must read -- and a gate may not judge an
    # artefact its producer never bound to this build.
    diagh=$(hash_of "$a/diagnostic/libmoq5_jni.so") \
        || die "$abi: cannot hash the diagnostic DSO"
    maph=$(hash_of "$a/link.map") || die "$abi: cannot hash the link map"
    resph=$(hash_of "$a/responses.manifest") || die "$abi: cannot hash the response manifest"
    _rcpt_h=$(hash_of "$rcpt") || die "$abi: cannot hash the receipt"
    _brootp=$(cd "$b" && pwd -P) || die "$abi: cannot resolve the build root"
    _ppp=$(cd "$pp" && pwd -P) || die "$abi: cannot resolve the staged picotls prefix"
    _ppmh=$(hash_of "$a/ptls-prefix.manifest") || die "$abi: cannot hash the picotls prefix manifest"
    _jch=$(hash_of "$a/jni-compile.json") || die "$abi: cannot hash the JNI compile record"

    # Every hash below is produced by hash_of, which fails the recipe on a
    # hasher error rather than leaving printf to report its own success.
    : > "$a/topology.provenance" || die "cannot start the topology provenance"
    {
        printf 'mode=product\n'
        printf 'abi=%s\n' "$abi"
        printf 'android.api=%s\n' "$api"
        printf 'ndk.version=%s\n' "$ndkver"
        printf 'ndk.cc=%s\n' "$cc"
        printf 'ndk.target=%s\n' "$tri"
        printf 'picoquic.commit=%s\n' "$pqc"
        printf 'picotls.commit=%s\n' "$ptc"
        printf 'openssl.commit=%s\n' "$osc"
    } >> "$a/topology.provenance" || die "cannot write the topology provenance"
    for d in $DEPS; do
        _ah=$(hash_of "$dep/$abi/lib/$d") || die "$abi: cannot hash $d"
        printf 'archive.%s=%s\n' "$d" "$_ah" >> "$a/topology.provenance" || die "provenance write"
    done
    for d in $DEPS_PINNED; do
        _ah=$(hash_of "$dep/$abi/lib/$d") || die "$abi: cannot hash $d"
        printf 'dependency.%s=%s\n' "$d" "$_ah" >> "$a/topology.provenance" || die "provenance write"
    done
    for m in $MOQS; do
        _ah=$(hash_of "$ai/$m") || die "$abi: cannot hash $m"
        printf 'archive.%s=%s\n' "$m" "$_ah" >> "$a/topology.provenance" || die "provenance write"
    done
    {
        printf 'libmoq.inputs.sha256=%s\n' "$dg"
        printf 'libmoq.tooling.sha256=%s\n' "$tg"
        printf 'compiler.runtime.path=%s\n' "$rt"
        printf 'compiler.runtime.sha256=%s\n' "$rth"
        printf 'dso.sha256=%s\n' "$dsoh"
        printf 'diagnostic.sha256=%s\n' "$diagh"
        printf 'map.sha256=%s\n' "$maph"
        printf 'receipt.sha256=%s\n' "$_rcpt_h"
        printf 'responses.sha256=%s\n' "$resph"
        printf 'build.root=%s\n' "$_brootp"
        printf 'ptls.prefix=%s\n' "$_ppp"
        printf 'ptls.prefix.manifest.sha256=%s\n' "$_ppmh"
        printf 'jni.compile.sha256=%s\n' "$_jch"
    } >> "$a/topology.provenance" || die "cannot write the topology provenance"
    # The receipt hash must describe the receipt as it now stands.
    _rh=$(hash_of "$rcpt") || die "$abi: cannot hash the receipt"
    sed "s|^receipt.sha256=.*|receipt.sha256=$_rh|" "$a/topology.provenance" > "$a/tp.n" \
        || die "cannot rewrite the provenance"
    mv "$a/tp.n" "$a/topology.provenance" || die "cannot finalise the provenance"
done

trap - HUP INT TERM
unlock
printf '%s\n' "$st"
