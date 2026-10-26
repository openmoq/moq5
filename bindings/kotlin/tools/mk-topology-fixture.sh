#!/bin/sh
# Builds a DISPOSABLE synthetic topology stage for both ABIs, used ONLY as
# non-vacuity substrate for tools/check-jni-topology.sh.
#
# It declares mode=selftest, so the checker refuses to audit it without an
# explicit --self-test and can never mistake it for the LibMoQ topology.
#
# Unlike the product topology it contains no LibMoQ code. It links the real
# Phase 0.2 dependency archives so the archive, qlog and closure oracles have
# real inputs, and builds one tiny REAL stand-in archive per declared LibMoQ
# archive, so the selected-member and archive-inspection oracles have something
# genuine to select and inspect. The stand-ins MODEL the LibMoQ archives by
# name; they are not the real archives. They are RETAINED under audit-inputs/ -- an archive the checker cannot
# open cannot be proven non-empty or ABI-correct (chats/libmoq/1153 R2).
#
# The receipt records the actual compile and link commands, the working
# directory they ran in, and their real output (chats/libmoq/1153 R9).
set -u
LC_ALL=C; export LC_ALL

here=$(cd "$(dirname "$0")" && pwd) || { echo "cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd)        || { echo "cannot resolve bindings/kotlin" >&2; exit 2; }
repo=$(cd "$root/../.." && pwd)     || { echo "cannot resolve the worktree root" >&2; exit 2; }
. "$here/lib-declaration.sh"
. "$here/lib-inputs.sh"
. "$here/lib-ptls-prefix.sh"

for t in awk sed grep sort tr find dirname basename cp cmp mv mkdir rm printf mktemp; do
    command -v "$t" >/dev/null 2>&1 || { echo "mk-topology-fixture: required tool not found: $t" >&2; exit 2; }
done
for f in decl_sha256 decl_is_sha256 props_count props_value inputs_digest tooling_digest; do
    command -v "$f" >/dev/null 2>&1 || { echo "mk-topology-fixture: required helper not available: $f" >&2; exit 2; }
done

# hash_of <file> -- echo a validated SHA-256; RETURN non-zero on any failure.
# Consumed ONLY in assignment form, `_h=$(hash_of "$f") || die ...`, because a
# helper cannot abort its caller from inside a command substitution: `$( )` runs
# in a subshell. The pattern this replaces, printf '...%s' "$(decl_sha256 f)",
# observes printf's status rather than the hasher's, so a hash failure became an
# empty field in a manifest that still looked well-formed (1171 blocker 2).
hash_of() {
    _hf=$1
    [ -f "$_hf" ] || return 1
    _hv=$(decl_sha256 "$_hf") || return 1
    decl_is_sha256 "$_hv" || return 1
    printf '%s' "$_hv"
}

# prov_of <properties-file> <key> -- echo the single declared value; RETURN
# non-zero otherwise, so zero, duplicate, malformed and unreadable values fail
# by name instead of becoming an empty string a later writer would record.
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
command -v sha256sum >/dev/null 2>&1 || command -v shasum >/dev/null 2>&1 \
    || { echo "mk-topology-fixture: no SHA-256 hasher is available" >&2; exit 2; }

usage() { echo "usage: $(basename "$0") <stage-dir> [variant]" >&2; exit 2; }
[ $# -ge 1 ] && [ $# -le 2 ] || usage
out=$1
variant=${2:-base}
case $variant in
    base|extra-export|dup-jni|qlog-standin|non-pic|extra-needed|product-mode) ;;
    *) echo "unknown variant: $variant" >&2; usage ;;
esac
case $out in ""|-*) usage ;; esac

# The builder never rm -rf's a caller-supplied path. It refuses a destination
# that already exists and creates only the directory it owns, marked as its own.
OWNER_MARK=".moq5-topology-fixture"
[ -e "$out" ] && { echo "mk-topology-fixture: refusing a pre-existing destination: $out" >&2; exit 2; }
mkdir -p "$out" || { echo "mk-topology-fixture: cannot create $out" >&2; exit 2; }
printf 'mk-topology-fixture\n' > "$out/$OWNER_MARK" || { echo "cannot mark ownership" >&2; exit 2; }
# The audited stage is a SUBDIRECTORY: its manifest is total, so the ownership
# marker and the fixture input root must not live inside it.
st="$out/stage"
mkdir -p "$st" || { echo "mk-topology-fixture: cannot create $st" >&2; exit 2; }
die() {
    echo "mk-topology-fixture: FATAL: $*" >&2
    # clean only a path this builder created and still owns
    if [ -f "$out/$OWNER_MARK" ]; then rm -rf -- "$out"; fi
    exit 1
}
trap 'die "interrupted"' HUP INT TERM

# The self-test's DT_NEEDED expectation is FROZEN here, not read back from the
# artifact: a mutant that adds a dependency must not declare it legal.
WANT_NEEDED="libc.so libdl.so"
# The LINK closure, mirroring the product split: libssl.a and libpicoquic-log.a
# are Phase 0.2 outputs that are hash-declared under the `dependency.` key
# family and never linked, so the self-test exercises the same partition the
# product does rather than a looser one of its own.
DEPS="libcrypto.a libpicotls-core.a libpicotls-openssl.a libpicotls-minicrypto.a libpicoquic-core.a libpicohttp-core.a"
DEPS_PINNED="libssl.a libpicoquic-log.a"
MOQS="libmoq-core.a libmoq-service.a libmoq-msf.a libmoq-media-object.a libmoq-loc.a libmoq-cmaf.a libmoq-adapter-picoquic.a libmoq-adapter-picoquic-threaded.a libmoq-adapter-pico-wt.a libmoq-adapter-pico-wt-managed.a libmoq-codec-signaling.a"
# One anchor symbol per LINKED dependency archive, so the archive set is
# identical across variants and only the oracle under test can differ. No anchor
# names a pinned-only archive: TLS_method would pull libssl.a in, and a log-only
# symbol such as cidset_create would pull libpicoquic-log.a in -- exactly the
# contributions the product forbids. The JNI source's picoquic_set_qlog
# reference is the qlog model: it resolves from picoquic-core's qlog_fns member,
# as it does in the product image.
ANCHORS="h3zero_create_error_frame AES_cfb128_encrypt ptls_hpke_setup_base_s ptls_openssl_create_default_certificate_store uECC_compute_public_key BBREnterStartupLongRTT"
dep="$root/.native"

# A small task-owned input tree whose CONTENT the digest is computed over. The
# self-test flips one byte here to prove the digest is content-sensitive.
inroot="$out/.inputs"
mkdir -p "$out/.scratch" || die "cannot create the fixture scratch"
# A sentinel under EVERY declared input family, so dropping a family from the
# inventory is detectable rather than invisible (chats/libmoq/1155 R3).
# The tools family is covered by the DECLARED native tool files, not by an
# arbitrary sentinel: since the authority was split (ruling 1734) a file under
# the tools directory that matches no declaration is a hard failure, and a
# fixture must not need an exemption the product tree does not get.
for _fam in $MOQ_INPUT_FAMILIES; do
    [ "$_fam" = "$MOQ_TOOLS_ROOT" ] && continue
    mkdir -p "$inroot/$_fam" || die "cannot create the fixture family $_fam"
    printf '/* fixture sentinel for %s */\n' "$_fam" > "$inroot/$_fam/sentinel.c" \
        || die "cannot write the sentinel for $_fam"
done
mkdir -p "$inroot/$MOQ_TOOLS_ROOT" || die "cannot create the fixture tools family"
for _nt in $MOQ_NATIVE_TOOL_FILES; do
    mkdir -p "$inroot/$(dirname "$_nt")" || die "cannot create the directory for $_nt"
    printf '# fixture native tool %s\n' "$_nt" > "$inroot/$_nt" \
        || die "cannot write the fixture native tool $_nt"
done
# One evidence tool too, so the fixture exercises the SPLIT rather than only
# the native half: it must land in the tooling manifest and nowhere else.
printf '#!/bin/sh\n# fixture evidence tool\n' \
    > "$inroot/$MOQ_TOOLS_ROOT/check-fixture-evidence.sh" \
    || die "cannot write the fixture evidence tool"
printf 'int moq_fixture_input(void);\n' > "$inroot/core/fixture.h" || die "input write"
printf 'int moq_fixture_input(void){return 1;}\n' > "$inroot/core/fixture.c" || die "input write"
for _fl in $MOQ_INPUT_FILES; do
    mkdir -p "$inroot/$(dirname "$_fl")" || die "cannot create the input file directory for $_fl"
    printf '# fixture input %s\n' "$_fl" > "$inroot/$_fl" || die "input write $_fl"
done

for abi in arm64-v8a x86_64; do
    prov="$dep/$abi/provenance.properties"
    [ -f "$prov" ] || die "missing the accepted Phase 0.2 provenance at $prov"
    cc=$(prov_of "$prov" ndk.cc) || die "$abi: no single ndk.cc in $prov"
    api=$(prov_of "$prov" android.api) || die "$abi: no single android.api in $prov"
    ndkver=$(prov_of "$prov" ndk.version) || die "$abi: no single ndk.version in $prov"
    pqc=$(prov_of "$prov" picoquic.commit) || die "$abi: no single picoquic.commit in $prov"
    ptc=$(prov_of "$prov" picotls.commit) || die "$abi: no single picotls.commit in $prov"
    osc=$(prov_of "$prov" openssl.commit) || die "$abi: no single openssl.commit in $prov"
    bin=$(dirname "$cc") || die "cannot resolve the toolchain bin"
    case $abi in arm64-v8a) tri=aarch64-linux-android;; *) tri=x86_64-linux-android;; esac
    a="$st/$abi"
    work="$a/audit-inputs"
    mkdir -p "$a/package" "$a/diagnostic" "$a/licenses" "$work" || die "mkdir"
    rcpt="$a/topology.log"
    : > "$rcpt" || die "cannot start the receipt"
    printf 'moq-topology: cwd: %s\n' "$work" >> "$rcpt"

    # The staged picotls prefix, exactly as the product recipe stages it, under
    # this fixture's build root. The fixture runs no CMake, so it records what a
    # configure against that prefix would have selected: a cache naming the
    # staged artifacts and a compile command whose picotls headers come from it.
    pp="$work/ptls-prefix"
    ptls_stage_prefix "$dep/_src/picotls/include" "$dep/$abi/lib" "$pp" "$a/ptls-prefix.manifest" \
        || die "$abi: cannot stage the picotls prefix"
    {
        printf 'MOQ_PICOTLS_PREFIX:UNINITIALIZED=%s\n' "$pp"
        printf 'PTLS_INCLUDE_DIR:PATH=%s\n' "$pp/include"
        for _pa in $PTLS_PREFIX_ARCHIVES; do
            _pv=$(printf '%s' "$_pa" | sed 's/^libpicotls-\(.*\)\.a$/\1/' | tr 'a-z' 'A-Z')
            printf 'PTLS_%s_LIBRARY:FILEPATH=%s\n' "$_pv" "$pp/lib/$_pa"
        done
    } > "$work/CMakeCache.txt" || die "$abi: cannot write the fixture CMake cache"
    _tls="$dep/_src/picoquic/picoquic/tls_api.c"
    printf '[\n{ "directory": "%s", "command": "%s -I%s -c %s", "file": "%s" }\n]\n' \
        "$work" "$cc" "$pp/include" "$_tls" "$_tls" > "$work/compile_commands.json" \
        || die "$abi: cannot write the fixture compile commands"

    # One tiny but REAL stand-in archive per LibMoQ archive, RETAINED for inspection.
    i=0
    for m in $MOQS; do
        i=$((i + 1))
        printf 'int moq_fixture_%d(void); int moq_fixture_%d(void){return %d;}\n' "$i" "$i" "$i" > "$work/m$i.c"
        _pic=-fPIC
        [ "$variant" = non-pic ] && [ "$i" = 1 ] && _pic=-fno-pic
        printf 'moq-topology: compile: %s -c %s -Wall -Wextra -Werror -o %s %s\n' \
            "$cc" "$_pic" "$work/m$i.o" "$work/m$i.c" >> "$rcpt"
        "$cc" -c "$_pic" -Wall -Wextra -Werror -o "$work/m$i.o" "$work/m$i.c" >> "$rcpt" 2>&1 \
            || die "$abi: fixture object $i did not compile warning-free"
        "$bin/llvm-ar" rcs "$work/$m" "$work/m$i.o" || die "$abi: fixture archive $m"
        rm -f "$work/m$i.c" "$work/m$i.o" || die "cannot tidy the fixture work files"
    done

    jni="$work/jni.c"
    {
        echo 'typedef struct JavaVM JavaVM;'
        if [ "$variant" = qlog-standin ]; then
            echo 'int picoquic_set_qlog(void *q, const char *d);'
            echo 'int picoquic_set_qlog(void *q, const char *d){(void)q;(void)d;return 0;}'
        else
            echo 'int picoquic_set_qlog(void *q, const char *d);'
        fi
        for s in $ANCHORS; do printf 'extern void %s(void);\n' "$s"; done
        printf 'static void *const dep_anchor[] = {'
        for s in $ANCHORS; do printf ' (void *)&%s,' "$s"; done
        printf ' };\n'
        i=0; for m in $MOQS; do i=$((i + 1)); printf 'int moq_fixture_%d(void);\n' "$i"; done
        _onload=JNI_OnLoad
        if [ "$variant" = dup-jni ]; then
            _onload=JNI_OnLoad_v1
            echo '__attribute__((visibility("default"))) int JNI_OnLoad_v2(JavaVM *vm, void *r);'
            echo 'int JNI_OnLoad_v2(JavaVM *vm, void *r){(void)vm;(void)r;return 0x00010006;}'
            echo '__asm__(".symver JNI_OnLoad_v2,JNI_OnLoad@V2");'
            echo '__asm__(".symver JNI_OnLoad_v1,JNI_OnLoad@@V1");'
        fi
        printf '__attribute__((visibility("default"))) int %s(JavaVM *vm, void *r);\n' "$_onload"
        printf 'int %s(JavaVM *vm, void *r) { int s = 0; (void)vm; (void)r;\n' "$_onload"
        i=0; for m in $MOQS; do i=$((i + 1)); printf '  s += moq_fixture_%d();\n' "$i"; done
        echo '  s += picoquic_set_qlog((void*)0, (const char*)0);'
        echo '  { unsigned _k; for (_k = 0; _k < sizeof dep_anchor / sizeof dep_anchor[0]; _k++) s += (dep_anchor[_k] != (void *)0); }'
        echo '  return s ? 0x00010006 : 0x00010006; }'
    } > "$jni" || die "cannot write the fixture JNI source"

    vmap="$work/ver.map"
    case $variant in
        extra-export) printf '{ global: JNI_OnLoad; moq_fixture_1; local: *; };\n' > "$vmap" ;;
        dup-jni)      printf 'V1 { global: JNI_OnLoad; local: *; };\nV2 { global: JNI_OnLoad; } V1;\n' > "$vmap" ;;
        *)            printf '{ global: JNI_OnLoad; local: *; };\n' > "$vmap" ;;
    esac
    [ -s "$vmap" ] || die "cannot write the version script"

    _ztext=-Wl,-z,text
    _extra_lib=
    [ "$variant" = non-pic ] && _ztext=-Wl,-z,notext
    [ "$variant" = extra-needed ] && _extra_lib="-Wl,--no-as-needed,-llog,--as-needed"
    # The archive list goes through a REAL response file, so the receipt
    # records an @file the auditor must open and re-classify recursively.
    rsp="$work/link.rsp"
    : > "$rsp" || die "cannot write the link response file"
    for d in $DEPS; do printf '%s\n' "$dep/$abi/lib/$d" >> "$rsp" || die "cannot write $rsp"; done
    for m in $MOQS; do printf '%s\n' "$work/$m" >> "$rsp" || die "cannot write $rsp"; done
    for d in $DEPS_PINNED; do
        if grep -q "/$d\$" "$rsp"; then die "$abi: the pinned-only archive $d entered the link response file"; fi
    done

    _linkcmd="$cc -shared -fPIC -fvisibility=hidden -Wall -Wextra -Werror -o $a/diagnostic/libmoq5_jni.so $jni -Wl,--version-script=$vmap -Wl,-Map,$a/link.map $_ztext $_extra_lib -Wl,--start-group @$rsp -Wl,--end-group"
    printf 'moq-topology: link: %s\n' "$_linkcmd" >> "$rcpt"
    ( cd "$work" && "$cc" -shared -fPIC -fvisibility=hidden -Wall -Wextra -Werror \
        -o "$a/diagnostic/libmoq5_jni.so" "$jni" \
        -Wl,--version-script="$vmap" -Wl,-Map,"$a/link.map" $_ztext $_extra_lib \
        -Wl,--start-group "@$rsp" -Wl,--end-group ) >> "$rcpt" 2>&1 \
        || { sed -n '$p' "$rcpt" >&2; die "$abi: fixture link failed"; }
    printf 'moq-topology: link: done\n' >> "$rcpt"
    rm -f "$jni" "$vmap" || die "cannot tidy the fixture link inputs"

    # Declare the response-file set with its content hash, so the bytes the
    # recorded command consumed are bound, not merely present (1159 F1).
    _tab=$(printf '\t')
    _rsph=$(hash_of "$rsp") || die "$abi: cannot hash the link response file"
    printf '%s%s%s\n' "$rsp" "$_tab" "$_rsph" > "$a/responses.manifest" \
        || die "cannot write the response-file declaration"

    cp "$a/diagnostic/libmoq5_jni.so" "$a/package/libmoq5_jni.so" || die "copy"
    "$bin/llvm-strip" --strip-debug --strip-unneeded "$a/package/libmoq5_jni.so" || die "strip"

    needed=$("$bin/llvm-readelf" --wide -d "$a/package/libmoq5_jni.so" \
             | sed -n 's/.*(NEEDED).*Shared library: \[\(.*\)\]/\1/p' | sort | tr '\n' ' ' | sed 's/ *$//')
    if [ "$variant" = base ] && [ "$needed" != "$WANT_NEEDED" ]; then
        die "$abi: the base fixture links [$needed]; the frozen expectation is [$WANT_NEEDED]"
    fi

    # Licence payloads, each copied from the SAME canonical source the checker
    # pins independently. picotls ships no top-level LICENSE in the pinned tree;
    # its MIT statement lives in README.md, which is recorded as that exact fact.
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
        printf '%s\t%s\t%s\t%s\n' "$c" "$_lh" \
            "licenses/$c.LICENSE" "$src" >> "$a/licenses.manifest" || die "manifest write"
    done

    rt=$("$cc" --print-libgcc-file-name) || die "$abi: cannot query the compiler runtime archive"
    [ -f "$rt" ] || die "$abi: the compiler runtime archive does not exist: $rt"
    dg=$(inputs_digest "$inroot" "$out/.scratch") || die "$abi: cannot compute the fixture input digest"
    tg=$(tooling_digest "$inroot" "$out/.scratch") || die "$abi: cannot compute the fixture tooling digest"
    # The fixture carries the same two manifests a product stage carries, so
    # the checker's stage-totality and bound-manifest rules are exercised here
    # rather than only on the product tree (ruling 1734).
    cp "$out/.scratch/libmoq-inputs.manifest" "$a/libmoq-inputs.manifest" \
        || die "$abi: cannot save the fixture input manifest"
    cp "$out/.scratch/libmoq-tooling.manifest" "$a/libmoq-tooling.manifest" \
        || die "$abi: cannot save the fixture tooling manifest"

    # Every hash is taken in assignment form BEFORE the provenance is written,
    # so a hasher failure aborts the fixture instead of leaving printf to report
    # its own success over an empty field.
    _rth=$(hash_of "$rt") || die "$abi: cannot hash the compiler runtime archive"
    _dsoh=$(hash_of "$a/package/libmoq5_jni.so") || die "$abi: cannot hash the packaged DSO"
    _diagh=$(hash_of "$a/diagnostic/libmoq5_jni.so") \
        || die "$abi: cannot hash the diagnostic DSO"
    _maph=$(hash_of "$a/link.map") || die "$abi: cannot hash the link map"
    _rcpth=$(hash_of "$rcpt") || die "$abi: cannot hash the receipt"
    _resph=$(hash_of "$a/responses.manifest") || die "$abi: cannot hash the response manifest"
    _ppp=$(cd "$pp" && pwd -P) || die "$abi: cannot resolve the staged picotls prefix"
    _ppmh=$(hash_of "$a/ptls-prefix.manifest") || die "$abi: cannot hash the picotls prefix manifest"
    _archive_lines=""
    for d in $DEPS; do
        _ah=$(hash_of "$dep/$abi/lib/$d") || die "$abi: cannot hash $d"
        _archive_lines="${_archive_lines}archive.$d=$_ah
"
    done
    for d in $DEPS_PINNED; do
        _ah=$(hash_of "$dep/$abi/lib/$d") || die "$abi: cannot hash $d"
        _archive_lines="${_archive_lines}dependency.$d=$_ah
"
    done
    for m in $MOQS; do
        _ah=$(hash_of "$work/$m") || die "$abi: cannot hash $m"
        _archive_lines="${_archive_lines}archive.$m=$_ah
"
    done

    {
        # A synthetic stage never spells itself `product`: the probe mode is
        # unmistakable and is accepted only under --self-test (1155 R10).
        if [ "$variant" = product-mode ]; then printf 'mode=selftest-product-probe\n'
        else printf 'mode=selftest\n'; fi
        printf 'abi=%s\n' "$abi"
        printf 'android.api=%s\n' "$api"
        printf 'ndk.version=%s\n' "$ndkver"
        printf 'ndk.cc=%s\n' "$cc"
        printf 'ndk.target=%s\n' "$tri"
        printf 'picoquic.commit=%s\n' "$pqc"
        printf 'picotls.commit=%s\n' "$ptc"
        printf 'openssl.commit=%s\n' "$osc"
        printf '%s' "$_archive_lines"
        printf 'libmoq.inputs.sha256=%s\n' "$dg"
        printf 'libmoq.tooling.sha256=%s\n' "$tg"
        printf 'compiler.runtime.path=%s\n' "$rt"
        printf 'compiler.runtime.sha256=%s\n' "$_rth"
        printf 'dso.sha256=%s\n' "$_dsoh"
        printf 'diagnostic.sha256=%s\n' "$_diagh"
        printf 'map.sha256=%s\n' "$_maph"
        printf 'receipt.sha256=%s\n' "$_rcpth"
        printf 'responses.sha256=%s\n' "$_resph"
        printf 'libmoq.inputs.root=%s\n' "$inroot"
        # The build root is declared OUT OF BAND and is contained by the stage,
        # so the receipt cannot nominate its own trusted root (1157 F1).
        printf 'build.root=%s\n' "$work"
        printf 'ptls.prefix=%s\n' "$_ppp"
        printf 'ptls.prefix.manifest.sha256=%s\n' "$_ppmh"
        if [ "$variant" != product-mode ]; then
            printf 'needed.selftest=%s\n' "$WANT_NEEDED"
        fi
    } > "$a/topology.provenance" || die "cannot write the topology provenance"
    # the receipt hash must describe the receipt as it now stands
    _rh=$(hash_of "$rcpt") || die "cannot hash the receipt"
    sed "s|^receipt.sha256=.*|receipt.sha256=$_rh|" "$a/topology.provenance" > "$a/tp.n" \
        || die "cannot rewrite the provenance"
    if ! mv "$a/tp.n" "$a/topology.provenance"; then die "cannot finalise the provenance"; fi
done

trap - HUP INT TERM
printf '%s\n' "$st"
