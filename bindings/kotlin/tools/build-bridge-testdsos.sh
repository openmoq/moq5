#!/bin/sh
# Build the host shared objects the bridge handshake tests load.
#
# Two kinds are produced, and the distinction is the whole point:
#
#   * PRODUCT   -- `jni/moq5_jni.c` itself, compiled for the host with
#                  MOQ5_JNI_HOST_TEST so the LibMoQ anchor table (which needs
#                  the Android cross-build) is omitted. Nothing else changes,
#                  so the registration and handshake code under test is the
#                  shipped code, not a transcription of it.
#   * FIXTURES  -- `jni/testfixtures/moq5_bridge_fixture.c`, which expands the
#                  SAME MOQ5_ABI_METHODS authority and parameterises only what a
#                  real skew varies. Each variant differs from the compatible
#                  build in exactly one respect.
#
# A JNI include directory is DISCOVERED, never hard-coded: a path is not a pin.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
case $0 in
    */*) _selfdir=${0%/*} ;;
    *)   _selfdir=. ;;
esac
here=$(cd "$_selfdir" && pwd) || { echo "$_self: cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd)  || { echo "$_self: cannot resolve bindings/kotlin" >&2; exit 2; }
[ -r "$here/lib-hash.sh" ] || { echo "$_self: missing $here/lib-hash.sh" >&2; exit 2; }
# shellcheck source=lib-hash.sh
. "$here/lib-hash.sh"

for t in cc mkdir rm printf sed strings; do
    command -v "$t" >/dev/null 2>&1 \
        || { echo "$_self: required tool not found: $t" >&2; exit 2; }
done

die() { echo "$_self: FATAL: $*" >&2; exit 1; }

# -- discover a JNI include directory -----------------------------------------
# In order: an explicit override, the active JAVA_HOME, then every JDK the host
# advertises. A JDK image without headers (the Android Studio JBR is one) is
# skipped rather than failing the search.
jni_include=""
try_jdk() {
    [ -n "${1:-}" ] || return 1
    [ -f "$1/include/jni.h" ] || return 1
    jni_include=$1/include
    return 0
}

if [ -n "${MOQ5_JNI_INCLUDE:-}" ]; then
    [ -f "$MOQ5_JNI_INCLUDE/jni.h" ] \
        || die "MOQ5_JNI_INCLUDE=$MOQ5_JNI_INCLUDE holds no jni.h"
    jni_include=$MOQ5_JNI_INCLUDE
fi

if [ -z "$jni_include" ]; then
    try_jdk "${JAVA_HOME:-}" || true
fi

if [ -z "$jni_include" ] && [ -x /usr/libexec/java_home ]; then
    _homes=$(/usr/libexec/java_home -V 2>&1 | sed -n 's|.* \(/.*/Contents/Home\)$|\1|p')
    for _h in $_homes; do
        try_jdk "$_h" && break
    done
fi

[ -n "$jni_include" ] || die \
    "no JNI headers found. The bridge boundary needs a JDK image that ships \
include/jni.h; the pinned Android Studio JBR does not. Set MOQ5_JNI_INCLUDE to \
one, or install a JDK the host advertises."

case $(uname -s) in
    Darwin) jni_md="$jni_include/darwin" ;;
    Linux)  jni_md="$jni_include/linux" ;;
    *)      die "unsupported host $(uname -s) for the bridge test DSOs" ;;
esac
[ -f "$jni_md/jni_md.h" ] || die "no jni_md.h under $jni_md"

case $(uname -s) in
    Darwin) dso_ext=dylib ;;
    *)      dso_ext=so ;;
esac

out="$root/.native/_bridge"
rm -rf -- "$out" || die "cannot clear $out"
mkdir -p "$out" || die "cannot create $out"

WARN="-Wall -Wextra -Werror -std=c11"

# ---- the host endpoint substrate -------------------------------------------
#
# The product-named host image links THIS worktree's real service endpoint and
# core, so the six endpoint entry points a JNI endpoint root needs are actually
# callable in an ordinary JVM. Three properties are deliberate:
#
#  - NO managed backend is compiled in. `moq_endpoint_connect()` therefore
#    reaches its real validation and its real "no managed facade in this build"
#    refusal without a socket, a DNS lookup, a thread or a peer -- which is what
#    makes the host oracle deterministic. Scripted roots come from the service's
#    own bare-endpoint seam, not from a stand-in transport.
#  - the sources are compiled from THIS worktree by path, so the image cannot
#    silently pick up a sibling checkout's libmoq.
#  - only the product-named image carries it. The handshake fixtures stay
#    exactly what they were, so their contract is unchanged by this link.
repo=$(cd "$root/../.." && pwd) || die "cannot resolve the repository root"
[ -f "$repo/service/src/endpoint.c" ] || die "no service/src/endpoint.c under $repo"

sub_dir="$out/substrate"
mkdir -p "$sub_dir" || die "cannot create $sub_dir"
SUBWARN="-Wall -Wextra -Wpedantic -Werror -std=c11 -fPIC -fvisibility=hidden"
sub_objs=""
sub_n=0

# compile_sub <source> [extra flags...]
compile_sub() {
    _cs=$1; shift
    _co="$sub_dir/$(printf '%s' "${_cs#"$repo/"}" | tr '/' '_').o"
    # shellcheck disable=SC2086  # SUBWARN must split into separate arguments
    cc -c $SUBWARN -I"$repo/core/include" -I"$repo/core/src"         -I"$repo/service/include" -I"$repo/service/src" "$@"         -o "$_co" "$_cs" || die "${_cs#"$repo/"} did not compile warning-free"
    sub_objs="$sub_objs $_co"
    sub_n=$((sub_n + 1))
}

for _c in $(find "$repo/core/src" -name '*.c' | sort); do
    compile_sub "$_c"
done
# The endpoint's scripted facade rides the service's own testing seam, which the
# Android build never defines -- so the seam cannot reach a shipped image.
#
# The INTERNAL trust route is compiled under a host-only name so the recorder
# in jni/testfixtures/moq5_trust_route_probe.c can define the product name,
# observe what the JNI hands it, and forward. The product translation unit is
# compiled unchanged and calls the product name; `moq_endpoint_connect()`
# inside endpoint.c calls the renamed route directly, so the ordinary row never
# passes through the recorder -- which is exactly what lets a row prove it
# does not.
compile_sub "$repo/service/src/endpoint.c" \
    -DMOQ_MEDIA_SENDER_TESTING=1 -DMOQ_MEDIA_RECEIVER_TESTING=1 \
    -Dmoq_endpoint_connect_with_trust=moq5_test_real_connect_with_trust
# The media RECEIVER, and the media tier it is built on (catalog, object,
# LOC, CMAF): the receiver rows execute the shipped C on the host too. The
# receiver is compiled with the service's own testing seam, which the Android
# build never defines, so the recorder can feed a catalog to a receiver with
# no peer and the REAL poll_track answers real discovery.
MEDIA_INCLUDES="-I$repo/media/msf/include -I$repo/media/object/include -I$repo/media/loc/include -I$repo/media/cmaf/include -I$repo/media/codec_signaling/include"
for _m in "$repo/media/msf/src/msf.c" "$repo/media/msf/src/msf_media.c" \
          "$repo/media/object/src/media_object.c" "$repo/media/loc/src/loc.c" \
          "$repo/media/cmaf/src/cmaf.c"; do
    compile_sub "$_m" $MEDIA_INCLUDES
done
compile_sub "$repo/service/src/media_receiver.c" $MEDIA_INCLUDES -DMOQ_MEDIA_RECEIVER_TESTING=1
# The media SENDER: the sender rows execute the shipped C on the host too, with
# no testing seam -- a live, unconnected root already answers every pre-ready
# path the rows pin.
compile_sub "$repo/service/src/media_sender.c" $MEDIA_INCLUDES
# The codec signaling utility, compiled WITHOUT any redirect: the recorder's
# forwarding names call it, so a redirect here would make forwarding recurse.
compile_sub "$repo/media/codec_signaling/src/codec_signaling.c" -I"$repo/media/codec_signaling/include"
compile_sub "$root/jni/testfixtures/moq5_trust_route_probe.c" \
    -I"$jni_include" -I"$jni_md" -I"$repo/service/include" $MEDIA_INCLUDES
# The product translation unit is compiled UNCHANGED, with six names redirected
# at compile time so the recorder can stand between the product and what it
# calls: the load entry (the recorder exports JNI_OnLoad and hands the product a
# forwarding JavaVM invocation table), the three pthread-key calls a per-thread
# cleanup registration and its release need (forwarded, with host-only fault
# arms), the
# registry reservation, the ordinary real constructor, and the scripted
# constructor's bare-endpoint seam (all counted and forwarded, the last two with
# one-shot fault arms). The trust route is redirected in endpoint.c above, not
# here. malloc/free in the product translation unit reach the recorder's
# allocation ledger, which forwards them untouched unless a codec row's own
# thread is armed. No product symbol or runtime branch is added; the image
# still exports exactly JNI_OnLoad.
PRODUCT_REDIRECTS="-DJNI_OnLoad=moq5_product_JNI_OnLoad \
-Dpthread_key_create=moq5_test_pthread_key_create \
-Dpthread_setspecific=moq5_test_pthread_setspecific \
-Dpthread_key_delete=moq5_test_pthread_key_delete \
-Dmoq5_reg_reserve_root=moq5_test_reg_reserve_root \
-Dmoq5_reg_insert_child=moq5_test_reg_insert_child \
-Dmoq5_reg_reserve_child=moq5_test_reg_reserve_child \
-Dmoq_endpoint_connect=moq5_test_endpoint_connect \
-Dmoq_endpoint_test_make_bare=moq5_test_endpoint_make_bare \
-Dmoq_media_object_cleanup=moq5_test_media_object_cleanup \
-Dmoq_media_sender_add_track=moq5_test_media_sender_add_track \
-Dmoq_codec_init_data_build=moq5_test_codec_init_data_build \
-Dmoq_codec_string_format=moq5_test_codec_string_format \
-Dmalloc=moq5_test_malloc \
-Dfree=moq5_test_free"
# The private handle registry is the same source the registry oracle already
# proves; the product image needs it because a published root is a token.
# MOQ5_REGISTRY_TESTING compiles the registry's already-private retirement hook,
# which is what lets a host row rendezvous at the exact post-invalidation
# milestone instead of sleeping. The Android build never defines it.
compile_sub "$root/jni/moq5_registry.c" -DMOQ5_REGISTRY_TESTING=1
printf '  compiled %s substrate object(s) from %s\n' "$sub_n" "$repo"

# The image must still export exactly one symbol. An archive's members carry
# default visibility, so hiding is asked of the LINKER, not of the compiler.
case $(uname -s) in
    Darwin)
        printf '_JNI_OnLoad\n' > "$sub_dir/exports.list" || die "cannot write the export list"
        SUB_LDFLAGS="-Wl,-exported_symbols_list,$sub_dir/exports.list"
        ;;
    *)
        printf '{ global: JNI_OnLoad; local: *; };\n' > "$sub_dir/exports.map" \
            || die "cannot write the export map"
        SUB_LDFLAGS="-Wl,--version-script=$sub_dir/exports.map"
        ;;
esac

# build <name> <source> [extra flags...]
build() {
    _name=$1
    _src=$2
    shift 2
    # shellcheck disable=SC2086  # WARN must split into separate arguments
    cc -shared -fPIC $WARN -I"$jni_include" -I"$jni_md" "$@" \
        -o "$out/lib$_name.$dso_ext" "$_src" \
        || die "$_name did not compile warning-free"
    [ -f "$out/lib$_name.$dso_ext" ] || die "$_name produced no shared object"
    printf '  built %s\n' "lib$_name.$dso_ext"
}

# OpenSSL and picotls, for the PRODUCT images and the trust fixture alike. The
# product's library-owned delegate verifies the reference identity with
# X509_check_host / X509_check_ip_asc and picotls' own IP-literal detector, so
# the host product image links libcrypto and picotls exactly as the Android
# image does. Discovery follows the same rule as the JNI headers above: an
# explicit override, then one documented default, then a NAMED failure.
ssl_prefix=""
if [ -n "${MOQ5_OPENSSL_PREFIX:-}" ]; then
    ssl_prefix=$MOQ5_OPENSSL_PREFIX
elif [ -d /opt/homebrew/opt/openssl@3 ]; then
    ssl_prefix=/opt/homebrew/opt/openssl@3
elif [ -d /usr/local/opt/openssl@3 ]; then
    ssl_prefix=/usr/local/opt/openssl@3
fi
[ -n "$ssl_prefix" ] || die \
    "no OpenSSL 3 prefix found. The trust-delegate fixture verifies a reference \
identity with X509_check_host, so it needs OpenSSL headers and libcrypto. Set \
MOQ5_OPENSSL_PREFIX to a prefix containing include/openssl/x509v3.h."
[ -f "$ssl_prefix/include/openssl/x509v3.h" ] \
    || die "no include/openssl/x509v3.h under $ssl_prefix"
# picotls is compiled from THIS worktree's pinned source, never from a sibling
# checkout: the same provenance rule the core substrate above follows. Four
# files is the whole of picotls-core plus its OpenSSL backend, so this needs no
# nested build system.
pt_src="$root/.native/_src/picotls"
[ -f "$pt_src/lib/openssl.c" ] || die \
    "no pinned picotls source at $pt_src: the trust wrapper needs picotls' own \
verifier to install verify_sign, which is file-static and cannot be reimplemented."
pt_objs=""
for _p in lib/picotls.c lib/pembase64.c lib/hpke.c lib/openssl.c; do
    _po="$sub_dir/picotls_$(basename "$_p").o"
    cc -c -Wall -Werror -std=c11 -fPIC -fvisibility=hidden \
        -I"$pt_src/include" -I"$ssl_prefix/include" \
        -o "$_po" "$pt_src/$_p" || die "picotls $_p did not compile warning-free"
    pt_objs="$pt_objs $_po"
done
# The product substrate, compiled from the SHIPPED source. MOQ5_JNI_HOST_TEST
# omits only the archive anchor table; the registration, preflight and native
# method implementations below it are compiled identically to the Android image.
# The core include root is supplied so the real semantic version header is used
# rather than a test-only substitute -- MOQ_VERSION_NUMBER is a compile-time
# macro, so this costs an include path and no link.
# THIS worktree's core headers, not a sibling checkout's.
#
# `service/src` is supplied as well: the internal trust route's header
# (`endpoint_trust.h`) is deliberately NOT installed and lives there, so the
# product translation unit can reach it only through this root.
#
# The path was `$root/../../../libmoq/core/include`, which leaves this worktree
# entirely and lands in the separate primary checkout. The two copies of
# moq/version.h happen to be byte-equal today, so the provenance defect passed
# by coincidence rather than by construction (chats/libmoq/1222 F2).
core_include=$(cd "$root/../../core/include" 2>/dev/null && pwd) || core_include=""
[ -n "$core_include" ] || die "cannot locate this worktree's core/include for <moq/version.h>"
# shellcheck disable=SC2086  # sub_objs/SUB_LDFLAGS must split into arguments
build bridge_product "$root/jni/moq5_jni.c" -DMOQ5_JNI_HOST_TEST $PRODUCT_REDIRECTS -I"$core_include" \
    -I"$repo/service/include" -I"$repo/service/src" $MEDIA_INCLUDES \
    -I"$ssl_prefix/include" -I"$pt_src/include" \
    -MMD -MF "$out/bridge_product.d" $sub_objs $pt_objs -L"$ssl_prefix/lib" -lcrypto $SUB_LDFLAGS

# The SAME product substrate under the PACKAGED library name, so the product
# entry point -- System.loadLibrary("moq5_jni") -- can be exercised on a host
# JVM by name rather than by path. This adds no production seam: the product
# code is unchanged and the host test simply points java.library.path at this
# directory, which is what an application's own library directory is on device.
# shellcheck disable=SC2086  # sub_objs/SUB_LDFLAGS must split into arguments
build moq5_jni "$root/jni/moq5_jni.c" -DMOQ5_JNI_HOST_TEST $PRODUCT_REDIRECTS -I"$core_include" \
    -I"$repo/service/include" -I"$repo/service/src" $MEDIA_INCLUDES \
    -I"$ssl_prefix/include" -I"$pt_src/include" \
    -MMD -MF "$out/moq5_jni.d" $sub_objs $pt_objs -L"$ssl_prefix/lib" -lcrypto $SUB_LDFLAGS

# The registry round-trip fixture: a test-only bridge that links the PRODUCTION
# registry source and carries a real token across JNI. The full-width rule is a
# claim about an actual crossing, and a C-side cast cannot test it.
build registry_roundtrip "$root/jni/testfixtures/moq5_registry_jni_fixture.c" \
    "$root/jni/moq5_registry.c" -fvisibility=hidden

# The TRUST fixture: TWO layers, both driven without a handshake.
#
#  1. the decision core, which takes a DER chain and a server name as data;
#  2. the picotls WRAPPER, a real ptls_verify_certificate_t whose callback the
#     rows invoke directly.
#
# It therefore needs OpenSSL AND picotls -- but still no picoquic, no socket and
# no peer. Discovery
# follows the same rule as the JNI headers above: an explicit override, then one
# documented default, then a NAMED failure. It is never silently skipped: a
# fixture that quietly does not build is a row that quietly does not run.
# shellcheck disable=SC2086  # pt_objs must split into separate arguments
build trust_delegate "$root/jni/testfixtures/moq5_trust_jni_fixture.c" \
    -fvisibility=hidden -I"$ssl_prefix/include" -I"$pt_src/include" \
    $pt_objs -L"$ssl_prefix/lib" -lcrypto

# PROVENANCE, proven by the compiler rather than by the path this script meant
# to pass. The depfile records what was actually included, so an include root
# pointing outside this worktree is caught even when the header bytes match.
# The expectation is derived from THIS WORKTREE, independently of the include
# root the build happened to pass. Comparing the depfile against `$core_include`
# only proved the two agreed: repointing that variable at a sibling checkout
# moved both sides together and the check passed.
_wt_root=$(cd "$root/../.." && pwd) || die "cannot resolve the worktree root"
_want_version_h="$_wt_root/core/include/moq/version.h"
for _dep in "$out/bridge_product.d" "$out/moq5_jni.d"; do
    [ -f "$_dep" ] || die "no depfile at $_dep; the version provenance is unproven"
    # awk splits on whitespace, so a depfile's continuation backslashes become
    # their own fields and every include path lands on its own line.
    _seen=$(awk '{ for (i = 1; i <= NF; i++) print $i }' "$_dep" |
        grep -E '/moq/version\.h$' | sort -u)
    [ -n "$_seen" ] || die "$_dep records no moq/version.h at all"
    for _v in $_seen; do
        [ "$_v" = "$_want_version_h" ] || die \
            "$_dep included $_v, which is not this worktree's $_want_version_h"
    done
done
printf '  version provenance: both host objects included %s\n' "$core_include/moq/version.h"

fx="$root/jni/testfixtures/moq5_bridge_fixture.c"
build bridge_compatible      "$fx"
# Stated RELATIVE to the authority, not as literals: an epoch bump would
# otherwise turn "high" into the compatible value and quietly retire the row.
build bridge_epoch_high      "$fx" '-DMOQ5_FIXTURE_EPOCH=(MOQ5_ABI_EPOCH + 1)'
build bridge_epoch_low       "$fx" '-DMOQ5_FIXTURE_EPOCH=(MOQ5_ABI_EPOCH - 1)'
build bridge_features_absent "$fx" -DMOQ5_FIXTURE_FEATURES=0u
build bridge_features_extra  "$fx" '-DMOQ5_FIXTURE_FEATURES=(MOQ5_FEATURES_PROVIDED|0x8000ULL)'
# An additive bit ABOVE 2^32, so a 32-bit transport truncation is visible.
build bridge_features_wide   "$fx" '-DMOQ5_FIXTURE_FEATURES=(MOQ5_FEATURES_PROVIDED|0x4000000000ULL)'
build bridge_omit_one        "$fx" -DMOQ5_FIXTURE_EMIT_nativeFeatureBits=0
build bridge_bad_descriptor  "$fx" '-DMOQ5_FIXTURE_DESC_nativeFeatureBits(d)="()I"'
build bridge_registration_fails "$fx" -DMOQ5_FIXTURE_REGISTRATION_FAILS

# No variant may share the compatible fixture's CONFIGURATION. Object bytes
# differ incidentally even between identical builds, so the comparison is on the
# signature each object embeds: a selection flag that was renamed or mistyped
# yields the compatible signature and is refused here rather than silently
# turning an adversarial control into a duplicate of the positive case.
signature_of() {
    strings -a "$1" | sed -n 's/^\(moq5-fixture-signature .*\)$/\1/p' | sed -n '1p'
}
_ref=$(signature_of "$out/libbridge_compatible.$dso_ext") \
    || die "cannot read the compatible fixture signature"
[ -n "$_ref" ] || die "the compatible fixture embeds no signature"
for _v in epoch_high epoch_low features_absent features_extra features_wide \
          omit_one bad_descriptor registration_fails; do
    _s=$(signature_of "$out/libbridge_$_v.$dso_ext") || die "cannot read $_v signature"
    [ -n "$_s" ] || die "variant $_v embeds no signature"
    [ "$_s" != "$_ref" ] \
        || die "variant $_v has the COMPATIBLE configuration ($_s); its selection \
flag did not take effect"
done
printf '  %s\n' "every variant's configuration differs from the compatible fixture"

# The producer receipt. A gate that judges one of these images must be able to
# bind it to the build that wrote it; without this a stale image from an earlier
# run is indistinguishable from a current one.
# Hashing goes through the shared fail-closed helper: a hasher that prints a
# valid digest and then exits nonzero must not be able to write a receipt.
_srch=$(moq_hash_file "$root/jni/moq5_jni.c" "$out") \
    || die "cannot hash the shipped jni/moq5_jni.c"
_rcpt="$out/bridge.provenance"
: > "$_rcpt" || die "cannot start the bridge provenance"
{
    printf 'producer=%s\n' "$_self"
    printf 'cc=%s\n' "$(command -v cc)"
    printf 'jni.include=%s\n' "$jni_include"
    printf 'openssl.prefix=%s\n' "$ssl_prefix"
    printf 'source.moq5_jni.c.sha256=%s\n' "$_srch"
} >> "$_rcpt" || die "cannot write the bridge provenance"
for _img in "$out"/lib*."$dso_ext"; do
    [ -f "$_img" ] || die "no shared objects to record"
    _ih=$(moq_hash_file "$_img" "$out") || die "cannot hash $_img"
    printf 'image.%s.sha256=%s\n' "${_img##*/}" "$_ih" >> "$_rcpt" \
        || die "cannot record $_img"
done
printf '  recorded %s\n' "${_rcpt##*/}"

printf '%s\n' "$out" > "$out/.built" || die "cannot record the output directory"
printf 'bridge test DSOs: built with JNI headers from %s\n' "$jni_include"
exit 0
