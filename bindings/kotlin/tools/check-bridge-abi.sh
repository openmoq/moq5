#!/bin/sh
# The bridge identity must agree across every representation of it.
#
# The authority is jni/moq5_abi.h. What has to match it is not Kotlin SOURCE but
# the artefacts that actually decide behaviour:
#
#   * the COMPILED bytecode, read with javap -- a source regex cannot see the
#     descriptor, the native flag, or whether a method is static, and an earlier
#     revision of this gate passed while the consumer rules named a shape the
#     bytecode never had;
#   * the consumer keep-rules, which must name that same shape;
#   * the BUILT shared objects' symbol inventories, so a Java_* export is caught
#     in the artefact rather than by a single-line source grep.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
case $0 in
    */*) _selfdir=${0%/*} ;;
    *)   _selfdir=. ;;
esac
here=$(cd "$_selfdir" && pwd) || { echo "$_self: cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd)  || { echo "$_self: cannot resolve bindings/kotlin" >&2; exit 2; }

for t in grep sed sort printf tr find mktemp rm; do
    command -v "$t" >/dev/null 2>&1 \
        || { echo "$_self: required tool not found: $t" >&2; exit 2; }
done

scratch=$(mktemp -d "${TMPDIR:-/tmp}/moq5-bridge-abi-XXXXXXXX") \
    || { echo "$_self: cannot create a task-owned scratch" >&2; exit 2; }
# shellcheck disable=SC2329  # invoked from the traps below
cleanup() { rm -rf -- "$scratch"; }
trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

abi_h="$root/jni/moq5_abi.h"
bridge_kt="$root/moq5-android/src/main/kotlin/org/openmoq/moq5/jni/NativeBridge.kt"
rules="$root/moq5-android/consumer-rules.pro"
for f in "$abi_h" "$bridge_kt" "$rules"; do
    [ -f "$f" ] || { echo "$_self: missing $f" >&2; exit 2; }
done

fails=0
pass() { printf 'PASS[%s]: %s\n' "$1" "$2"; }
fail() { fails=$((fails + 1)); printf 'FAIL[%s]: %s\n' "$1" "$2"; }

c_define() {
    _c=$(grep -c "^#define $1 " "$abi_h") || return 1
    [ "$_c" = "1" ] || return 1
    sed -n "s/^#define $1 \\(.*\\)\$/\\1/p" "$abi_h"
}
kt_const() {
    _c=$(grep -c "const val $1" "$bridge_kt") || return 1
    [ "$_c" = "1" ] || return 1
    sed -n "s/.*const val $1[^=]*= *\\([^ ]*\\).*/\\1/p" "$bridge_kt"
}
# Normalise a numeric literal to a comparable value: drop casts, parentheses and
# width suffixes, and resolve the one named bit to its number. Both sides define
# their masks in terms of that symbol, so comparing the raw text would compare
# spellings rather than values.
norm_num() {
    printf '%s' "$1" |
        tr -d ' ()' |
        sed "s/unsignedlonglong//g; s/MOQ5_FEATURE_BRIDGE_HANDSHAKE/$2/g; \
             s/FEATURE_BRIDGE_HANDSHAKE/$2/g; s/[uUlL]*\$//"
}

# -- the ABI epoch, and its independence from the semantic version -------------
c_epoch=$(c_define MOQ5_ABI_EPOCH) || c_epoch=""
kt_epoch=$(kt_const ABI_EPOCH) || kt_epoch=""
if [ -z "$c_epoch" ] || [ -z "$kt_epoch" ]; then
    fail bridge.epoch "the ABI epoch is not declared exactly once on both sides"
elif [ "$c_epoch" != "$kt_epoch" ]; then
    fail bridge.epoch "authority declares $c_epoch but Kotlin declares $kt_epoch"
else
    pass bridge.epoch "both sides declare ABI epoch $c_epoch"
fi
if printf '%s' "$c_epoch" | grep -qE '^[0-9]+$'; then
    if grep -E '^#define ' "$abi_h" | grep -q 'MOQ_VERSION'; then
        fail bridge.epoch.independent "a #define derives the epoch from MOQ_VERSION_*"
    else
        pass bridge.epoch.independent "the epoch is a literal ($c_epoch), not the library version"
    fi
else
    fail bridge.epoch.independent "the epoch is not a literal integer but '$c_epoch'"
fi

# -- the JNI version the bridge requires ---------------------------------------
c_jni=$(c_define MOQ5_JNI_VERSION) || c_jni=""
if [ "$c_jni" = "JNI_VERSION_1_6" ]; then
    pass bridge.jni-version "the authority requires $c_jni"
else
    fail bridge.jni-version "the authority declares MOQ5_JNI_VERSION='$c_jni', expected JNI_VERSION_1_6"
fi

# -- the feature vocabulary, all three masks ----------------------------------
c_feat=$(norm_num "$(c_define MOQ5_FEATURE_BRIDGE_HANDSHAKE)" x)
kt_feat=$(norm_num "$(kt_const FEATURE_BRIDGE_HANDSHAKE)" x)
c_req=$(norm_num "$(c_define MOQ5_FEATURES_REQUIRED)" "$c_feat")
kt_req=$(norm_num "$(kt_const FEATURES_REQUIRED)" "$kt_feat")
c_known=$(norm_num "$(c_define MOQ5_FEATURES_KNOWN)" "$c_feat")
kt_known=$(norm_num "$(kt_const FEATURES_KNOWN)" "$kt_feat")
c_prov=$(norm_num "$(c_define MOQ5_FEATURES_PROVIDED)" "$c_feat")
if [ -z "$c_feat" ] || [ "$c_feat" != "$kt_feat" ]; then
    fail bridge.feature.bit "handshake bit: authority '$c_feat' vs Kotlin '$kt_feat'"
else
    pass bridge.feature.bit "both sides declare the handshake bit $c_feat"
fi
if [ -z "$c_req" ] || [ "$c_req" != "$kt_req" ]; then
    fail bridge.feature.required "REQUIRED: authority '$c_req' vs Kotlin '$kt_req'"
else
    pass bridge.feature.required "both sides require $c_req"
fi
if [ -z "$c_known" ] || [ "$c_known" != "$kt_known" ]; then
    fail bridge.feature.known "KNOWN: authority '$c_known' vs Kotlin '$kt_known'"
else
    pass bridge.feature.known "both sides know $c_known"
fi
if [ -z "$c_prov" ] || [ "$c_prov" != "$c_req" ]; then
    fail bridge.feature.provided "PROVIDED '$c_prov' does not cover REQUIRED '$c_req'"
else
    pass bridge.feature.provided "the native side provides everything required ($c_prov)"
fi

# -- the transport is 64-bit on both sides -------------------------------------
if grep -q 'X(nativeFeatureBits, "()J")' "$abi_h" && grep -q 'FEATURE_BRIDGE_HANDSHAKE: Long' "$bridge_kt"; then
    if grep -q 'ULL' "$abi_h"; then
        pass bridge.feature.width "the feature transport is 64-bit on both sides"
    else
        fail bridge.feature.width "the authority's feature constants are not explicitly 64-bit"
    fi
else
    fail bridge.feature.width "the feature transport is not declared 64-bit on both sides"
fi

# -- the bound class -----------------------------------------------------------
c_class=$(sed -n 's/^#define MOQ5_BRIDGE_CLASS "\(.*\)"$/\1/p' "$abi_h")
kt_pkg=$(sed -n 's/^package \(.*\)$/\1/p' "$bridge_kt" | tr '.' '/')
want="$kt_pkg/NativeBridge"
if [ "$c_class" != "$want" ]; then
    fail bridge.class "authority binds '$c_class' but the Kotlin object is '$want'"
else
    pass bridge.class "both sides name $c_class"
fi
dotted=$(printf '%s' "$c_class" | tr '/' '.')

# -- the authority table -------------------------------------------------------
sed -n 's/^ *X(\([A-Za-z0-9_]*\), *"\([^"]*\)").*$/\1 \2/p' "$abi_h" | sort > "$scratch/authority" \
    || { echo "$_self: cannot read the authority table" >&2; exit 2; }
if [ ! -s "$scratch/authority" ]; then
    fail bridge.methods "the authority declares no method table"
    printf '\nbridge ABI authority: %s failure(s)\n' "$((fails + 1))"
    exit 1
fi
rows=$(grep -c . "$scratch/authority")

# The authority's chosen invocation shape, derived ONCE and independently of
# whether any bytecode is available. It is read by the consumer-rule oracle too,
# so deriving it inside the bytecode branch left it unset -- and `set -u` turned
# a missing artefact into a shell diagnostic instead of a named gate failure
# (chats/libmoq/1216 F2).
want_static=$(c_define MOQ5_BRIDGE_METHOD_STATIC) || want_static=""
case $want_static in
    0) want_shape=instance ;;
    1) want_shape=static ;;
    *) want_shape="" ;;
esac

# -- the COMPILED bytecode -----------------------------------------------------
javap=""
if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/javap" ]; then
    javap="$JAVA_HOME/bin/javap"
elif command -v javap >/dev/null 2>&1; then
    javap=$(command -v javap)
fi
# The class artefacts are SUPPLIED under NAMED producer entries, never
# discovered and never flattened into one string.
#
# Discovery was unsound: match order is a filesystem property, and the build
# directory holds outputs of tasks this gate does not depend on. A free-form
# list was unsound too, in two further ways: it could not express WHICH
# producer an artefact came from -- so dropping the release producer silently
# turned a two-variant contract into a one-variant gate -- and iterating it
# unquoted split any path containing whitespace into nonexistent paths
# (chats/libmoq/1216 F1, F2).
#
# Each producer therefore has its own variable holding exactly one path. Both
# are REQUIRED; there is no fallback and no inference from a count, since two
# copies of one artefact would also count two.
_dbg=${MOQ5_BRIDGE_CLASS_DEBUG:-}
_rel=${MOQ5_BRIDGE_CLASS_RELEASE:-}
_have_bytecode=0
if [ -z "$javap" ]; then
    fail bridge.bytecode "no javap; the JVM side must be judged on bytecode, never on Kotlin source text"
elif [ -z "$_dbg" ] || [ -z "$_rel" ]; then
    _absent_producers=""
    [ -n "$_dbg" ] || _absent_producers="$_absent_producers compileDebugKotlin(MOQ5_BRIDGE_CLASS_DEBUG)"
    [ -n "$_rel" ] || _absent_producers="$_absent_producers compileReleaseKotlin(MOQ5_BRIDGE_CLASS_RELEASE)"
    fail bridge.bytecode.producers "required producer artefact(s) not supplied:$_absent_producers"
elif [ ! -f "$_dbg" ] || [ ! -f "$_rel" ]; then
    _absent_files=""
    [ -f "$_dbg" ] || _absent_files="$_absent_files compileDebugKotlin:$_dbg"
    [ -f "$_rel" ] || _absent_files="$_absent_files compileReleaseKotlin:$_rel"
    fail bridge.bytecode.producers "supplied producer artefact(s) do not exist:$_absent_files"
elif [ ! -r "$_dbg" ] || [ ! -r "$_rel" ]; then
    # Readability is preflighted HERE, before any producer PASS. Discovering it
    # later, at the javap call, produced a tool diagnostic and an abrupt exit
    # instead of a named gate failure and a summary (chats/libmoq/1218 F2).
    _unreadable=""
    [ -r "$_dbg" ] || _unreadable="$_unreadable compileDebugKotlin:$_dbg"
    [ -r "$_rel" ] || _unreadable="$_unreadable compileReleaseKotlin:$_rel"
    fail bridge.bytecode.producers "supplied producer artefact(s) are not readable:$_unreadable"
else
    # The two entries must be two DIFFERENT FILES, compared by identity rather
    # than by path spelling. Two producer-shaped symlinks to one compiled class
    # have distinct absolute paths and pass every method oracle, which is the
    # same false pass a shared path would give (chats/libmoq/1218 F1).
    #
    # Byte equality is deliberately NOT used: debug and release may legitimately
    # compile to identical bytes, so equal content proves nothing about
    # provenance. Provenance is the Gradle providers' job; this check exists
    # only to reject two aliases of one actual file.
    _fid() {
        stat -L -f '%d:%i' "$1" 2>/dev/null && return 0
        stat -L -c '%d:%i' "$1" 2>/dev/null && return 0
        return 1
    }
    _dbg_id=$(_fid "$_dbg") || _dbg_id=""
    _rel_id=$(_fid "$_rel") || _rel_id=""
    _dbg_real=$(cd "$(dirname "$_dbg")" && printf '%s/%s' "$(pwd)" "$(basename "$_dbg")")
    _rel_real=$(cd "$(dirname "$_rel")" && printf '%s/%s' "$(pwd)" "$(basename "$_rel")")
    if [ -z "$_dbg_id" ] || [ -z "$_rel_id" ]; then
        # Fail closed: without file identity the alias case cannot be excluded.
        fail bridge.bytecode.producers \
            "cannot determine file identity (no usable stat); the gate refuses to \
judge artefacts it cannot prove are two different files"
    elif [ "$_dbg_id" = "$_rel_id" ]; then
        fail bridge.bytecode.producers \
            "both producer entries resolve to the SAME file (id $_dbg_id): \
$_dbg_real and $_rel_real; the debug and release variants must each supply their own"
    else
        # Corroboration, not the identity itself: the identity is the NAME of
        # the entry the caller used. This catches the two paths being swapped or
        # one producer's output being passed under the other's name.
        _mis=""
        case $_dbg_real in *"/compileDebugKotlin/"*) ;; *) _mis="$_mis MOQ5_BRIDGE_CLASS_DEBUG=$_dbg_real" ;; esac
        case $_rel_real in *"/compileReleaseKotlin/"*) ;; *) _mis="$_mis MOQ5_BRIDGE_CLASS_RELEASE=$_rel_real" ;; esac
        if [ -n "$_mis" ]; then
            fail bridge.bytecode.producers \
                "artefact(s) not produced by the named producer:$_mis"
        else
            _have_bytecode=1
            pass bridge.bytecode.producers \
                "both required producers supplied exactly one readable, distinct \
artefact (judged below, per producer)"
        fi
    fi
fi
if [ "$_have_bytecode" = "1" ]; then
  for _entry in debug release; do
    if [ "$_entry" = "debug" ]; then
        cls=$_dbg
        _label=compileDebugKotlin
    else
        cls=$_rel
        _label=compileReleaseKotlin
    fi
    # A javap failure is attributed to ITS producer and skips only that
    # producer's bytecode-dependent claims. Exiting here bypassed the final
    # summary and emitted a tool diagnostic instead of a named failure
    # (chats/libmoq/1218 F2). A readable but malformed class lands here too.
    if ! "$javap" -p -s "$cls" > "$scratch/javap" 2>/dev/null; then
        fail bridge.bytecode.readable \
            "[$_label] javap could not read $cls; no bytecode claim is made for \
this producer"
        continue
    fi
    # name descriptor static modifiers  -- for every NATIVE method.
    # The modifier set is captured in javap's own order and compared EXACTLY
    # later: reporting the observed shape while asserting only part of it is
    # what let a `public final native` contract rest on a `native` membership
    # test (chats/libmoq/1212 F3).
    awk '
        /native/ { name=$0; sub(/\(.*/,"",name); sub(/.* /,"",name);
                   st = (index($0,"static")>0) ? "static" : "instance";
                   mods = "";
                   for (i = 1; i <= NF; i++) {
                       t = $i;
                       if (t == "public" || t == "protected" || t == "private" ||
                           t == "static" || t == "final" || t == "native" ||
                           t == "abstract" || t == "synchronized" || t == "strictfp")
                           mods = (mods == "") ? t : mods ":" t;
                   }
                   getline d; sub(/.*descriptor: /,"",d);
                   print name, d, st, mods }
    ' "$scratch/javap" | sort > "$scratch/bytecode"
    cut -d' ' -f1,2 "$scratch/bytecode" | sort > "$scratch/bytecode_nd"

    if ! diff -q "$scratch/authority" "$scratch/bytecode_nd" >/dev/null 2>&1; then
        _extra=$(comm -13 "$scratch/authority" "$scratch/bytecode_nd" | tr '\n' ';')
        _absent=$(comm -23 "$scratch/authority" "$scratch/bytecode_nd" | tr '\n' ';')
        fail bridge.methods \
            "[$_label] bytecode and authority disagree -- only in bytecode: [$_extra] only in authority: [$_absent]"
    else
        pass bridge.methods "[$_label] $rows native method(s) match the authority by name AND descriptor"
    fi

    _dupes=$(cut -d' ' -f1 "$scratch/bytecode" | sort | uniq -d | tr '\n' ' ')
    if [ -n "$_dupes" ]; then
        fail bridge.methods.unique "[$_label] duplicate native method name(s): $_dupes"
    else
        pass bridge.methods.unique "[$_label] no duplicate native method name"
    fi

    if [ -z "$want_shape" ]; then
        fail bridge.methods.shape "the authority does not declare MOQ5_BRIDGE_METHOD_STATIC as 0 or 1"
    else
        _wrong=$(awk -v w="$want_shape" '$3 != w { printf "%s(%s) ", $1, $3 }' "$scratch/bytecode")
        if [ -n "$_wrong" ]; then
            fail bridge.methods.shape "[$_label] the authority chose $want_shape methods; bytecode has: $_wrong"
        else
            pass bridge.methods.shape "[$_label] every bound method is $want_shape, as the authority chose"
        fi

        # The COMPLETE declared shape, not merely its native and static parts.
        # This is the shape the shipped consumer rule names, so the rule and the
        # bytecode are held to one statement.
        if [ "$want_shape" = "static" ]; then
            _want_mods="public:static:final:native"
        else
            _want_mods="public:final:native"
        fi
        _badmods=$(awk -v w="$_want_mods" '$4 != w { printf "%s[%s] ", $1, $4 }' "$scratch/bytecode")
        if [ -n "$_badmods" ]; then
            fail bridge.methods.access \
                "[$_label] every bound method must be exactly '$_want_mods'; bytecode has: $_badmods"
        else
            pass bridge.methods.access "[$_label] every bound method is exactly $_want_mods"
        fi
    fi

    # The Kotlin `object` singleton's INSTANCE field: a call to an instance
    # method goes through it, and the shipped consumer rule keeps it by name and
    # type. If the authority ever chooses static methods the field is no longer
    # on the call path, so the requirement follows that choice.
    if [ "$want_shape" = "instance" ]; then
        _fld=$(awk '
            /[ \t]INSTANCE;/ { line=$0; getline d; sub(/.*descriptor: /,"",d);
                               mods="";
                               for (i = 1; i <= NF; i++) {
                                   t = $i;
                                   if (t == "public" || t == "static" || t == "final")
                                       mods = (mods == "") ? t : mods ":" t;
                               }
                               print mods, d }
        ' "$scratch/javap" | sed -n '1p')
        if [ "$_fld" = "public:static:final L$c_class;" ]; then
            pass bridge.instance-field "[$_label] INSTANCE is public static final L$c_class;"
        else
            fail bridge.instance-field \
                "[$_label] the object singleton's INSTANCE field is '$_fld', expected 'public:static:final L$c_class;'"
        fi
    fi
  done
fi

# -- the consumer keep-rules name that same shape ------------------------------
if grep -q -- "-keep.*class $dotted" "$rules"; then
    _bad=""
    while read -r m d; do
        [ -n "$m" ] || continue
        grep -q "native .*$m(" "$rules" || _bad="$_bad $m"
    done < "$scratch/authority"
    _shape_bad=""
    if [ -s "$scratch/bytecode" ]; then
        if [ "$want_shape" = "instance" ] && grep -qE 'static native' "$rules"; then
            _shape_bad="the rules keep STATIC natives but the bytecode has instance methods"
        fi
        if [ "$want_shape" = "static" ] && grep -qE 'final native' "$rules"; then
            _shape_bad="the rules keep instance natives but the bytecode has static methods"
        fi
    fi
    if [ -n "$_bad" ]; then
        fail bridge.consumer-rules "the consumer rules do not keep:$_bad"
    elif [ -n "$_shape_bad" ]; then
        fail bridge.consumer-rules "$_shape_bad"
    else
        pass bridge.consumer-rules "the rules keep $dotted and all $rows method(s), $want_shape"
    fi
else
    fail bridge.consumer-rules "the consumer rules do not keep the bound class $dotted"
fi

# -- no Java_* export, judged on SOURCE across line breaks and on ARTEFACTS ----
find "$root/jni" -name '*.c' -o -name '*.h' | while read -r f; do
    tr '\n' ' ' < "$f"
    printf '\n'
done > "$scratch/joined" 2>/dev/null
if grep -qE 'JNIEXPORT[^;{]*Java_' "$scratch/joined"; then
    fail bridge.no-java-exports.source "a Java_* export is declared in the JNI sources"
else
    pass bridge.no-java-exports.source "no Java_* export in the JNI sources (line breaks folded)"
fi

_dsos=$(find "$root/.native" -name 'libbridge_*.dylib' -o -name 'libbridge_*.so' 2>/dev/null)
if [ -z "$_dsos" ]; then
    fail bridge.exports.built "no built bridge object to inspect; run tools/build-bridge-testdsos.sh"
else
    _bad=""
    for d in $_dsos; do
        _syms=$(nm -gU "$d" 2>/dev/null | awk '{print $3}' | sed 's/^_//' | grep -v '^$' | sort | tr '\n' ' ')
        [ "$_syms" = "JNI_OnLoad " ] || _bad="$_bad ${d##*/}:[$_syms]"
    done
    if [ -n "$_bad" ]; then
        fail bridge.exports.built "a built object exports more than JNI_OnLoad:$_bad"
    else
        pass bridge.exports.built "every built bridge object exports exactly JNI_OnLoad"
    fi
fi

# -- the TRUSTED-CREATE contract (ruling 1365) ---------------------------------
# The frozen contract, checked against the SAME authority table and epoch this
# file already parses, so there is one grammar rather than a second copy.
#
#   nativeEndpointCreateTrusted(String url, int trustPolicy, int versionPolicy,
#                               int[] versions, long handshakeMicros, int protocol,
#                               String webTransportPath, long[] out) -> int
#   descriptor: (Ljava/lang/String;II[IJILjava/lang/String;[J)I   (the endpoint
#                configuration -- version offer, handshake bound, transport
#                selection -- rides the one create row)
#   ABI epoch 4 or later, moved on BOTH sides together
#
# The rows above prove the two sides AGREE; these prove they agree on the
# CONTRACT. A consistent ten-row epoch-3 bridge satisfies every check above and
# must still fail here until the row exists.
trusted_name=nativeEndpointCreateTrusted
trusted_desc='(Ljava/lang/String;II[IJILjava/lang/String;[J)I'
ordinary_desc='(Ljava/lang/String;[J)I'

# The row, by EXACT name and EXACT descriptor. Counting rows would accept a
# same-count table with a misspelled name or a wrong descriptor.
_n_named=$(grep -c "^$trusted_name " "$scratch/authority")
if [ "$_n_named" -eq 0 ]; then
    fail bridge.trusted.row \
        "the authority declares no '$trusted_name' row; the trusted-create contract is absent"
elif [ "$_n_named" -gt 1 ]; then
    fail bridge.trusted.row \
        "the authority declares '$trusted_name' $_n_named times; exactly one row is required"
else
    _got=$(sed -n "s/^$trusted_name //p" "$scratch/authority")
    if [ "$_got" != "$trusted_desc" ]; then
        fail bridge.trusted.descriptor \
            "'$trusted_name' is declared '$_got', the contract is '$trusted_desc'"
    else
        pass bridge.trusted.descriptor "'$trusted_name' carries $trusted_desc"
    fi
fi

# The ordinary row must be behaviourally unchanged: same name, same descriptor.
_n_ord=$(grep -c "^nativeEndpointCreate " "$scratch/authority")
if [ "$_n_ord" -ne 1 ]; then
    fail bridge.trusted.ordinary-row \
        "expected exactly one 'nativeEndpointCreate' row, found $_n_ord"
else
    _got_ord=$(sed -n 's/^nativeEndpointCreate //p' "$scratch/authority")
    if [ "$_got_ord" != "$ordinary_desc" ]; then
        fail bridge.trusted.ordinary-row \
            "'nativeEndpointCreate' is declared '$_got_ord', expected '$_got_ord' to remain '$ordinary_desc'"
    else
        pass bridge.trusted.ordinary-row "'nativeEndpointCreate' still carries $ordinary_desc"
    fi
fi

# The epoch this contract belongs to. `bridge.epoch` above already fails a
# ONE-SIDED move; this additionally requires the agreed value to be at least
# 4, so a consistent epoch-3 pair cannot satisfy the contract. Later rows move
# the epoch further and carry their own exact check.
if [ -n "$c_epoch" ] && [ -n "$kt_epoch" ]; then
    if ! printf '%s' "$c_epoch" | grep -qE '^[0-9]+$' || [ "$c_epoch" -lt 4 ] || [ "$kt_epoch" != "$c_epoch" ]; then
        fail bridge.trusted.epoch \
            "the trusted-create row is an ABI change: both sides must declare epoch 4 or later, found C=$c_epoch Kotlin=$kt_epoch"
    else
        pass bridge.trusted.epoch "both sides declare ABI epoch $c_epoch (the trusted row needs 4 or later)"
    fi
fi

# -- the INTERRUPT-LATCH contract ---------------------------------------------
# `moq_endpoint_set_interrupted()` is any-thread and void, so its row is leased
# like `wake` and answers only MOQ_OK or a registry refusal:
#
#   nativeEndpointSetInterrupted(long token, boolean interrupted) -> int
#   descriptor: (JZ)I
#   ABI epoch 5 or later, moved on BOTH sides together
latch_name=nativeEndpointSetInterrupted
latch_desc='(JZ)I'
_n_latch=$(grep -c "^$latch_name " "$scratch/authority")
if [ "$_n_latch" -ne 1 ]; then
    fail bridge.latch.row \
        "the authority declares '$latch_name' $_n_latch time(s); exactly one row is required"
else
    _got_latch=$(sed -n "s/^$latch_name //p" "$scratch/authority")
    if [ "$_got_latch" != "$latch_desc" ]; then
        fail bridge.latch.descriptor \
            "'$latch_name' is declared '$_got_latch', the contract is '$latch_desc'"
    else
        pass bridge.latch.descriptor "'$latch_name' carries $latch_desc"
    fi
fi
if [ -n "$c_epoch" ] && [ -n "$kt_epoch" ]; then
    if ! printf '%s' "$c_epoch" | grep -qE '^[0-9]+$' || [ "$c_epoch" -lt 5 ] || [ "$kt_epoch" != "$c_epoch" ]; then
        fail bridge.latch.epoch \
            "the latch row is an ABI change: both sides must declare epoch 5 or later, found C=$c_epoch Kotlin=$kt_epoch"
    else
        pass bridge.latch.epoch "both sides declare ABI epoch $c_epoch (the latch row needs 5 or later)"
    fi
fi

# -- the DRAIN contract --------------------------------------------------------
# `moq_endpoint_drain()` is any-thread for the application and blocking, so its
# row is leased like `wake` and carries the C result verbatim:
#
#   nativeEndpointDrain(long token, long timeoutMicros) -> int
#   descriptor: (JJ)I
#   ABI epoch 6 or later, moved on BOTH sides together
drain_name=nativeEndpointDrain
drain_desc='(JJ)I'
_n_drain=$(grep -c "^$drain_name " "$scratch/authority")
if [ "$_n_drain" -ne 1 ]; then
    fail bridge.drain.row \
        "the authority declares '$drain_name' $_n_drain time(s); exactly one row is required"
else
    _got_drain=$(sed -n "s/^$drain_name //p" "$scratch/authority")
    if [ "$_got_drain" != "$drain_desc" ]; then
        fail bridge.drain.descriptor \
            "'$drain_name' is declared '$_got_drain', the contract is '$drain_desc'"
    else
        pass bridge.drain.descriptor "'$drain_name' carries $drain_desc"
    fi
fi
if [ -n "$c_epoch" ] && [ -n "$kt_epoch" ]; then
    if ! printf '%s' "$c_epoch" | grep -qE '^[0-9]+$' || [ "$c_epoch" -lt 6 ] || [ "$kt_epoch" != "$c_epoch" ]; then
        fail bridge.drain.epoch \
            "the drain row is an ABI change: both sides must declare epoch 6 or later, found C=$c_epoch Kotlin=$kt_epoch"
    else
        pass bridge.drain.epoch "both sides declare ABI epoch $c_epoch (the drain row needs 6 or later)"
    fi
fi

# -- the CONFIGURED create contract --------------------------------------------
# The trusted create row carries the endpoint configuration (version offer,
# handshake bound); the descriptor above is that shape, and it moved the epoch:
#
#   ABI epoch 7 or later, moved on BOTH sides together
if [ -n "$c_epoch" ] && [ -n "$kt_epoch" ]; then
    if ! printf '%s' "$c_epoch" | grep -qE '^[0-9]+$' || [ "$c_epoch" -lt 7 ] || [ "$kt_epoch" != "$c_epoch" ]; then
        fail bridge.config.epoch \
            "the configured create row is an ABI change: both sides must declare epoch 7 or later, found C=$c_epoch Kotlin=$kt_epoch"
    else
        pass bridge.config.epoch "both sides declare ABI epoch $c_epoch (the configured row needs 7 or later)"
    fi
fi

# -- the TRANSPORT-SELECTION contract ------------------------------------------
# The trusted create row also carries the protocol override and the
# WebTransport CONNECT request target; the descriptor above is that shape:
#
#   ABI epoch 8 or later, moved on BOTH sides together
if [ -n "$c_epoch" ] && [ -n "$kt_epoch" ]; then
    if ! printf '%s' "$c_epoch" | grep -qE '^[0-9]+$' || [ "$c_epoch" -lt 8 ] || [ "$kt_epoch" != "$c_epoch" ]; then
        fail bridge.transport.epoch \
            "the transport-selection row is an ABI change: both sides must declare epoch 8 or later, found C=$c_epoch Kotlin=$kt_epoch"
    else
        pass bridge.transport.epoch "both sides declare ABI epoch $c_epoch (the transport row needs 8 or later)"
    fi
fi

# -- the RECEIVER contract -----------------------------------------------------
# Two rows: attach a receiver under a root (an owned child token published to
# out[0]) and close that child under its owner. Byte identities cross as byte
# arrays, never as text:
#
#   nativeReceiverAttach(long root, byte[][] namespace, int overflow, int maxObjects,
#                        long maxBytes, int maxTrackEvents, boolean autoSubscribe,
#                        byte[] catalogTrack, int timeMode, long[] out) -> int
#   descriptor: (J[[BIIJIZ[BI[J)I
#   nativeReceiverClose(long root, long child) -> int
#   descriptor: (JJ)I
#   ABI epoch 9 or later, moved on BOTH sides together
for _pair in 'nativeReceiverAttach|(J[[BIIJIZ[BI[J)I' 'nativeReceiverClose|(JJ)I'; do
    _rn=${_pair%%|*}
    _rd=${_pair#*|}
    _n_rx=$(grep -c "^$_rn " "$scratch/authority")
    if [ "$_n_rx" -ne 1 ]; then
        fail "bridge.receiver.$_rn" "the authority declares '$_rn' $_n_rx time(s); exactly one row is required"
    else
        _got_rx=$(sed -n "s/^$_rn //p" "$scratch/authority")
        if [ "$_got_rx" != "$_rd" ]; then
            fail "bridge.receiver.$_rn" "'$_rn' is declared '$_got_rx', the contract is '$_rd'"
        else
            pass "bridge.receiver.$_rn" "'$_rn' carries $_rd"
        fi
    fi
done
if [ -n "$c_epoch" ] && [ -n "$kt_epoch" ]; then
    if ! printf '%s' "$c_epoch" | grep -qE '^[0-9]+$' || [ "$c_epoch" -lt 9 ] || [ "$kt_epoch" != "$c_epoch" ]; then
        fail bridge.receiver.epoch \
            "the receiver rows are an ABI change: both sides must declare epoch 9 or later, found C=$c_epoch Kotlin=$kt_epoch"
    else
        pass bridge.receiver.epoch "both sides declare ABI epoch $c_epoch (the receiver rows need 9 or later)"
    fi
fi

# -- the RECEIVER EVENT contract ----------------------------------------------
# Two rows: drain one track event from a receiver child into a fixed scalar
# block (a track handle crosses as a BORROWED child token of the receiver),
# and snapshot a track's description as byte arrays plus its scalar block:
#
#   nativeReceiverPollTrack(long root, long child, long[] scalars) -> int
#   descriptor: (JJ[J)I
#   nativeTrackDescribe(long root, long child, long track, long[] scalars) -> byte[][]
#   descriptor: (JJJ[J)[[B
#   ABI epoch 10 or later, moved on BOTH sides together
for _pair in 'nativeReceiverPollTrack|(JJ[J)I' 'nativeTrackDescribe|(JJJ[J)[[B'; do
    _rn=${_pair%%|*}
    _rd=${_pair#*|}
    _n_rx=$(grep -c "^$_rn " "$scratch/authority")
    if [ "$_n_rx" -ne 1 ]; then
        fail "bridge.receiver-event.$_rn" "the authority declares '$_rn' $_n_rx time(s); exactly one row is required"
    else
        _got_rx=$(sed -n "s/^$_rn //p" "$scratch/authority")
        if [ "$_got_rx" != "$_rd" ]; then
            fail "bridge.receiver-event.$_rn" "'$_rn' is declared '$_got_rx', the contract is '$_rd'"
        else
            pass "bridge.receiver-event.$_rn" "'$_rn' carries $_rd"
        fi
    fi
done
if [ -n "$c_epoch" ] && [ -n "$kt_epoch" ]; then
    if ! printf '%s' "$c_epoch" | grep -qE '^[0-9]+$' || [ "$c_epoch" -lt 10 ] || [ "$kt_epoch" != "$c_epoch" ]; then
        fail bridge.receiver-event.epoch \
            "the receiver event rows are an ABI change: both sides must declare epoch 10 or later, found C=$c_epoch Kotlin=$kt_epoch"
    else
        pass bridge.receiver-event.epoch "both sides declare ABI epoch $c_epoch (the receiver event rows need 10 or later)"
    fi
fi

# -- the RECEIVER OBJECT contract ---------------------------------------------
# One row: one poll_object attempt on a receiver child, the object's bytes
# copied out as byte arrays (payload, fragment, packed samples) with a fixed
# scalar block, the C object released exactly once inside the row:
#
#   nativeReceiverPollObject(long root, long child, long[] scalars) -> byte[][]
#   descriptor: (JJ[J)[[B
#   ABI epoch 11 or later, moved on BOTH sides together
_on='nativeReceiverPollObject'
_od='(JJ[J)[[B'
_n_ob=$(grep -c "^$_on " "$scratch/authority")
if [ "$_n_ob" -ne 1 ]; then
    fail "bridge.receiver-object.$_on" "the authority declares '$_on' $_n_ob time(s); exactly one row is required"
else
    _got_ob=$(sed -n "s/^$_on //p" "$scratch/authority")
    if [ "$_got_ob" != "$_od" ]; then
        fail "bridge.receiver-object.$_on" "'$_on' is declared '$_got_ob', the contract is '$_od'"
    else
        pass "bridge.receiver-object.$_on" "'$_on' carries $_od"
    fi
fi
if [ -n "$c_epoch" ] && [ -n "$kt_epoch" ]; then
    if ! printf '%s' "$c_epoch" | grep -qE '^[0-9]+$' || [ "$c_epoch" -lt 11 ] || [ "$kt_epoch" != "$c_epoch" ]; then
        fail bridge.receiver-object.epoch \
            "the receiver object row is an ABI change: both sides must declare epoch 11 or later, found C=$c_epoch Kotlin=$kt_epoch"
    else
        pass bridge.receiver-object.epoch "both sides declare ABI epoch $c_epoch (the receiver object row needs 11 or later)"
    fi
fi

# -- the RECEIVER CONTROL contract --------------------------------------------
# Three rows: subscribe a track (the start-mode ordinal, a has-priority flag
# and the priority), unsubscribe it, and read its state into a one-slot block;
# each takes the root, the receiver child and the track's borrowed token:
#
#   nativeTrackSubscribe(long root, long child, long track, int start, boolean hasPriority, int priority) -> int
#   descriptor: (JJJIZI)I
#   nativeTrackUnsubscribe(long root, long child, long track) -> int
#   descriptor: (JJJ)I
#   nativeTrackState(long root, long child, long track, long[] out) -> int
#   descriptor: (JJJ[J)I
#   ABI epoch 12 or later, moved on BOTH sides together
for _pair in 'nativeTrackSubscribe|(JJJIZI)I' 'nativeTrackUnsubscribe|(JJJ)I' 'nativeTrackState|(JJJ[J)I'; do
    _rn=${_pair%%|*}
    _rd=${_pair#*|}
    _n_rx=$(grep -c "^$_rn " "$scratch/authority")
    if [ "$_n_rx" -ne 1 ]; then
        fail "bridge.receiver-control.$_rn" "the authority declares '$_rn' $_n_rx time(s); exactly one row is required"
    else
        _got_rx=$(sed -n "s/^$_rn //p" "$scratch/authority")
        if [ "$_got_rx" != "$_rd" ]; then
            fail "bridge.receiver-control.$_rn" "'$_rn' is declared '$_got_rx', the contract is '$_rd'"
        else
            pass "bridge.receiver-control.$_rn" "'$_rn' carries $_rd"
        fi
    fi
done
if [ -n "$c_epoch" ] && [ -n "$kt_epoch" ]; then
    if ! printf '%s' "$c_epoch" | grep -qE '^[0-9]+$' || [ "$c_epoch" -lt 12 ] || [ "$kt_epoch" != "$c_epoch" ]; then
        fail bridge.receiver-control.epoch \
            "the receiver control rows are an ABI change: both sides must declare epoch 12 or later, found C=$c_epoch Kotlin=$kt_epoch"
    else
        pass bridge.receiver-control.epoch "both sides declare ABI epoch $c_epoch (the receiver control rows need 12 or later)"
    fi
fi

# -- the RECEIVER RECORD contract ---------------------------------------------
# Two rows: one poll_sap / poll_media_timeline attempt into a fixed scalar
# block that also carries the family's deferral counter, read after the poll
# under the same registry pin:
#
#   nativeReceiverPollSap(long root, long child, long[] scalars) -> int
#   descriptor: (JJ[J)I
#   nativeReceiverPollMediaTimeline(long root, long child, long[] scalars) -> int
#   descriptor: (JJ[J)I
#   ABI epoch 13 or later, moved on BOTH sides together
for _pair in 'nativeReceiverPollSap|(JJ[J)I' 'nativeReceiverPollMediaTimeline|(JJ[J)I'; do
    _rn=${_pair%%|*}
    _rd=${_pair#*|}
    _n_rx=$(grep -c "^$_rn " "$scratch/authority")
    if [ "$_n_rx" -ne 1 ]; then
        fail "bridge.receiver-record.$_rn" "the authority declares '$_rn' $_n_rx time(s); exactly one row is required"
    else
        _got_rx=$(sed -n "s/^$_rn //p" "$scratch/authority")
        if [ "$_got_rx" != "$_rd" ]; then
            fail "bridge.receiver-record.$_rn" "'$_rn' is declared '$_got_rx', the contract is '$_rd'"
        else
            pass "bridge.receiver-record.$_rn" "'$_rn' carries $_rd"
        fi
    fi
done
if [ -n "$c_epoch" ] && [ -n "$kt_epoch" ]; then
    if ! printf '%s' "$c_epoch" | grep -qE '^[0-9]+$' || [ "$c_epoch" -lt 13 ] || [ "$kt_epoch" != "$c_epoch" ]; then
        fail bridge.receiver-record.epoch \
            "the receiver record rows are an ABI change: both sides must declare epoch 13 or later, found C=$c_epoch Kotlin=$kt_epoch"
    else
        pass bridge.receiver-record.epoch "both sides declare ABI epoch $c_epoch (the receiver record rows need 13 or later)"
    fi
fi

# -- the RECEIVER STATS contract ----------------------------------------------
# One row: one get_stats read on a receiver child, every field of
# moq_media_receiver_stats_t published exactly once into a fixed scalar
# block in the struct's order:
#
#   nativeReceiverStats(long root, long child, long[] scalars) -> int
#   descriptor: (JJ[J)I
#   ABI epoch 14 or later, moved on BOTH sides together
_sn='nativeReceiverStats'
_sd='(JJ[J)I'
_n_st=$(grep -c "^$_sn " "$scratch/authority")
if [ "$_n_st" -ne 1 ]; then
    fail "bridge.receiver-stats.$_sn" "the authority declares '$_sn' $_n_st time(s); exactly one row is required"
else
    _got_st=$(sed -n "s/^$_sn //p" "$scratch/authority")
    if [ "$_got_st" != "$_sd" ]; then
        fail "bridge.receiver-stats.$_sn" "'$_sn' is declared '$_got_st', the contract is '$_sd'"
    else
        pass "bridge.receiver-stats.$_sn" "'$_sn' carries $_sd"
    fi
fi
if [ -n "$c_epoch" ] && [ -n "$kt_epoch" ]; then
    if ! printf '%s' "$c_epoch" | grep -qE '^[0-9]+$' || [ "$c_epoch" -lt 14 ] || [ "$kt_epoch" != "$c_epoch" ]; then
        fail bridge.receiver-stats.epoch \
            "the receiver stats row is an ABI change: both sides must declare epoch 14 or later, found C=$c_epoch Kotlin=$kt_epoch"
    else
        pass bridge.receiver-stats.epoch "both sides declare ABI epoch $c_epoch (the receiver stats row needs 14 or later)"
    fi
fi

# -- the SENDER contract ------------------------------------------------------
# Eleven rows: the sender is an OWNED child of the root, its tracks BORROWED
# children of the sender; every block is refused short before any acquire,
# and the write row transfers its buffers on MOQ_OK only.
#
#   ABI epoch 15 or later, moved on BOTH sides together
for _pair in 'nativeSenderAttach|(J[[B[B[J[J)I' 'nativeSenderClose|(JJ)I' \
             'nativeSenderAddTrack|(JJ[[B[J[J)I' 'nativeSenderRemoveTrack|(JJJ)I' \
             'nativeSenderWrite|(JJJ[B[B[J)I' 'nativeSenderEndTrack|(JJJ)I' \
             'nativeSenderConvertToVod|(JJ[J[J)I' 'nativeSenderComplete|(JJ)I' \
             'nativeSenderProbe|(JJ[J)I' 'nativeSenderTrackDemand|(JJJ[J)I' \
             'nativeSenderStats|(JJ[J)I'; do
    _sn=${_pair%%|*}
    _sd=${_pair#*|}
    _n_tx=$(grep -c "^$_sn " "$scratch/authority")
    if [ "$_n_tx" -ne 1 ]; then
        fail "bridge.sender.$_sn" "the authority declares '$_sn' $_n_tx time(s); exactly one row is required"
    else
        _got_tx=$(sed -n "s/^$_sn //p" "$scratch/authority")
        if [ "$_got_tx" != "$_sd" ]; then
            fail "bridge.sender.$_sn" "'$_sn' is declared '$_got_tx', the contract is '$_sd'"
        else
            pass "bridge.sender.$_sn" "'$_sn' carries $_sd"
        fi
    fi
done
if [ -n "$c_epoch" ] && [ -n "$kt_epoch" ]; then
    if ! printf '%s' "$c_epoch" | grep -qE '^[0-9]+$' || [ "$c_epoch" -lt 15 ] || [ "$kt_epoch" != "$c_epoch" ]; then
        fail bridge.sender.epoch \
            "the sender rows are an ABI change: both sides must declare epoch 15 or later, found C=$c_epoch Kotlin=$kt_epoch"
    else
        pass bridge.sender.epoch "both sides declare ABI epoch $c_epoch (the sender rows need 15 or later)"
    fi
fi

# -- the CODEC UTILITY contract -----------------------------------------------
# Two pure rows over the codec signaling utility: no endpoint, token or
# worker. Each copies its inputs, asks the size, builds into an exact buffer,
# and publishes out[0] on MOQ_OK only.
#
#   ABI epoch 16 or later, moved on BOTH sides together
for _pair in 'nativeCodecInitDataBuild|(I[BI[[B)I' 'nativeCodecStringFormat|(I[BZI[B[[B)I'; do
    _cn=${_pair%%|*}
    _cd=${_pair#*|}
    _n_cx=$(grep -c "^$_cn " "$scratch/authority")
    if [ "$_n_cx" -ne 1 ]; then
        fail "bridge.codec.$_cn" "the authority declares '$_cn' $_n_cx time(s); exactly one row is required"
    else
        _got_cx=$(sed -n "s/^$_cn //p" "$scratch/authority")
        if [ "$_got_cx" != "$_cd" ]; then
            fail "bridge.codec.$_cn" "'$_cn' is declared '$_got_cx', the contract is '$_cd'"
        else
            pass "bridge.codec.$_cn" "'$_cn' carries $_cd"
        fi
    fi
done
if [ -n "$c_epoch" ] && [ -n "$kt_epoch" ]; then
    if ! printf '%s' "$c_epoch" | grep -qE '^[0-9]+$' || [ "$c_epoch" -lt 16 ] || [ "$kt_epoch" != "$c_epoch" ]; then
        fail bridge.codec.epoch \
            "the codec rows are an ABI change: both sides must declare epoch 16 or later, found C=$c_epoch Kotlin=$kt_epoch"
    else
        pass bridge.codec.epoch "both sides declare ABI epoch $c_epoch (the codec rows need 16 or later)"
    fi
fi

# The shipped keep rule must name the trusted row in the authority's shape, so
# the row survives a consumer's R8 pass. The generic consumer-rule oracle above
# iterates the authority and is therefore silent while the row is absent from
# it; this names the contract row directly.
_trusted_rule='public final native int nativeEndpointCreateTrusted(java.lang.String, int, int, int[], long, int, java.lang.String, long[]);'
if grep -qF -- "$_trusted_rule" "$rules"; then
    pass bridge.trusted.consumer-rule "the consumer rules keep the trusted row in the instance shape"
else
    fail bridge.trusted.consumer-rule \
        "the consumer rules do not keep '$_trusted_rule'; a row R8 may strip is not a shipped contract"
fi

# CORROBORATION ONLY. A definition or a textual occurrence can be dead,
# unregistered, or on the wrong method's path; the reachability proof is the
# host JVM rows (TrustedCreateTest), which register the real product
# translation unit through MOQ5_ABI_METHODS, invoke the row, and observe the
# trusted route through the host recorder. These two rows exist so a textual
# regression is named alongside the behavioural one, never instead of it.
jni_c="$root/jni/moq5_jni.c"
if [ ! -f "$jni_c" ]; then
    fail bridge.trusted.impl "missing $jni_c"
else
    _n_def=$(grep -c "^static jint JNICALL moq5_impl_$trusted_name(" "$jni_c")
    if [ "$_n_def" -ne 1 ]; then
        fail bridge.trusted.impl \
            "expected exactly one product definition of moq5_impl_$trusted_name in moq5_jni.c, found $_n_def (corroboration; reachability is judged by the host JVM rows)"
    else
        pass bridge.trusted.impl "moq5_impl_$trusted_name is defined in the product translation unit (corroboration only)"
    fi
    if ! grep -q 'moq_endpoint_connect_with_trust' "$jni_c"; then
        fail bridge.trusted.route \
            "moq5_jni.c never names moq_endpoint_connect_with_trust() (corroboration; the host JVM rows judge whether the row actually reaches it)"
    else
        pass bridge.trusted.route "moq5_jni.c names moq_endpoint_connect_with_trust() (corroboration only)"
    fi
fi

# ONE constructor transaction. The ordinary and trusted rows must enter one
# reserve -> construct -> commit -> publish implementation: a second copy of
# that body is a second lifecycle to drift. Corroboration on the translation
# unit: exactly one call site of each registry step and of the publication.
if [ -f "$jni_c" ]; then
    _n_res=$(grep -c 'moq5_reg_reserve_root(' "$jni_c")
    _n_com=$(grep -c 'moq5_reg_commit_root(' "$jni_c")
    _n_pub=$(grep -c 'moq5_publish_token(env' "$jni_c")
    if [ "$_n_res" -eq 1 ] && [ "$_n_com" -eq 1 ] && [ "$_n_pub" -eq 1 ]; then
        pass bridge.trusted.one-transaction \
            "moq5_jni.c reserves, commits and publishes at exactly one call site each"
    else
        fail bridge.trusted.one-transaction \
            "moq5_jni.c has $_n_res reserve, $_n_com commit and $_n_pub publish call site(s); the ordinary and trusted rows must share one transaction body"
    fi
fi

# -- the internal route stays internal ----------------------------------------
# `moq_endpoint_connect_with_trust` carries no MOQ_API and its header is not
# installed. Judged on ARTEFACTS: no built host bridge object may export the
# symbol, no Android diagnostic image may export it (each read with its own
# recorded toolchain), and no service install rule may name the header.
_route_sym=moq_endpoint_connect_with_trust
if [ -n "$_dsos" ]; then
    _leak=""
    for d in $_dsos; do
        if nm -gU "$d" 2>/dev/null | awk '{print $3}' | sed 's/^_//' | grep -qx "$_route_sym"; then
            _leak="$_leak ${d##*/}"
        fi
    done
    if [ -n "$_leak" ]; then
        fail bridge.trusted.route-unexported "a built host object exports $_route_sym:$_leak"
    else
        pass bridge.trusted.route-unexported "no built host object exports $_route_sym"
    fi
else
    fail bridge.trusted.route-unexported "no built host object to inspect for $_route_sym"
fi
for _abi in arm64-v8a x86_64; do
    _adir="$root/.native/_topology/$_abi"
    _aimg="$_adir/diagnostic/libmoq5_jni.so"
    _aprov="$_adir/topology.provenance"
    if [ ! -f "$_aimg" ] || [ ! -r "$_aprov" ]; then
        fail "bridge.trusted.android-unexported.$_abi" \
            "no diagnostic image or receipt for $_abi under $_adir; run tools/build-jni-topology.sh"
        continue
    fi
    _acc=$(sed -n 's/^ndk\.cc=//p' "$_aprov" | sed -n '1p')
    case $_acc in
        */bin/*) _anm="${_acc%/*}/llvm-nm" ;;
        *) _anm="" ;;
    esac
    if [ -z "$_anm" ] || [ ! -x "$_anm" ]; then
        fail "bridge.trusted.android-unexported.$_abi" \
            "the $_abi receipt names no usable toolchain (ndk.cc='$_acc'); the image cannot be read with its own llvm-nm"
        continue
    fi
    if ! "$_anm" --dynamic --defined-only "$_aimg" > "$scratch/adyn.$_abi" 2>/dev/null; then
        fail "bridge.trusted.android-unexported.$_abi" "llvm-nm could not read the dynamic symbols of $_aimg"
        continue
    fi
    if awk '{print $NF}' "$scratch/adyn.$_abi" | grep -qx "$_route_sym"; then
        fail "bridge.trusted.android-unexported.$_abi" "$_abi exports $_route_sym"
    else
        pass "bridge.trusted.android-unexported.$_abi" "$_abi does not export $_route_sym"
    fi
done
_svc_cmake="$root/../../service/CMakeLists.txt"
if [ ! -f "$_svc_cmake" ]; then
    fail bridge.trusted.header-uninstalled "missing $_svc_cmake"
else
    # Every install rule, joined across its parentheses, then searched for the
    # internal header by name or for its directory as a whole.
    tr '\n' ' ' < "$_svc_cmake" | sed 's/install(/\ninstall(/g' | grep '^install(' > "$scratch/installs"
    if grep -qE 'endpoint_trust\.h|DIRECTORY[[:space:]]+src([[:space:]/]|$)' "$scratch/installs"; then
        fail bridge.trusted.header-uninstalled \
            "a service install rule names endpoint_trust.h or the src directory: $(grep -E 'endpoint_trust\.h|DIRECTORY[[:space:]]+src' "$scratch/installs" | sed -n '1p' | cut -c1-120)"
    else
        pass bridge.trusted.header-uninstalled \
            "no service install rule names endpoint_trust.h ($(grep -c . "$scratch/installs") install rule(s) inspected)"
    fi
fi

if [ "$fails" -eq 0 ]; then
    printf '\nbridge ABI authority: 0 failure(s)\n'
    exit 0
fi
printf '\nbridge ABI authority: %s failure(s)\n' "$fails"
exit 1
