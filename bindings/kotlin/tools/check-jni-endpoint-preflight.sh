#!/bin/sh
# The bootstrap boundary for a JNI-backed endpoint root.
#
# A real endpoint root needs the SIX service entry points -- connect,
# get_snapshot, wait, stop, wake, destroy -- to be callable from the JNI
# substrate. The packaged Android image links the service tier, and so does the
# host image a deterministic host-JVM test loads: the substrate is complete, and
# the ABI table carries the endpoint rows (create, trusted create with its
# configuration, release, wake, wait, snapshot, stop, destroy, the latch, drain).
#
# THE INVARIANT, kept for the day a host image is built without the substrate:
# while the host endpoint substrate is incomplete, the ABI method table must
# remain EXACTLY the signed three-row handshake authority. Any added row --
# whatever it is called -- is refused then. Reachability is never inferred from
# a method's English spelling, because a name is not a semantic authority.
#
# Rules this gate follows, each of which an earlier revision broke:
#
#  1. No tool's status or diagnostic is discarded. `nm`, `awk`, `find` and the
#     hasher are each observed directly; a pipeline's status is never taken for
#     a producer's. Hashing goes through lib-hash.sh for exactly that reason.
#  2. Substrate presence is decided by IDENTITY over the exact six names, never
#     by a count of any `moq_*` symbol, and undefined symbols are excluded
#     structurally so an unresolved reference cannot read as a definition.
#  3. Every judged image is bound to its producer's receipt, parsed with
#     LITERAL keys and exact cardinality against a declared schema, and the
#     receipt is bound to the current source it claims to describe.
#  4. Each Android ABI is judged independently, with the `llvm-nm` belonging to
#     that ABI's own recorded toolchain -- not the first one found on the host.
#
# Fail-closed throughout: a missing tool, image, receipt, schema row, or
# unreadable symbol table is a named failure, never an empty pass.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
fails=0
pass() { printf 'PASS[%s]: %s\n' "$1" "$2"; }
fail() { fails=$((fails + 1)); printf 'FAIL[%s]: %s\n' "$1" "$2"; }

case $0 in
    */*) _selfdir=${0%/*} ;;
    *)   _selfdir=. ;;
esac
here=$(cd "$_selfdir" && pwd) || { echo "$_self: cannot resolve the script directory" >&2; exit 2; }
root=${MOQ5_EP_PREFLIGHT_ROOT:-$(cd "$here/.." && pwd)} \
    || { echo "$_self: cannot resolve bindings/kotlin" >&2; exit 2; }
[ -d "$root" ] || { echo "$_self: not a directory: $root" >&2; exit 2; }

for t in grep sed awk sort; do
    command -v "$t" >/dev/null 2>&1 || { echo "$_self: required tool not found: $t" >&2; exit 2; }
done
[ -r "$here/lib-hash.sh" ] || { echo "$_self: missing $here/lib-hash.sh" >&2; exit 2; }
# shellcheck source=lib-hash.sh
. "$here/lib-hash.sh"

scratch=$(mktemp -d "${TMPDIR:-/tmp}/moq5-eppf-XXXXXXXX") \
    || { echo "$_self: cannot create a task-owned scratch" >&2; exit 2; }
# shellcheck disable=SC2329  # invoked from the traps below
cleanup() { rm -rf -- "$scratch"; }
trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

# A missing hasher is a NAMED failure of this gate on every platform, not an
# exit-2 abort that reads as an environment problem.
if ! _hashtool=$(moq_hash_available 2>"$scratch/h.err"); then
    fail endpoint-preflight.tooling \
        "no SHA-256 tool: $(sed -n '1p' "$scratch/h.err"); every provenance row is unprovable"
    printf '\njni endpoint preflight: %s failure(s)\n' "$fails"
    exit 1
fi

# -- declared authorities -----------------------------------------------------
# The endpoint substrate, named exactly. Not a pattern, not a count.
EP_SET='moq_endpoint_connect moq_endpoint_get_snapshot moq_endpoint_wait
moq_endpoint_stop moq_endpoint_wake moq_endpoint_destroy'
EP_N=6

# The media RECEIVER substrate the receiver rows execute, named exactly: the
# attach/close rows' five, the event rows' description and terminal readers,
# the object row's poll and release, the control rows' subscribe,
# unsubscribe and state, and the record rows' polls and stats. Judged by identity in every image, like the six, whenever the ABI
# table carries the receiver rows (the rows are the only reason the substrate
# must be present).
RX_SET='moq_media_receiver_cfg_init moq_media_receiver_attach moq_media_receiver_destroy
moq_media_receiver_wait moq_media_receiver_poll_track moq_media_track_desc_get
moq_media_receiver_is_closed moq_media_receiver_is_fatal moq_media_receiver_fatal_code
moq_media_receiver_poll_object moq_media_object_cleanup
moq_media_receiver_track_subscribe_cfg_init moq_media_receiver_subscribe_track
moq_media_receiver_unsubscribe_track moq_media_receiver_track_state
moq_media_receiver_poll_sap moq_media_receiver_poll_media_timeline moq_media_receiver_get_stats'
RX_N=18
RECEIVER_ROWS='nativeReceiverAttach|(J[[BIIJIZ[BI[J)I
nativeReceiverClose|(JJ)I'

# The media SENDER substrate the sender rows execute, named exactly: the
# attach/close rows' initializers, attach and destroy, the track rows' add,
# remove, write, end and convert, complete, the probe's five queries, the
# demand count, the stats read, and the rcbufs the write row builds and
# releases. Judged by identity whenever the ABI table carries the sender rows.
TX_SET='moq_media_sender_cfg_init_sized moq_media_sender_callbacks_init_sized
moq_media_sender_attach moq_media_sender_destroy moq_media_track_cfg_init
moq_media_sender_add_track moq_media_sender_remove_track moq_media_sender_write
moq_media_sender_end_track moq_media_sender_convert_to_vod moq_media_sender_complete
moq_media_sender_is_ready moq_media_sender_is_closed moq_media_sender_is_fatal
moq_media_sender_fatal_code moq_media_sender_has_media_subscriber
moq_media_sender_track_subscriptions moq_media_sender_get_stats
moq_rcbuf_create moq_rcbuf_decref'
TX_N=20
SENDER_ROWS='nativeSenderAttach|(J[[B[B[J[J)I
nativeSenderClose|(JJ)I'

# The Android ABIs that must BOTH be judged. One-ABI-only is a failure.
ABIS='arm64-v8a x86_64'

# Symbols that exist ONLY to select host test inputs. Writable indirect dispatch
# for an ordinary product call is not product state, so these names must not
# appear in a shipped image at all -- in either ABI, diagnostic or packaged.
HOST_ONLY_SYMS='moq5_wait_target moq5_snapshot_target moq5_wait_closed
moq5_wait_odd moq5_snapshot_future moq5_retiring_milestone moq5_mode
moq5_test_real_connect_with_trust moq5_test_media_object_cleanup
moq_media_receiver_test_ingest moq_media_receiver_test_push_object
moq_media_receiver_test_mark_desired moq_media_receiver_test_set_transport_version
moq_media_receiver_test_track_at'

# The internal trust route a trusted-create row must reach. Whenever the ABI
# table carries that row, every judged image must define the route exactly
# once -- a row whose constructor is absent from the image cannot be executed
# by any test that loads it.
TRUSTED_ROW='nativeEndpointCreateTrusted|(Ljava/lang/String;II[IJILjava/lang/String;[J)I'
TRUST_ROUTE=moq_endpoint_connect_with_trust

# The SIGNED handshake authority: the exact rows the ABI table may carry while
# the host substrate is incomplete, as name|descriptor pairs. With the substrate
# complete (the case today) the table legitimately carries more than these.
AUTH_ROWS='nativeAbiEpoch|()I
nativeFeatureBits|()J
nativeLibmoqVersion|()I'

# The host producer's receipt schema: every key that must appear exactly once,
# and no others. The image rows are the COMPLETE produced set, so a dropped or
# added image is visible rather than inferred from one consumed row.
HOST_RCPT_KEYS='producer cc jni.include openssl.prefix source.moq5_jni.c.sha256'
HOST_IMAGES='bridge_product moq5_jni registry_roundtrip bridge_compatible
bridge_epoch_high bridge_epoch_low bridge_features_absent bridge_features_extra
bridge_features_wide bridge_omit_one bridge_bad_descriptor
bridge_registration_fails trust_delegate'
HOST_PRODUCER='build-bridge-testdsos.sh'

# -- receipt parsing: literal keys, exact cardinality -------------------------
# rcpt_count <file> <key>  -- rows whose key is EXACTLY <key> (no regex).
rcpt_count() {
    awk -v k="$2" 'index($0, k "=") == 1 { n++ } END { print n + 0 }' "$1" 2>"$scratch/awk.err"
    if [ -s "$scratch/awk.err" ]; then
        printf 'AWKFAIL' ; return 1
    fi
    return 0
}
# rcpt_one <file> <key>  -- the single value, or fail naming the cardinality.
rcpt_one() {
    _rc=$(rcpt_count "$1" "$2") || { echo "awk failed reading $2" >&2; return 1; }
    if [ "$_rc" -ne 1 ]; then
        echo "receipt declares $_rc row(s) for '$2'; exactly 1 is required" >&2
        return 1
    fi
    awk -v k="$2" 'index($0, k "=") == 1 { print substr($0, length(k) + 2) }' "$1" \
        2>"$scratch/awk.err"
    [ -s "$scratch/awk.err" ] && { echo "awk failed extracting $2" >&2; return 1; }
    return 0
}

# -- symbol reading: no discarded status, no discarded diagnostic -------------
dump_syms() {
    _dnm=$1; _dimg=$2; _dout=$3
    "$_dnm" "$_dimg" > "$scratch/nm.out" 2> "$scratch/nm.err"
    _dst=$?
    if [ "$_dst" -ne 0 ]; then
        printf '%s\n' "$_dnm exited $_dst: $(sed -n '1p' "$scratch/nm.err")"
        return 1
    fi
    if [ -s "$scratch/nm.err" ]; then
        printf '%s\n' "$_dnm wrote a diagnostic: $(sed -n '1p' "$scratch/nm.err")"
        return 1
    fi
    # The type column is the field before the name; U/u are undefined and are
    # excluded here, so an unresolved REFERENCE can never read as a definition.
    awk 'NF >= 2 { t = $(NF-1); n = $NF;
                   if (t != "U" && t != "u") print t, n }' \
        "$scratch/nm.out" > "$_dout" 2> "$scratch/awk.err"
    _ast=$?
    if [ "$_ast" -ne 0 ] || [ -s "$scratch/awk.err" ]; then
        printf '%s\n' "awk failed reading the symbol table: $(sed -n '1p' "$scratch/awk.err")"
        return 1
    fi
    return 0
}

# defined_count <symfile> <name>  -- occurrences, tolerating a leading underscore
defined_count() {
    awk -v n="$2" '{ s = $NF; sub(/^_/, "", s); if (s == n) c++ } END { print c + 0 }' "$1"
}

# ep_membership <symfile> <label>  -- sets ep_present/ep_missing/ep_dup
ep_membership() {
    ep_present=0; ep_missing=""; ep_dup=""
    for _s in $EP_SET; do
        _c=$(defined_count "$1" "$_s")
        case $_c in
            0) ep_missing="$ep_missing $_s" ;;
            1) ep_present=$((ep_present + 1)) ;;
            *) ep_dup="$ep_dup $_s(x$_c)" ;;
        esac
    done
}

# receiver_rule <symfile> <label> <image description>: whenever the ABI table
# carries the receiver rows, the receiver entry points must be present exactly
# once each in the image under judgement; with the rows absent there is
# nothing to execute and the rule says so rather than passing silently.
receiver_rule() {
    if [ ! -s "$scratch/rows.actual" ]; then
        fail "$2" "no parsed ABI table; the receiver rule cannot be applied to $3"
        return
    fi
    _rows_absent=0
    for _rr in $RECEIVER_ROWS; do
        grep -qxF -- "$_rr" "$scratch/rows.actual" || _rows_absent=1
    done
    if [ "$_rows_absent" -eq 1 ]; then
        pass "$2" "the ABI table carries no receiver row, so $3 need not define the receiver entry points"
        return
    fi
    _rxp=0; _rxm=""; _rxd=""
    for _s in $RX_SET; do
        _c=$(defined_count "$1" "$_s")
        case $_c in
            0) _rxm="$_rxm $_s" ;;
            1) _rxp=$((_rxp + 1)) ;;
            *) _rxd="$_rxd $_s(x$_c)" ;;
        esac
    done
    if [ -n "$_rxd" ]; then
        fail "$2" "$3 defines a duplicate or aliased receiver entry point:$_rxd"
    elif [ "$_rxp" -ne "$RX_N" ]; then
        fail "$2" "$3 defines $_rxp of $RX_N receiver entry points (missing:$_rxm)"
    else
        pass "$2" "$3 defines all $RX_N receiver entry points, once each"
    fi
}

# sender_rule <symfile> <label> <image description>: the sender twin of the
# receiver rule, over the sender rows and TX_SET.
sender_rule() {
    if [ ! -s "$scratch/rows.actual" ]; then
        fail "$2" "no parsed ABI table; the sender rule cannot be applied to $3"
        return
    fi
    _rows_absent=0
    for _sr in $SENDER_ROWS; do
        grep -qxF -- "$_sr" "$scratch/rows.actual" || _rows_absent=1
    done
    if [ "$_rows_absent" -eq 1 ]; then
        pass "$2" "the ABI table carries no sender row, so $3 need not define the sender entry points"
        return
    fi
    _txp=0; _txm=""; _txd=""
    for _s in $TX_SET; do
        _c=$(defined_count "$1" "$_s")
        case $_c in
            0) _txm="$_txm $_s" ;;
            1) _txp=$((_txp + 1)) ;;
            *) _txd="$_txd $_s(x$_c)" ;;
        esac
    done
    if [ -n "$_txd" ]; then
        fail "$2" "$3 defines a duplicate or aliased sender entry point:$_txd"
    elif [ "$_txp" -ne "$TX_N" ]; then
        fail "$2" "$3 defines $_txp of $TX_N sender entry points (missing:$_txm)"
    else
        pass "$2" "$3 defines all $TX_N sender entry points, once each"
    fi
}

# trusted_route_rule <symfile> <label> <image description>
# Applies only when the ABI table carries the trusted-create row (read from the
# table parsed in step 1); with the row absent there is nothing to bind and the
# rule says so rather than passing silently on a missing table.
trusted_route_rule() {
    if [ ! -s "$scratch/rows.actual" ]; then
        fail "$2" "no parsed ABI table; the trust-route rule cannot be applied to $3"
        return
    fi
    if ! grep -qxF -- "$TRUSTED_ROW" "$scratch/rows.actual"; then
        pass "$2" "the ABI table carries no trusted-create row, so $3 need not define $TRUST_ROUTE"
        return
    fi
    _tc=$(defined_count "$1" "$TRUST_ROUTE")
    case $_tc in
        1) pass "$2" "the ABI table carries the trusted-create row and $3 defines $TRUST_ROUTE once" ;;
        0) fail "$2" "the ABI table carries the trusted-create row but $3 does not define $TRUST_ROUTE: the row's constructor is unreachable there" ;;
        *) fail "$2" "$3 defines $TRUST_ROUTE $_tc times" ;;
    esac
}

# -- 1. the ABI method table, against the signed authority --------------------
ABI_H="$root/jni/moq5_abi.h"
table_is_authority=0
table_delta=""
if [ ! -r "$ABI_H" ]; then
    fail endpoint-preflight.table "no ABI method table at $ABI_H"
else
    sed -n '/#define MOQ5_ABI_METHODS/,/^$/p' "$ABI_H" |
        sed -n 's/^ *X(\([A-Za-z0-9_]*\), *"\([^"]*\)").*/\1|\2/p' |
        sort > "$scratch/rows.actual"
    printf '%s\n' "$AUTH_ROWS" | sort > "$scratch/rows.auth"
    _n=$(awk 'END { print NR }' "$scratch/rows.actual")
    if [ "${_n:-0}" -lt 1 ]; then
        fail endpoint-preflight.table "the ABI method table has no readable rows"
    else
        table_delta=$(comm -3 "$scratch/rows.auth" "$scratch/rows.actual" | tr '\t' ' ' | tr '\n' ' ')
        if [ -z "$table_delta" ]; then
            table_is_authority=1
            pass endpoint-preflight.table \
                "the ABI method table is exactly the signed handshake authority ($_n rows)"
        else
            pass endpoint-preflight.table \
                "the ABI method table declares $_n row(s) and differs from the signed authority:$table_delta"
        fi
    fi
fi

# -- 2. the host receipt, its source binding, and the substrate ---------------
host_complete=0
bdir="$root/.native/_bridge"
brcpt="$bdir/bridge.provenance"
host_img=""
for _c in "$bdir/libmoq5_jni.dylib" "$bdir/libmoq5_jni.so"; do
    [ -f "$_c" ] && { host_img=$_c; break; }
done
if [ -z "$host_img" ]; then
    fail endpoint-preflight.host.image "no host bridge image under $bdir; run $HOST_PRODUCER"
elif [ ! -r "$brcpt" ]; then
    fail endpoint-preflight.host.provenance \
        "no producer receipt at $brcpt: the host image cannot be bound to the build that wrote it"
else
    _schema_bad=""
    # every declared key exactly once ...
    for _k in $HOST_RCPT_KEYS; do
        _c=$(rcpt_count "$brcpt" "$_k") || { _schema_bad="awk failed on '$_k'"; break; }
        [ "$_c" -eq 1 ] || { _schema_bad="key '$_k' appears $_c time(s), expected 1"; break; }
    done
    # ... one row per produced image, the COMPLETE expected set ...
    if [ -z "$_schema_bad" ]; then
        for _i in $HOST_IMAGES; do
            _found=0
            for _e in dylib so; do
                _c=$(rcpt_count "$brcpt" "image.lib$_i.$_e.sha256") \
                    || { _schema_bad="awk failed on image.lib$_i"; break; }
                _found=$((_found + _c))
            done
            [ -n "$_schema_bad" ] && break
            [ "$_found" -eq 1 ] || { _schema_bad="image 'lib$_i' has $_found receipt row(s), expected 1"; break; }
        done
    fi
    # ... and no rows beyond the schema.
    if [ -z "$_schema_bad" ]; then
        _rows=$(awk 'NF { n++ } END { print n + 0 }' "$brcpt")
        _want=0
        for _k in $HOST_RCPT_KEYS; do _want=$((_want + 1)); done
        for _i in $HOST_IMAGES; do _want=$((_want + 1)); done
        [ "$_rows" -eq "$_want" ] \
            || _schema_bad="the receipt carries $_rows row(s); the declared schema is exactly $_want"
    fi

    if [ -n "$_schema_bad" ]; then
        fail endpoint-preflight.host.provenance "the host receipt does not match its declared schema: $_schema_bad"
    else
        _prod=$(rcpt_one "$brcpt" producer 2>"$scratch/r.err")
        _srcd=$(rcpt_one "$brcpt" source.moq5_jni.c.sha256 2>>"$scratch/r.err")
        _imgd=$(rcpt_one "$brcpt" "image.${host_img##*/}.sha256" 2>>"$scratch/r.err")
        _now_src=$(moq_hash_file "$root/jni/moq5_jni.c" "$scratch" 2>>"$scratch/r.err")
        _now_img=$(moq_hash_file "$host_img" "$scratch" 2>>"$scratch/r.err")
        if [ -s "$scratch/r.err" ] || [ -z "$_prod" ] || [ -z "$_srcd" ] || [ -z "$_imgd" ] \
           || [ -z "$_now_src" ] || [ -z "$_now_img" ]; then
            fail endpoint-preflight.host.provenance \
                "the host receipt cannot be read or hashed: $(sed -n '1p' "$scratch/r.err")"
        elif [ "$_prod" != "$HOST_PRODUCER" ]; then
            fail endpoint-preflight.host.provenance \
                "the host receipt names producer '$_prod', expected '$HOST_PRODUCER'"
        elif ! moq_is_sha256 "$_srcd" || ! moq_is_sha256 "$_imgd"; then
            fail endpoint-preflight.host.provenance \
                "the host receipt carries a value that is not a SHA-256"
        elif [ "$_srcd" != "$_now_src" ]; then
            fail endpoint-preflight.host.provenance \
                "the receipt describes moq5_jni.c $_srcd but the current source is $_now_src: \
the image was built from different source"
        elif [ "$_imgd" != "$_now_img" ]; then
            fail endpoint-preflight.host.provenance \
                "${host_img##*/} is $_now_img but its producer recorded $_imgd: a stale image is being judged"
        else
            pass endpoint-preflight.host.provenance \
                "the host image and its receipt describe the current $HOST_PRODUCER build of moq5_jni.c"
            if ! _err=$(dump_syms "${MOQ5_EP_PREFLIGHT_HOST_NM:-nm}" "$host_img" "$scratch/host.syms"); then
                fail endpoint-preflight.host.substrate "cannot read the host symbol table: $_err"
            else
                ep_membership "$scratch/host.syms"
                _anymoq=$(awk '{ s = $NF; sub(/^_/, "", s);
                                 if (s ~ /^moq_/) c++ } END { print c + 0 }' "$scratch/host.syms")
                if [ -n "$ep_dup" ]; then
                    fail endpoint-preflight.host.substrate \
                        "the host image defines a duplicate endpoint entry point:$ep_dup"
                elif [ "$ep_present" -eq "$EP_N" ]; then
                    host_complete=1
                    pass endpoint-preflight.host.substrate \
                        "the host image defines all $EP_N endpoint entry points, once each"
                else
                    pass endpoint-preflight.host.substrate \
                        "the host image defines $ep_present of $EP_N endpoint entry points (missing:$ep_missing); \
it links $_anymoq LibMoQ symbol(s), which do NOT substitute for the six"
                fi
                trusted_route_rule "$scratch/host.syms" endpoint-preflight.host.trust-route "the host image"
                receiver_rule "$scratch/host.syms" endpoint-preflight.host.receiver "the host image"
                sender_rule "$scratch/host.syms" endpoint-preflight.host.sender "the host image"
            fi
        fi
    fi
fi

# -- 3. both Android ABIs, each with its OWN recorded toolchain ---------------
for _abi in $ABIS; do
    _adir="$root/.native/_topology/$_abi"
    _prov="$_adir/topology.provenance"
    _diag="$_adir/diagnostic/libmoq5_jni.so"
    if [ ! -d "$_adir" ]; then
        fail "endpoint-preflight.android.$_abi" \
            "no topology output for $_abi; both ABIs must be judged"
        continue
    fi
    if [ ! -f "$_diag" ]; then
        fail "endpoint-preflight.android.$_abi" "no diagnostic image at $_diag"
        continue
    fi
    if [ ! -r "$_prov" ]; then
        fail "endpoint-preflight.android.$_abi" "no topology receipt at $_prov"
        continue
    fi
    : > "$scratch/a.err"
    _want=$(rcpt_one "$_prov" 'diagnostic.sha256' 2>>"$scratch/a.err")
    _cc=$(rcpt_one "$_prov" 'ndk.cc' 2>>"$scratch/a.err")
    _ndkv=$(rcpt_one "$_prov" 'ndk.version' 2>>"$scratch/a.err")
    _rabi=$(rcpt_one "$_prov" 'abi' 2>>"$scratch/a.err")
    if [ -s "$scratch/a.err" ]; then
        fail "endpoint-preflight.android.$_abi" \
            "the $_abi receipt is not exact: $(sed -n '1p' "$scratch/a.err")"
        continue
    fi
    if [ "$_rabi" != "$_abi" ]; then
        fail "endpoint-preflight.android.$_abi" \
            "the receipt in $_abi/ declares abi=$_rabi: a receipt from another ABI is being consulted"
        continue
    fi
    _have=$(moq_hash_file "$_diag" "$scratch" 2>>"$scratch/a.err")
    if [ -z "$_have" ] || ! moq_is_sha256 "$_want"; then
        fail "endpoint-preflight.android.$_abi" \
            "cannot hash the $_abi diagnostic image, or its recorded digest is malformed: \
$(sed -n '1p' "$scratch/a.err")"
        continue
    fi
    if [ "$_want" != "$_have" ]; then
        fail "endpoint-preflight.android.$_abi" \
            "$_abi diagnostic image is $_have but its producer recorded $_want"
        continue
    fi
    # The tool that belongs to THIS ABI's recorded toolchain, not the first one
    # found on the host: a symbol table read by an unrelated nm is unattributed.
    _anm=${MOQ5_EP_PREFLIGHT_ANDROID_NM:-}
    if [ -z "$_anm" ]; then
        case $_cc in
            */bin/*) _anm="${_cc%/*}/llvm-nm" ;;
            *) fail "endpoint-preflight.android.$_abi" \
                   "the recorded ndk.cc '$_cc' has no toolchain bin directory"; continue ;;
        esac
        case $_cc in
            *"$_ndkv"*) : ;;
            *) fail "endpoint-preflight.android.$_abi" \
                   "the recorded ndk.cc does not belong to the recorded ndk.version $_ndkv"; continue ;;
        esac
        if [ ! -x "$_anm" ]; then
            fail "endpoint-preflight.android.$_abi" \
                "no executable llvm-nm beside the recorded compiler at $_anm"
            continue
        fi
    fi
    if ! _err=$(dump_syms "$_anm" "$_diag" "$scratch/a.syms"); then
        fail "endpoint-preflight.android.$_abi" "cannot read the $_abi symbol table: $_err"
        continue
    fi
    # Host-only selectors must be absent from the shipped image.
    _hostbad=""
    for _h in $HOST_ONLY_SYMS; do
        _c=$(defined_count "$scratch/a.syms" "$_h")
        [ "$_c" -eq 0 ] || _hostbad="$_hostbad $_h(x$_c)"
    done
    if [ -n "$_hostbad" ]; then
        fail "endpoint-preflight.android.$_abi" \
            "$_abi defines host-only test selector symbol(s):$_hostbad"
        continue
    fi

    ep_membership "$scratch/a.syms"
    if [ -n "$ep_dup" ]; then
        fail "endpoint-preflight.android.$_abi" \
            "$_abi defines a duplicate or aliased endpoint entry point:$ep_dup"
    elif [ "$ep_present" -ne "$EP_N" ]; then
        fail "endpoint-preflight.android.$_abi" \
            "$_abi defines $ep_present of $EP_N endpoint entry points (missing:$ep_missing)"
    else
        pass "endpoint-preflight.android.$_abi" \
            "$_abi defines all $EP_N endpoint entry points, once each, read with its own recorded toolchain"
    fi
    trusted_route_rule "$scratch/a.syms" "endpoint-preflight.android.$_abi.trust-route" "$_abi"
    receiver_rule "$scratch/a.syms" "endpoint-preflight.android.$_abi.receiver" "$_abi"
    sender_rule "$scratch/a.syms" "endpoint-preflight.android.$_abi.sender" "$_abi"
done

# -- 4. THE INVARIANT ---------------------------------------------------------
# Not "does a row look like an endpoint method". While the host substrate is
# incomplete the table must BE the signed authority; any added row is refused
# whatever it is called.
if [ "$host_complete" -ne 1 ] && [ "$table_is_authority" -ne 1 ]; then
    fail endpoint-preflight.reachable \
        "the ABI table is not the signed handshake authority ($table_delta) while the host image \
lacks the complete endpoint substrate: no added native method could be executed by any \
deterministic host test"
elif [ "$host_complete" -eq 1 ]; then
    pass endpoint-preflight.reachable \
        "the host image carries the endpoint substrate, so added ABI rows are executable there"
else
    pass endpoint-preflight.reachable \
        "the ABI table is exactly the signed authority, which is what the incomplete host substrate permits"
fi

if [ "$fails" -eq 0 ]; then
    printf '\njni endpoint preflight: 0 failure(s)\n'
    exit 0
fi
printf '\njni endpoint preflight: %s failure(s)\n' "$fails"
exit 1
