#!/usr/bin/env bash
#
# Permanent gate for the consumer drivers' provenance contract.
#
# Runs under BOTH interpreters the drivers will actually meet: /bin/bash, which
# on macOS is 3.2 and treats "${arr[@]}" on an EMPTY array as an unbound
# variable under `set -u`, and the normal PATH bash.
#
# Two modes are exercised:
#   - argument validation, which must produce a NAMED diagnostic rather than an
#     unbound-variable abort;
#   - MOQ_CONSUMER_PREFLIGHT=1, which constructs EVERY final configure/compile/
#     link array and stops without building, configuring, or deleting anything.
#     A generic marker is not an oracle: this gate judges the exact per-array
#     element counts AND the exact array each optional value entered, so
#     omitting one optional append, or bypassing a late builder entirely, is a
#     named failure rather than a silently identical "OK".
#
# `--child-empty-array-probe` is this script's own mode for the Bash 3.2
# hazard, so the probe needs no quoting suppression.
set -euo pipefail

if [ "${1-}" = "--child-empty-array-probe" ]; then
    probe=()
    printf '%s' "${probe[@]}"
    exit 0
fi

# `--child-canonical-probe <cmake> <driver> <label> <value>` sources the named
# driver with its library-only seam and calls THAT driver's own
# canonical_artifact. Expressed as a mode of this script rather than a quoted
# `-c` body, so no expansion-suppression diagnostic has to be silenced.
if [ "${1-}" = "--child-canonical-probe" ]; then
    probe_cmake="$2"
    probe_driver="$3"
    probe_label="$4"
    probe_value="$5"
    MOQ_CONSUMER_LIB_ONLY=1
    export MOQ_CONSUMER_LIB_ONLY
    # shellcheck source=/dev/null
    . "$probe_driver"
    # Set AFTER sourcing: the seam replaces the positional parameters and the
    # driver assigns its own CMAKE from them.
    # Read by the sourced driver's helper, not by this script.
    CMAKE="$probe_cmake"
    export CMAKE
    canonical_artifact "$probe_label" "$probe_value"
    exit 0
fi

if [ "${1-}" = "--child-version" ]; then
    printf '%s.%s' "${BASH_VERSINFO[0]}" "${BASH_VERSINFO[1]}"
    exit 0
fi

# An ISOLATED work directory per invocation, so a stale path left by another
# run cannot make the no-mutation proof pass vacuously. A sentinel file inside
# it pins directory identity as well as existence.
SELFTEST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/moq-driver-selftest.XXXXXX")"
WORKDIR="$SELFTEST_ROOT/work"
SENTINEL="$SELFTEST_ROOT/sentinel"
printf 'do-not-touch' > "$SENTINEL"
SENTINEL_BEFORE="$(cat "$SENTINEL")"
cleanup() { rm -rf "$SELFTEST_ROOT"; }
trap cleanup EXIT

HERE="$(cd "$(dirname "$0")" && pwd)"
INSTALL_SH="$HERE/run_install_consumer_threaded.sh"
PKGCFG_SH="$HERE/../../pico_wt/tests/run_pkgconfig_consumer.sh"
SELF="$HERE/$(basename "$0")"
CMAKE_EXE="${MOQ_SELFTEST_CMAKE:-cmake}"

failures=0
fail() { echo "FAIL[$1]: $2" >&2; failures=$((failures + 1)); }

# A valid 20-argument call with every OPTIONAL field empty. Positions:
# 1 cmake, 2-3 srcs, 4 build dir, 5 pq src, 6 ptls prefix, 7 cc, 8 cflags,
# 9 ldflags, 10 build type, 11 openssl root, 12 work, 13 libdir,
# 14 verifier, 15 WITH_OPENSSL, 16 WITH_MBEDTLS, 17 prefix path,
# 18 ptls core, 19 ptls minicrypto, 20 manifest.
base_args() {
    printf '%s\n' cmake x y z p q cc "" "" Debug "$1" "$WORKDIR" \
        lib openssl ON OFF "$2" "$3" "$4" "$SELFTEST_ROOT/manifest"
}

expect_named() {  # <tag> <shell> <script> <expected-substring> [args...]
    local tag="$1" sh="$2" script="$3" want="$4"; shift 4
    local out rc=0
    out="$("$sh" "$script" "$@" 2>&1)" || rc=$?
    case "$out" in
        *"$want"*) ;;
        *) fail "$tag" "expected '$want', got: $(printf '%s' "$out" | head -1)" ;;
    esac
    case "$out" in
        *"unbound variable"*) fail "$tag" "unbound variable in output" ;;
    esac
    if [ "$rc" = 0 ]; then fail "$tag" "expected a non-zero exit"; fi
}

# --- artifact-identity canonicalization, exercised in BOTH drivers ---------
# A narrative-only alias check would leave a regression to parent-only `pwd -P`
# green, so the discriminator is permanent and runs under both interpreters.
# It calls each driver's own canonical_artifact() by sourcing the driver with
# MOQ_CONSUMER_LIB_ONLY=1, which defines the functions and returns without
# executing anything.
CANON_DIR="$SELFTEST_ROOT/canon"
mkdir -p "$CANON_DIR"
printf 'archive-one' > "$CANON_DIR/real-one.a"
printf 'archive-two' > "$CANON_DIR/real-two.a"
ln -s "$CANON_DIR/real-one.a" "$CANON_DIR/alias-a.a"
ln -s "$CANON_DIR/real-one.a" "$CANON_DIR/alias-b.a"

# The fixture must be what it claims: distinct spellings, both symlinks, both
# pointing at the intended file. A failed `ln` or a stale path must not pass.
alias_a="$CANON_DIR/alias-a.a"
alias_b="$CANON_DIR/alias-b.a"
if [ "$alias_a" = "$alias_b" ]; then
    fail "canon/fixture" "the two aliases are the same path spelling"
fi
for a in alias-a alias-b; do
    if [ ! -L "$CANON_DIR/$a.a" ]; then
        fail "canon/fixture" "$a.a is not a symlink"
    fi
    if [ "$(cat "$CANON_DIR/$a.a")" != "archive-one" ]; then
        fail "canon/fixture" "$a.a does not point at real-one.a"
    fi
done
if [ "$(cat "$CANON_DIR/real-two.a")" = "$(cat "$CANON_DIR/real-one.a")" ]; then
    fail "canon/fixture" "the two real files are not distinct"
fi

# Runs the probe and reports BOTH halves in globals: `canon_out` and
# `canon_rc`. It must NOT be called through a command substitution -- that runs
# in a subshell, so the status would never reach the caller (this harness had
# exactly that defect). Asserting only the text would in any case let a helper
# that prints the named diagnostic and then returns success pass, so "fail by
# name" would be half-proven.
canon_out=""
canon_rc=0
canon_probe() {  # <shell> <driver> <label> <value>
    canon_rc=0
    canon_out="$("$1" "$SELF" --child-canonical-probe "$CMAKE_EXE" "$2" "$3" "$4" 2>&1)" \
        || canon_rc=$?
}

for SH in /bin/bash "$(command -v bash)"; do
    tag="$("$SH" "$SELF" --child-version)"
    for S in "$INSTALL_SH" "$PKGCFG_SH"; do
        base="$(basename "$S")"
        canon_probe "$SH" "$S" fixture "$alias_a"; a="$canon_out"; a_rc="$canon_rc"
        canon_probe "$SH" "$S" fixture "$alias_b"; b="$canon_out"; b_rc="$canon_rc"
        canon_probe "$SH" "$S" fixture "$CANON_DIR/real-two.a"; t="$canon_out"; t_rc="$canon_rc"
        # A valid artifact must succeed AND yield one usable canonical result.
        for pair in "alias_a:$a_rc:$a" "alias_b:$b_rc:$b" "real_two:$t_rc:$t"; do
            nm="${pair%%:*}"; rest="${pair#*:}"; rc="${rest%%:*}"; val="${rest#*:}"
            if [ "$rc" != 0 ]; then
                fail "$base/$tag/canon-valid-status" \
                     "$nm resolved with status $rc; a valid artifact must succeed"
            fi
            if [ -z "$val" ] || [ ! -f "$val" ]; then
                fail "$base/$tag/canon-valid-result" \
                     "$nm produced no usable canonical result ('$val')"
            fi
        done
        # two aliases of ONE archive must compare equal ...
        if [ "$a" != "$b" ]; then
            fail "$base/$tag/canon-alias" \
                 "two aliases of one archive resolved differently: '$a' vs '$b'"
        fi
        # The expectation is itself canonical: the temporary root can sit under
        # a symlinked prefix (/var -> /private/var on macOS), so comparing
        # against the raw fixture spelling would fail for the wrong reason.
        canon_probe "$SH" "$S" fixture "$CANON_DIR/real-one.a"; want="$canon_out"
        if [ "$a" != "$want" ]; then
            fail "$base/$tag/canon-target" \
                 "an alias resolved to '$a', expected '$want'"
        fi
        # ... and two genuinely different files must not.
        if [ "$a" = "$t" ]; then
            fail "$base/$tag/canon-distinct" \
                 "two different archives resolved to the same path '$a'"
        fi
        # Invalid inputs must fail by STATUS and by NAME. Either alone is
        # insufficient: a nonzero status without the diagnostic is unattributable,
        # and the diagnostic with status zero is not a failure at all.
        canon_probe "$SH" "$S" fixture "relative/path.a"; rel="$canon_out"; rel_rc="$canon_rc"
        if [ "$rel_rc" = 0 ]; then
            fail "$base/$tag/canon-relative-status" \
                 "a relative path returned success"
        fi
        case "$rel" in
            *"is not an absolute path"*) ;;
            *) fail "$base/$tag/canon-relative" "expected the named absolute-path diagnostic, got: $rel" ;;
        esac
        canon_probe "$SH" "$S" fixture "$CANON_DIR/absent.a"; nx="$canon_out"; nx_rc="$canon_rc"
        if [ "$nx_rc" = 0 ]; then
            fail "$base/$tag/canon-missing-status" \
                 "a nonexistent path returned success"
        fi
        case "$nx" in
            *"does not name an existing file"*) ;;
            *) fail "$base/$tag/canon-missing" "expected the named existing-file diagnostic, got: $nx" ;;
        esac
    done
done

# Preflight inventory oracles. They read the current $out, $counts, $base,
# $tag and $combo, so every block below can use them whichever combination
# ran last.
check_count() {  # <array> <expected>
    local got
    got="$(printf '%s\n' "$counts" | tr ' ' '\n' | grep "^$1=" | cut -d= -f2)"
    if [ "$got" != "$2" ]; then
        fail "$base/$tag/preflight$combo" \
             "array '$1' has $got elements, expected $2"
    fi
}
# out_has <grep option> <pattern>: whether a line of $out matches. grep is
# handed the whole value and reads all of it, so no pipe writer can take
# SIGPIPE under pipefail and turn a match into a miss (or, for an absence
# check, a real presence into a silent pass). A grep error is a failure of its
# own, never an answer.
grep_errors=0
out_has() {
    local rc=0
    grep "$1" -- "$2" <<<"$out" >/dev/null || rc=$?
    case "$rc" in
        0) return 0 ;;
        1) return 1 ;;
        *) grep_errors=$((grep_errors + 1))
           fail "$base/$tag/preflight$combo" "grep exited $rc matching '$2'"
           return 1 ;;
    esac
}
in_array() {  # <array> <exact element> ; must be present
    if ! out_has -xF "PREFLIGHT-ITEM $1 $2"; then
        fail "$base/$tag/preflight$combo" \
             "array '$1' is missing element '$2'"
    fi
}
not_in_array() {  # <array> <exact element> ; must be absent
    if out_has -xF "PREFLIGHT-ITEM $1 $2"; then
        fail "$base/$tag/preflight$combo" \
             "array '$1' unexpectedly contains '$2'"
    fi
}
has_escaped_space() {  # any element still carrying pkg-config's "\ " escape
    out_has -G 'PREFLIGHT-ITEM .*\\ '
}

# The oracles themselves, judged on a known value: the element sits on the
# first line, ahead of more output than one pipe buffer holds, so an oracle
# that lets grep stop reading early misjudges every case here, not only under
# load. Each case must add exactly the failures it names, none of them a grep
# error standing in for the expected one.
oracle_case() {  # <label> <expected new failures> <command...>
    local label="$1" want="$2" before="$failures" errs="$grep_errors" got
    shift 2
    "$@" 2>/dev/null
    got=$((failures - before))
    failures="$before"
    if [ "$got" != "$want" ] || [ "$grep_errors" != "$errs" ]; then
        fail "oracle/$label" "expected $want failure(s) and no grep error, got $got failure(s), $((grep_errors - errs)) grep error(s)"
    fi
}
escape_case() {  # <label> <expected 0|1 for present>
    local got=0 errs="$grep_errors"
    if has_escaped_space; then got=1; fi
    if [ "$got" != "$2" ] || [ "$grep_errors" != "$errs" ]; then
        fail "oracle/$1" "escaped-space predicate returned $got with $((grep_errors - errs)) grep error(s), expected $2 and none"
    fi
}
base=oracle tag=self combo=""
oracle_fill="$(awk 'BEGIN { for (i = 0; i < 65536; i++) print "PREFLIGHT-FILLER " i }')"
out="PREFLIGHT-ITEM prov -DPRESENT=ON"$'\n'"PREFLIGHT-ITEM cc -I/pre\\ flight"$'\n'"$oracle_fill"
oracle_case present-in     0 in_array     prov "-DPRESENT=ON"
oracle_case present-not-in 1 not_in_array prov "-DPRESENT=ON"
oracle_case absent-in      1 in_array     prov "-DABSENT=ON"
oracle_case absent-not-in  0 not_in_array prov "-DABSENT=ON"
escape_case escape-present 1
out="PREFLIGHT-ITEM cc -I/pre flight"$'\n'"$oracle_fill"
escape_case escape-absent  0
out=""
unset oracle_fill

for SH in /bin/bash "$(command -v bash)"; do
    tag="$("$SH" "$SELF" --child-version)"

    # The hazard this gate exists for must still be real on this interpreter,
    # or the gate is vacuous. Probed through this script's own mode.
    if [ "$SH" = /bin/bash ]; then
        if "$SH" "$SELF" --child-empty-array-probe >/dev/null 2>&1; then
            echo "note[$tag]: empty-array expansion no longer fails here" >&2
        fi
    fi

    for S in "$INSTALL_SH" "$PKGCFG_SH"; do
        base="$(basename "$S")"

        # 1. A short call must produce the named argc diagnostic, not an
        #    unbound-variable abort from dereferencing $4.
        expect_named "$base/$tag/argc-3" "$SH" "$S" "expected 20 arguments, got 3" a b c
        # 2. Two short of the contract.
        expect_named "$base/$tag/argc-18" "$SH" "$S" "expected 20 arguments, got 18" \
            cmake x y z p q cc "" "" Debug "" "$WORKDIR" lib \
            openssl ON OFF "" ""
        # 3. And one short of the contract.
        expect_named "$base/$tag/argc-19" "$SH" "$S" "expected 20 arguments, got 19" \
            cmake x y z p q cc "" "" Debug "" "$WORKDIR" lib \
            openssl ON OFF "" "" ""
        # 3. Mandatory provenance present but empty.
        expect_named "$base/$tag/verifier-empty" "$SH" "$S" "mandatory provenance" \
            cmake x y z p q cc "" "" Debug "" "$WORKDIR" lib \
            "" ON OFF "" "" "" /tmp/m
        # 4. Unknown verifier.
        expect_named "$base/$tag/verifier-unknown" "$SH" "$S" "unknown LibMoQ verifier" \
            cmake x y z p q cc "" "" Debug "" "$WORKDIR" lib \
            bogus ON OFF "" "" "" /tmp/m
        # 5. Non-boolean upstream value.
        expect_named "$base/$tag/with-openssl-bad" "$SH" "$S" "WITH_OPENSSL must be ON or OFF" \
            cmake x y z p q cc "" "" Debug "" "$WORKDIR" lib \
            openssl yes OFF "" "" "" /tmp/m

        # EVERY combination of the four optional inputs, in preflight mode.
        # The arrays are judged by EXACT counts and by which array each
        # optional value entered -- not by the presence of a marker.
        for ossl in "" "/opt/ossl"; do
        for pfx  in "" "/opt/pfx"; do
        for core in "" "/opt/core.a"; do
        for mini in "" "/opt/mini.a"; do
            combo="[$ossl|$pfx|$core|$mini]"
            args=()
            while IFS= read -r line; do args+=("$line"); done < <(base_args "$ossl" "$pfx" "$core" "$mini")
            out=""; rc=0
            out="$(MOQ_CONSUMER_PREFLIGHT=1 "$SH" "$S" "${args[@]}" 2>&1)" || rc=$?

            case "$out" in
                *"unbound variable"*)
                    fail "$base/$tag/preflight$combo" "unbound variable" ;;
            esac
            case "$out" in
                *PREFLIGHT-OK*) ;;
                *) fail "$base/$tag/preflight$combo" \
                        "no PREFLIGHT-OK marker (rc=$rc): $(printf '%s' "$out" | head -1)"
                   continue ;;
            esac
            # The exit status is judged INDEPENDENTLY of the marker, the counts
            # and the element inventory: a driver that prints a complete valid
            # preflight and then exits nonzero has not succeeded, and a
            # marker-only oracle would accept it.
            if [ "$rc" != 0 ]; then
                fail "$base/$tag/preflight$combo" \
                     "preflight emitted its inventory but exited $rc"
            fi

            # prov carries the two mandatory booleans plus one entry per
            # non-empty optional input.
            n_opt=0
            [ -n "$ossl" ] && n_opt=$((n_opt + 1))
            [ -n "$pfx" ]  && n_opt=$((n_opt + 1))
            [ -n "$core" ] && n_opt=$((n_opt + 1))
            [ -n "$mini" ] && n_opt=$((n_opt + 1))
            want_prov=$((2 + n_opt))

            counts="$(printf '%s\n' "$out" | grep '^PREFLIGHT-COUNTS ' || true)"
            if [ -z "$counts" ]; then
                fail "$base/$tag/preflight$combo" "no PREFLIGHT-COUNTS line"
                continue
            fi

            check_count prov "$want_prov"
            in_array prov "-DWITH_OPENSSL=ON"
            in_array prov "-DWITH_MBEDTLS=OFF"
            # Each optional value must land in prov exactly when supplied.
            if [ -n "$pfx" ];  then in_array prov "-DCMAKE_PREFIX_PATH=$pfx"
                              else not_in_array prov "-DCMAKE_PREFIX_PATH="; fi
            if [ -n "$core" ]; then in_array prov "-DPTLS_CORE_LIBRARY=$core"
                              else not_in_array prov "-DPTLS_CORE_LIBRARY="; fi
            if [ -n "$mini" ]; then in_array prov "-DPTLS_MINICRYPTO_LIBRARY=$mini"
                              else not_in_array prov "-DPTLS_MINICRYPTO_LIBRARY="; fi
            if [ -n "$ossl" ]; then in_array prov "-DOPENSSL_ROOT_DIR=$ossl"
                              else not_in_array prov "-DOPENSSL_ROOT_DIR="; fi

            if [ "$base" = run_install_consumer_threaded.sh ]; then
                # con: the prefix, plus the OpenSSL root when supplied.
                if [ -n "$ossl" ]; then
                    check_count con 2
                    in_array con "-DOPENSSL_ROOT_DIR=$ossl"
                else
                    check_count con 1
                    not_in_array con "-DOPENSSL_ROOT_DIR="
                fi
                # compile: cc, one synthetic -I, then -c <src> -o <obj>.
                # link:    cc, the object, two synthetic libs, then -o <exe>.
                # Both are built by LATE builders, so a bypassed builder is a
                # missing counts field rather than an identical marker.
                check_count compile 6
                check_count link 6
                in_array compile "-I/preflight/inc"
                in_array link "-L/preflight/lib"
                in_array link "-lmoq"
            else
                # cc: cc, src, one synthetic -I, two synthetic libs, -o <exe>.
                check_count cc 7
                in_array cc "-I/preflight/inc"
                in_array cc "-L/preflight/lib"
                in_array cc "-lmoq"
            fi
        done; done; done; done

        # A runtime search directory (shared installs) enters the link as ONE
        # argument, only when supplied -- including a directory with spaces.
        # The default counts above prove it is absent otherwise.
        combo="[runtime-libdir]"
        rt_dir="/pre flight/lib dir"
        args=()
        while IFS= read -r line; do args+=("$line"); done < <(base_args "" "" "" "")
        out=""; rc=0
        out="$(MOQ_CONSUMER_PREFLIGHT=1 MOQ_PREFLIGHT_RUNTIME_LIBDIR="$rt_dir" \
               "$SH" "$S" "${args[@]}" 2>&1)" || rc=$?
        case "$out" in
            *PREFLIGHT-OK*) ;;
            *) fail "$base/$tag/preflight$combo" "no PREFLIGHT-OK marker (rc=$rc)" ;;
        esac
        if [ "$rc" != 0 ]; then
            fail "$base/$tag/preflight$combo" "preflight emitted its inventory but exited $rc"
        fi
        counts="$(printf '%s\n' "$out" | grep '^PREFLIGHT-COUNTS ' || true)"
        if [ "$base" = run_install_consumer_threaded.sh ]; then
            check_count compile 6
            check_count link 7
            in_array link "-Wl,-rpath,$rt_dir"
            not_in_array compile "-Wl,-rpath,$rt_dir"
        else
            check_count cc 8
            in_array cc "-Wl,-rpath,$rt_dir"
        fi

        # pkg-config escapes a space in a path with a backslash; each escaped
        # path must reach the compiler as ONE word, with the counts unchanged.
        combo="[escaped-space]"
        out=""; rc=0
        out="$(MOQ_CONSUMER_PREFLIGHT=1 MOQ_PREFLIGHT_PC_CFLAGS='-I/pre\ flight/inc' \
               MOQ_PREFLIGHT_PC_LIBS='-L/pre\ flight/lib -lmoq' \
               "$SH" "$S" "${args[@]}" 2>&1)" || rc=$?
        case "$out" in
            *PREFLIGHT-OK*) ;;
            *) fail "$base/$tag/preflight$combo" "no PREFLIGHT-OK marker (rc=$rc): $(printf '%s' "$out" | head -1)" ;;
        esac
        if [ "$rc" != 0 ]; then
            fail "$base/$tag/preflight$combo" "preflight emitted its inventory but exited $rc"
        fi
        counts="$(printf '%s\n' "$out" | grep '^PREFLIGHT-COUNTS ' || true)"
        if [ "$base" = run_install_consumer_threaded.sh ]; then
            check_count compile 6
            check_count link 6
            in_array compile "-I/pre flight/inc"
            in_array link "-L/pre flight/lib"
        else
            check_count cc 7
            in_array cc "-I/pre flight/inc"
            in_array cc "-L/pre flight/lib"
        fi
        # The escape must not survive into any element.
        if has_escaped_space; then
            fail "$base/$tag/preflight$combo" "an element still carries a backslash escape"
        fi

        # pkg-config output the parser would have to guess about fails by name.
        for bad in "-I'/pre/inc" "-I/pre/inc\\"; do
            out=""; rc=0
            out="$(MOQ_CONSUMER_PREFLIGHT=1 MOQ_PREFLIGHT_PC_CFLAGS="$bad" \
                   "$SH" "$S" "${args[@]}" 2>&1)" || rc=$?
            case "$bad" in
                *"'"*) want="contains a quote character" ;;
                *) want="ends in a dangling backslash" ;;
            esac
            case "$out" in
                *"$want"*) ;;
                *) fail "$base/$tag/malformed-pkg-config" "expected '$want' for $bad, got: $(printf '%s' "$out" | head -1)" ;;
            esac
            if [ "$rc" = 0 ]; then
                fail "$base/$tag/malformed-pkg-config" "$bad was accepted"
            fi
        done

        # Preflight must not have created the work directory, and must not
        # have disturbed the isolated root or its sentinel.
        if [ -e "$WORKDIR" ]; then
            fail "$base/$tag/preflight-no-mutation" "preflight created the work dir"
        fi
        if [ ! -d "$SELFTEST_ROOT" ]; then
            fail "$base/$tag/preflight-no-mutation" "the isolated root is gone"
        elif [ "$(cat "$SENTINEL" 2>/dev/null || true)" != "$SENTINEL_BEFORE" ]; then
            fail "$base/$tag/preflight-no-mutation" "the sentinel changed"
        fi
    done
done

if [ "$failures" != 0 ]; then
    echo "consumer_driver_selftest: $failures failure(s)" >&2
    exit 1
fi
echo "consumer_driver_selftest: ok"
