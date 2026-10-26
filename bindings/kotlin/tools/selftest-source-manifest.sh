#!/bin/sh
# Proves the source manifest cannot mint an identity from a FAILED hash producer.
#
# The defect this pins: `_h=$(sha "$f" | cut -f1)` reports `cut`'s status, so a
# hasher that printed a plausible 64-hex digest and exited nonzero produced a
# complete manifest and exit 0 (chats/libmoq/1240). A digest that looks well
# formed is exactly the case a "the value is empty" check cannot catch, so the
# probes below all emit VALID-LOOKING output and fail only by status.
#
# The manifest has TWO producer invocations -- per file, and the final digest
# over the manifest lines -- and either could mask the other, so they are
# probed separately as well as together.
#
# It mutates nothing: each probe only prepends a scratch directory holding a
# fake `shasum` to PATH for one child process. The real candidate tree is never
# written, and the manifest identity is measured before and after to prove it.
#
# Fail-closed: if a fake cannot be created, is not executable, does not actually
# shadow the real hasher, or the scratch cannot be removed, that is a FAILURE --
# never a silently skipped case.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
for t in mktemp rm cat chmod grep sed command; do
    command -v "$t" >/dev/null 2>&1 || { echo "$_self: required tool not found: $t" >&2; exit 2; }
done

case $0 in
    */*) _selfdir=${0%/*} ;;
    *)   _selfdir=. ;;
esac
here=$(cd "$_selfdir" && pwd) || { echo "$_self: cannot resolve the script directory" >&2; exit 2; }
MANIFEST="$here/source-manifest.sh"
[ -x "$MANIFEST" ] || { echo "$_self: no executable manifest tool at $MANIFEST" >&2; exit 2; }

REAL_SHA=$(command -v shasum 2>/dev/null) || REAL_SHA=""
[ -n "$REAL_SHA" ] || REAL_SHA=$(command -v sha256sum 2>/dev/null) || REAL_SHA=""
[ -n "$REAL_SHA" ] || { echo "$_self: no real hash producer to delegate to" >&2; exit 2; }

work=$(mktemp -d "${TMPDIR:-/tmp}/moq5-manifest-selftest-XXXXXXXX") \
    || { echo "$_self: cannot create a task-owned scratch" >&2; exit 2; }
# shellcheck disable=SC2329  # invoked from the traps below
cleanup() {
    rm -rf -- "$work" || { echo "$_self: could not remove the scratch $work" >&2; return 1; }
    return 0
}
trap 'cleanup; exit 130' HUP INT TERM

fails=0
pass() { printf 'PASS[%s]: %s\n' "$1" "$2"; }
fail() { fails=$((fails + 1)); printf 'FAIL[%s]: %s\n' "$1" "$2"; }

# The identity before any probe runs. Every probe must leave it unchanged.
if before=$("$MANIFEST" 2>"$work/berr") && [ -n "$before" ]; then
    :
else
    echo "$_self: the manifest does not succeed unmutated, so no probe can be attributed:" >&2
    sed -n '1,5p' "$work/berr" >&2
    cleanup
    exit 2
fi

# -- the baseline itself is a case: a real run yields a full 64-hex identity ---
_d=$(printf '%s\n' "$before" | sed -n 's/.*digest \([0-9a-f]*\)$/\1/p')
if [ "${#_d}" -eq 64 ]; then
    pass manifest.baseline "an unmutated run yields a full 64-hex identity"
else
    fail manifest.baseline "an unmutated run yielded a ${#_d}-character digest, expected 64"
fi

# mk_fake <name> <body-file-marker>: writes an executable fake `shasum`, then
# PROVES it shadows the real one before any probe trusts it.
mk_fake() {
    _mf_dir="$work/$1"
    mkdir -p "$_mf_dir" || return 1
    cat > "$_mf_dir/shasum" || return 1
    chmod +x "$_mf_dir/shasum" || return 1
    [ -x "$_mf_dir/shasum" ] || return 1
    _mf_seen=$(PATH="$_mf_dir:$PATH" command -v shasum 2>/dev/null) || return 1
    [ "$_mf_seen" = "$_mf_dir/shasum" ] || return 1
    return 0
}

# probe <id> <dir> <description>
# Requires: nonzero status, a named diagnostic on stderr, and NO manifest line
# on stdout -- a failed producer must never yield an identity.
probe() {
    _p_id=$1; _p_dir=$2; _p_desc=$3
    PATH="$work/$_p_dir:$PATH" "$MANIFEST" > "$work/out" 2> "$work/err"
    _p_rc=$?
    if [ "$_p_rc" -eq 0 ]; then
        fail "$_p_id" "$_p_desc: exited 0"
        return
    fi
    if grep -q 'android source manifest' "$work/out"; then
        fail "$_p_id" "$_p_desc: emitted a manifest identity anyway: $(sed -n '1p' "$work/out")"
        return
    fi
    if ! grep -q "^source-manifest.sh: " "$work/err"; then
        fail "$_p_id" "$_p_desc: exited $_p_rc with no named diagnostic"
        return
    fi
    pass "$_p_id" "$_p_desc: $(sed -n '1p' "$work/err")"
}

# 1. the 1240 reproducer -- valid-looking digest, nonzero status, both paths
if mk_fake both <<'EOF'
#!/bin/sh
printf '0000000000000000000000000000000000000000000000000000000000000000  -\n'
exit 1
EOF
then
    probe manifest.producer-status both "a hasher printing a valid-looking digest and exiting 1"
else
    fail manifest.producer-status "could not install the failing-hasher fake"
fi

# 2. per-file path ONLY -- the final digest would still succeed
if mk_fake perfile <<EOF
#!/bin/sh
for a; do
    case \$a in -a|256) ;; *) printf '1111111111111111111111111111111111111111111111111111111111111111  %s\n' "\$a"; exit 1 ;; esac
done
exec "$REAL_SHA" "\$@"
EOF
then
    probe manifest.producer-status.per-file perfile "a hasher failing only on a named file"
else
    fail manifest.producer-status.per-file "could not install the per-file fake"
fi

# 3. final-digest path ONLY -- every per-file hash succeeds first, so a
#    per-file check cannot be what catches this
if mk_fake final <<EOF
#!/bin/sh
for a; do
    case \$a in -a|256) ;; *) exec "$REAL_SHA" "\$@" ;; esac
done
printf '2222222222222222222222222222222222222222222222222222222222222222  -\n'
exit 1
EOF
then
    probe manifest.producer-status.final final "a hasher failing only on the manifest digest"
else
    fail manifest.producer-status.final "could not install the final-digest fake"
fi

# 4. SUCCESSFUL producer, truncated digest -- status alone cannot catch this
if mk_fake short <<'EOF'
#!/bin/sh
printf '0123456789abcdef  -\n'
EOF
then
    probe manifest.digest-shape.short short "a successful hasher emitting a 16-character digest"
else
    fail manifest.digest-shape.short "could not install the short-digest fake"
fi

# 5. SUCCESSFUL producer, diagnostic text where a digest belongs
if mk_fake nonhex <<'EOF'
#!/bin/sh
printf 'warning: could not read input\n'
EOF
then
    probe manifest.digest-shape.nonhex nonhex "a successful hasher emitting non-hexadecimal text"
else
    fail manifest.digest-shape.nonhex "could not install the non-hex fake"
fi

# 6. SUCCESSFUL producer, a digest of the right LENGTH but the wrong alphabet.
#    The length check cannot catch this one, so it is what makes the
#    hexadecimal rule load-bearing rather than incidental.
if mk_fake wide <<'EOF'
#!/bin/sh
printf 'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz  -\n'
EOF
then
    probe manifest.digest-shape.alphabet wide "a successful hasher emitting 64 non-hexadecimal characters"
else
    fail manifest.digest-shape.alphabet "could not install the wide non-hex fake"
fi

# 7. SUCCESSFUL producer, several digests -- only one may be the identity
if mk_fake multiline <<'EOF'
#!/bin/sh
printf '3333333333333333333333333333333333333333333333333333333333333333  a\n'
printf '4444444444444444444444444444444444444444444444444444444444444444  b\n'
EOF
then
    probe manifest.digest-shape.multiline multiline "a successful hasher emitting several digests"
else
    fail manifest.digest-shape.multiline "could not install the multi-line fake"
fi

# -- the candidate tree was not mutated by any probe -------------------------
after=$("$MANIFEST" 2>/dev/null)
if [ "$after" = "$before" ]; then
    pass manifest.no-mutation "the candidate identity is unchanged after every probe"
else
    fail manifest.no-mutation "the candidate identity changed: '$before' -> '$after'"
fi

# -- restoration is itself a checked stage, not a best effort ----------------
if cleanup; then
    trap - EXIT HUP INT TERM
    pass manifest.restore "the probe scratch was removed"
else
    fails=$((fails + 1))
    printf 'FAIL[%s]: %s\n' manifest.restore "the probe scratch could not be removed"
fi

if [ "$fails" -eq 0 ]; then
    printf '\nsource manifest self-test: 0 failure(s)\n'
    exit 0
fi
printf '\nsource manifest self-test: %s failure(s)\n' "$fails"
exit 1
