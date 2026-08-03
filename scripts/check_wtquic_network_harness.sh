#!/usr/bin/env bash
#
# check_wtquic_network_harness.sh - self-tests for the wtquic_network dependency/runtime
# harness itself (the guards that keep the runtime proof honest):
#
#   1. DIRTY PIN: a pinned wtquic checkout with an untracked file (HEAD
#      still exactly the pin) must fail verification BEFORE anything
#      compiles, printing the pin and the dirty-checkout diagnosis.
#   2. ORPHAN GUARD: a server that never prints PORT must not survive
#      the driver's timeout death — after the run no child remains.
#   3. STALE LINKAGE: the RUNTIME-SELECTED scratch key (queried from
#      check_wtquic_network_runtime.sh --print-scratch-key; the algorithm is
#      never re-derived here) must change when a dependency archive is
#      replaced and return after byte-exact restoration — proven on a
#      TEMPORARY COPY of the prefixes, never on the installed ones.
#
# This is an acceptance GATE: every case must run and pass. Missing
# prerequisites (MOQ_WTQUIC_NETWORK_BUILD, PKG_CONFIG_PATH) FAIL the gate unless
# --allow-skip was passed explicitly (developer convenience only).
#
# Usage:
#   MOQ_WTQUIC_NETWORK_BUILD=<service build dir> \
#   PKG_CONFIG_PATH=<service+wtquic pkgconfig> \
#   scripts/check_wtquic_network_harness.sh [--allow-skip]

set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)

log() { printf '[check_wtquic_network_harness] %s\n' "$*" >&2; }
die() { printf '[check_wtquic_network_harness] ERROR: %s\n' "$*" >&2; exit 1; }

allow_skip=0
[ "${1:-}" = "--allow-skip" ] && allow_skip=1
skipped=0
skip_or_die() {
    if [ "$allow_skip" = 1 ]; then
        log "SKIP: $*"
        skipped=1
    else
        die "$* (pass --allow-skip to tolerate; the gate requires all cases)"
    fi
}

checkout="$repo_root/.deps/wtquic-ci/wtquic"
[ -d "$checkout/.git" ] || die "no pinned checkout; run scripts/setup_wtquic_deps.sh first"
pin=$(sed -n 's/^WTQUIC_REF="\${WTQUIC_REF:-\(.*\)}"$/\1/p' \
    "$script_dir/setup_wtquic_deps.sh" | head -1)
[ -n "$pin" ] || die "cannot read the pin"
head_sha=$(git -C "$checkout" rev-parse HEAD)
[ "$head_sha" = "$pin" ] || die "checkout is not at the pin ($head_sha); re-run setup first"

# Prerequisites are validated UP FRONT: the gate fails here, before
# any case runs, rather than letting a later case fail for a
# misleading downstream reason.
have_build=1
if [ -z "${MOQ_WTQUIC_NETWORK_BUILD:-}" ] || \
   [ ! -x "${MOQ_WTQUIC_NETWORK_BUILD:-}/service/wtquic_network_smoke_server" ]; then
    skip_or_die "MOQ_WTQUIC_NETWORK_BUILD is unset or has no wtquic_network_smoke_server"
    have_build=0
fi
have_pcp=1
if [ -z "${PKG_CONFIG_PATH:-}" ]; then
    skip_or_die "PKG_CONFIG_PATH is unset (needed by cases 2 and 3)"
    have_pcp=0
fi

# --- 1: dirty pinned checkout fails verification pre-compile ---------------
marker="$checkout/HARNESS_DIRTY_MARKER"
cleanup_marker() { rm -f "$marker"; }
trap cleanup_marker EXIT
echo dirty > "$marker"

out=$("$script_dir"/setup_wtquic_deps.sh 2>&1 >/dev/null) && \
    die "setup accepted a DIRTY pinned checkout"
echo "$out" | grep -q "DIRTY" || die "setup failure lacks the dirty diagnosis: $out"
echo "$out" | grep -q "$pin" || die "setup failure lacks the pin: $out"

out=$(MOQ_IOS_OPENSSL_ROOT=/nonexistent \
      "$script_dir"/build_ios_xcframework.sh 2>&1 >/dev/null) && \
    die "xcframework build accepted a DIRTY pinned checkout"
echo "$out" | grep -q "DIRTY" || die "xcframework failure lacks the dirty diagnosis"
echo "$out" | grep -q "$pin" || die "xcframework failure lacks the pin"
echo "$out" | grep -q "configure wtquic" && \
    die "xcframework verification ran AFTER compilation started"
rm -f "$marker"
log "PASS: dirty pinned checkout refused pre-compile (both scripts)"

# --- 2: a PORT-silent server never survives the driver ----------------------
build="${MOQ_WTQUIC_NETWORK_BUILD:-}"
if [ "$have_build" = 1 ] && [ "$have_pcp" = 1 ]; then
    stub_dir=$(mktemp -d)
    mkdir -p "$stub_dir/service"
    tag="wtquic-network-harness-stub-$$"
    cat > "$stub_dir/service/wtquic_network_smoke_server" <<STUB
#!/usr/bin/env bash
# $tag : never prints PORT; sleeps forever unless killed
exec -a "$tag" sleep 600
STUB
    chmod +x "$stub_dir/service/wtquic_network_smoke_server"
    log "orphan case: running the driver against a PORT-silent stub (~10s)"
    if MOQ_WTQUIC_NETWORK_BUILD="$stub_dir" "$script_dir"/check_wtquic_network_runtime.sh \
        > "$stub_dir/run.log" 2>&1; then
        die "driver PASSED against a PORT-silent server"
    fi
    grep -q "did not print PORT" "$stub_dir/run.log" \
        || die "driver failed for the wrong reason: $(tail -3 "$stub_dir/run.log")"
    sleep 0.5
    if pgrep -f "$tag" > /dev/null 2>&1; then
        pkill -f "$tag" 2>/dev/null || true
        die "the PORT-silent server SURVIVED the driver (orphan)"
    fi
    rm -rf "$stub_dir"
    log "PASS: PORT-silent server reaped on the timeout path"
else
    log "SKIP: orphan case (missing prerequisite, reported above)"
    skipped=1
fi

# --- 3: replacing a dependency archive re-keys the RUNTIME's scratch --------
if [ "$have_pcp" = 1 ]; then
    # Work on TEMPORARY COPIES of every prefix in PKG_CONFIG_PATH: the
    # installed dependency trees are never mutated (the .pc files are
    # prefix-relative/relocatable, so the copies resolve on their own).
    # The copies live under a directory WITH A SPACE, so this case is
    # load-bearing for the fingerprint's escaped-argument parsing too:
    # pkg-config shell-escapes such paths, and a splitting parser
    # resolves ZERO archives here.
    tmp_root=$(mktemp -d)
    cleanup_tmp() { rm -rf "$tmp_root"; }
    trap 'cleanup_marker; cleanup_tmp' EXIT
    spaced_root="$tmp_root/relocated prefixes"
    mkdir -p "$spaced_root"
    tmp_pcp=""
    i=0
    IFS=: read -r -a pc_entries <<< "$PKG_CONFIG_PATH"
    for pc in "${pc_entries[@]}"; do
        prefix=$(cd "$pc/../.." && pwd)
        copy="$spaced_root/prefix-$i"
        cp -R "$prefix" "$copy"
        tmp_pcp="${tmp_pcp:+$tmp_pcp:}$copy/lib/pkgconfig"
        i=$((i + 1))
    done

    query_key() {
        PKG_CONFIG_PATH="$tmp_pcp" \
            "$script_dir"/check_wtquic_network_runtime.sh --print-scratch-key
    }
    key1=$(query_key) || die "runtime scratch-key query failed"

    # replace one archive the closure resolves (contents change)
    target=$(find "$tmp_root" -name 'libmoq-service.a' | head -1)
    [ -n "$target" ] || die "no libmoq-service.a in the copied closure"
    cp "$target" "$target.orig"
    printf '\0' >> "$target"
    key2=$(query_key) || die "runtime scratch-key query failed post-replace"
    [ "$key1" != "$key2" ] || die \
        "the RUNTIME-selected scratch key did not change after the archive \
was replaced (a constant or stale key would silently reuse an old link)"

    # byte-exact restoration returns the exact original key
    mv "$target.orig" "$target"
    key3=$(query_key) || die "runtime scratch-key query failed post-restore"
    [ "$key3" = "$key1" ] || die "restored archive did not restore the key"

    cleanup_tmp
    trap cleanup_marker EXIT
    log "PASS: runtime scratch key tracks the archive under a spaced prefix ($key1 -> $key2 -> restored)"
else
    log "SKIP: linkage case (missing prerequisite, reported above)"
    skipped=1
fi

if [ "$skipped" = 1 ]; then
    log "HARNESS INCOMPLETE: one or more cases were skipped (--allow-skip)"
    exit 0
fi
log "HARNESS PASS (all three cases executed)"
