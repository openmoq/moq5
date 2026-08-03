#!/usr/bin/env bash
#
# check_wtquic_network_runtime.sh - the .wtquicNetwork runtime proof.
#
# Network.framework does NOT establish connections inside an xctest host,
# so the proof runs as a STANDALONE executable (moq-wtquic-network-runtime-proof)
# against fresh wtquic MsQuic MEDIA servers (MSF catalog + LOC track +
# one deterministic object), driving the real Swift vertical
# (LiveMediaSession -> service endpoint -> managed wtquic adapter ->
# Network.framework):
#   reconnect <A> <B> : full draft-16 media exchange (establish +
#                       SUBSCRIBE + exact object bytes + stop) on A,
#                       then the same again on B (same process) --
#                       each its own fresh server.
#   exchange <C> 18   : the same full exchange against a draft-18
#                       server (fresh server, fresh session).
#   untrusted <D>     : certificateUnverified / errSecNotTrusted
#                       (-67843).
#
# Establishment is a SINGLE deterministic verdict against a server that is
# proven alive -- NO retry-for-acceptance (a retry could normalize an
# intermittently broken establishment path). One fresh server (one
# session) per connection.
#
# Server lifecycle is SIGNAL-driven and tracked ENTIRELY in this (parent)
# shell: each server is a background process with a known PID and a
# captured stderr file; it is stopped with SIGTERM (clean teardown) and
# reaped. No command substitution around the spawn (that would run it in a
# subshell and lose the PID), no dynamic-FD / FIFO syntax (unsupported by
# Apple's /bin/bash 3.2), no untracked helper processes.
#
# Opt-in / prerequisites (Apple host, real sockets, never default CI):
#   - a service tree built with the wtquic Network backend AND the wtquic
#     msquic component (the server is a MsQuic attach peer)
#   - MOQ_SERVICE prefix for the Swift build (PKG_CONFIG_PATH)
#
# Usage:
#   MOQ_WTQUIC_NETWORK_BUILD=<service build dir with wtquic_network_smoke_server> \
#   PKG_CONFIG_PATH=<service+wtquic pkgconfig> \
#   scripts/check_wtquic_network_runtime.sh

set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)

log() { printf '[check_wtquic_network_runtime] %s\n' "$*" >&2; }
die() { printf '[check_wtquic_network_runtime] ERROR: %s\n' "$*" >&2; exit 1; }

# Query mode for the harness: print the EXACT scratch key this run
# would use (repo-rooted path keyed by the shared fingerprint) and
# exit. Requires only PKG_CONFIG_PATH.
if [ "${1:-}" = "--print-scratch-key" ]; then
    dep_fp=$("$script_dir"/wtquic_network_dep_fingerprint.sh) \
        || die "dependency fingerprint failed"
    printf '%s\n' "$repo_root/.build-wtquic-network-proof/$dep_fp"
    exit 0
fi

build="${MOQ_WTQUIC_NETWORK_BUILD:-}"
[ -n "$build" ] || die "set MOQ_WTQUIC_NETWORK_BUILD to a service build dir"
server="$build/service/wtquic_network_smoke_server"
[ -x "$server" ] || die "server not built: $server
  (build the service tree with -DMOQ_BUILD_WTQUIC_NETWORK_MANAGED=ON and a
   wtquic package that provides the msquic component)"

# The cleanup trap is installed BEFORE anything it guards exists, so
# an early failure (openssl missing, swift build error) can never leak
# the fixture directory or a server process.
fixture_dir=""
srv_pids=()   # background server PIDs
srv_errs=()   # captured stderr files (dumped on a failed verdict)
srv_ports=()  # bound ports
srv_bad_exit=""   # nonzero server exit observed by stop_all
stop_all() {
    local i st
    for i in "${!srv_pids[@]}"; do
        [ -n "${srv_pids[$i]}" ] || continue
        kill -TERM "${srv_pids[$i]}" 2>/dev/null || true
        if wait "${srv_pids[$i]}" 2>/dev/null; then st=0; else st=$?; fi
        # SIGTERM is the commanded shutdown; anything else is a server
        # verdict the judged flow must see (recorded, not fatal here,
        # so trap-time cleanup preserves the original failure).
        if [ "$st" -ne 0 ] && [ "$st" -ne 143 ]; then
            log "server pid ${srv_pids[$i]} (port ${srv_ports[$i]}) exited $st"
            cat "${srv_errs[$i]}" >&2 2>/dev/null || true
            srv_bad_exit="$st"
        fi
        rm -f "${srv_errs[$i]}" 2>/dev/null || true
    done
    srv_pids=(); srv_errs=(); srv_ports=()
}
cleanup() {
    stop_all
    [ -n "$fixture_dir" ] && rm -rf "$fixture_dir"   # never outlives the run
}
trap cleanup EXIT

# FRESH trust fixture per run, in a private temp dir removed on every
# exit: subjectAltName=IP:127.0.0.1 (the address the client dials), so
# the untrusted flow's only judgeable failure is TRUST-ROOT rejection
# (errSecNotTrusted) -- never a hostname mismatch, and never a stale
# certificate from an earlier run.
fixture_dir=$(mktemp -d)
cert="$fixture_dir/cert.pem"
key="$fixture_dir/key.pem"
command -v openssl >/dev/null 2>&1 || die "openssl not found (need self-signed certs)"
log "generating fresh self-signed cert (SAN IP:127.0.0.1) in $fixture_dir"
openssl req -x509 -newkey rsa:2048 -sha256 -days 2 -nodes \
    -keyout "$key" -out "$cert" -subj /CN=localhost \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" >/dev/null 2>&1 \
    || die "openssl certificate generation failed"

# SwiftPM does NOT track external static archives: a proof binary
# linked against an older wtquic/libmoq install would be silently
# reused and judged. Key the scratch path on THE shared dependency
# fingerprint (the resolved libmoq-service pkg-config closure: flags
# plus every resolved archive's contents — see
# scripts/wtquic_network_dep_fingerprint.sh, the single implementation), so
# replacing any link input forces a fresh link (and stale scratch
# trees are pruned).
dep_fp=$("$script_dir"/wtquic_network_dep_fingerprint.sh) \
    || die "dependency fingerprint failed"
scratch_root="$repo_root/.build-wtquic-network-proof"
scratch="$scratch_root/$dep_fp"
if [ -d "$scratch_root" ]; then
    for d in "$scratch_root"/*; do
        [ "$d" = "$scratch" ] || rm -rf "$d"
    done
fi
log "building moq-wtquic-network-runtime-proof (deps fingerprint $dep_fp)"
MOQ_SERVICE=1 swift build --package-path "$repo_root" \
    --scratch-path "$scratch" \
    --product moq-wtquic-network-runtime-proof >&2
bin="$(MOQ_SERVICE=1 swift build --package-path "$repo_root" \
        --scratch-path "$scratch" \
        --product moq-wtquic-network-runtime-proof --show-bin-path)/moq-wtquic-network-runtime-proof"
[ -x "$bin" ] || die "runtime-proof binary not produced"

# --- server pool: all state lives in THESE parent-shell arrays ----------

# start_server [VERSION] : spawn a fresh server (draft VERSION, default
# 16), wait until it prints PORT and is alive; appends to the arrays.
# Runs in the PARENT shell (never $()).
start_server() {
    local outf errf pid port i ver idx
    ver="${1:-16}"
    outf=$(mktemp); errf=$(mktemp)
    "$server" --cert "$cert" --key "$key" --version "$ver" \
        >"$outf" 2>"$errf" &
    pid=$!
    # TRACK IMMEDIATELY: the child is under the cleanup trap from this
    # point, so no failure path below (die on no-PORT included) can
    # leave a live orphan behind.
    idx=${#srv_pids[@]}
    srv_pids+=("$pid"); srv_errs+=("$errf"); srv_ports+=("?")
    port=""
    for ((i = 0; i < 100; i++)); do
        port=$(sed -n 's/^PORT=//p' "$outf" 2>/dev/null | head -1)
        [ -n "$port" ] && break
        if ! kill -0 "$pid" 2>/dev/null; then
            log "server stderr:"; cat "$errf" >&2
            rm -f "$outf"; die "server exited before printing PORT"
        fi
        sleep 0.1
    done
    rm -f "$outf"
    [ -n "$port" ] || { cat "$errf" >&2; die "server did not print PORT within 10s"; }
    srv_ports[$idx]="$port"
    sleep 0.5   # explicit readiness settle (listener fully accepting)
}

# Assert every recorded server is alive right now (before/around a client run).
assert_servers_alive() {
    local i
    for i in "${!srv_pids[@]}"; do
        kill -0 "${srv_pids[$i]}" 2>/dev/null \
            || { cat "${srv_errs[$i]}" >&2; die "server pid ${srv_pids[$i]} died before the flow completed"; }
    done
}

dump_server_errs() {
    local i
    for i in "${!srv_errs[@]}"; do
        log "server[$i] port ${srv_ports[$i]} stderr:"; cat "${srv_errs[$i]}" >&2 || true
    done
}

# reconnect: two fresh draft-16 servers, ONE executable process (full
# media exchange + stop, then the same again = same-process reconnect)
start_server 16   # index 0 -> connect + exchange + stop
start_server 16   # index 1 -> reconnect + exchange + stop
assert_servers_alive
log "reconnect flow: fresh servers ${srv_ports[0]} (connect+stop) and ${srv_ports[1]} (reconnect)"
if ! "$bin" reconnect "${srv_ports[0]}" "${srv_ports[1]}"; then
    dump_server_errs; die "reconnect flow failed"
fi
assert_servers_alive   # both must have stayed up for the whole flow
stop_all
[ -z "$srv_bad_exit" ] || die "a reconnect-flow server exited $srv_bad_exit"

# draft-18: its own fresh server and session
start_server 18
assert_servers_alive
log "draft-18 flow: fresh server ${srv_ports[0]}"
if ! "$bin" exchange "${srv_ports[0]}" 18; then
    dump_server_errs; die "draft-18 exchange failed"
fi
assert_servers_alive
stop_all
[ -z "$srv_bad_exit" ] || die "the draft-18 server exited $srv_bad_exit"

# untrusted: its own fresh server (the SAN-correct fixture isolates
# trust-root rejection as the only judgeable failure)
start_server 16
assert_servers_alive
log "untrusted flow: fresh server ${srv_ports[0]}"
if ! "$bin" untrusted "${srv_ports[0]}"; then
    dump_server_errs; die "untrusted flow failed"
fi
assert_servers_alive
stop_all
[ -z "$srv_bad_exit" ] || die "the untrusted-flow server exited $srv_bad_exit"

log "PROOF PASS: .wtquicNetwork d16 exchange + reconnect + d18 exchange + untrusted"
