#!/usr/bin/env bash
#
# Binary-boundary hygiene for the blocked-arm trio:
#   plain    moq5-relay          — no blocked record, no test seam, env INERT
#   verify   moq-relay-verify   — record + debug seam + constraint seam
#   measure  moq-relay-measure  — constraint seam ONLY (production bind):
#                                 the timing binary must reproduce the
#                                 constrained pools but carry no blocked
#                                 record and no debug counters.
# Usage:
#   relay_verify_hygiene.sh <moq5-relay> <moq-relay-verify> <moq-relay-measure> \
#                           <libmoq-relay-core> <libmoq-relay>
# All five arguments are MANDATORY: the three blocked-arm binaries plus the
# two PRODUCTION libraries. Archives are checked directly because
# final-binary dead stripping can hide a debug symbol that is nonetheless
# compiled into a production library.
set -u

plain="${1:?plain binary}"
verify="${2:?verify binary}"
measure="${3:?measure binary}"
core_a="${4:?production core archive}"
runtime_a="${5:?production relay runtime library}"
fail=0

count_str() { strings "$1" | grep -c "$2"; }
count_sym() { nm "$1" 2>/dev/null | grep -c "$2"; }

# --- production binary must be CLEAN ---
for pat in RELAY_BLOCKED_V0; do
    n=$(count_str "$plain" "$pat")
    if [ "$n" != "0" ]; then
        echo "FAIL: plain moq5-relay contains string '$pat' ($n)"; fail=1
    fi
done
for pat in RELAY_SGDIAG RELAY_SEALLOG RELAY_SEALMETA; do
    n=$(count_str "$plain" "$pat")
    if [ "$n" != "0" ]; then
        echo "FAIL: plain moq5-relay contains string '$pat' ($n)"; fail=1
    fi
done
for sym in moqr_bind_debug moqr_cli_blocked seed_blocked moqr_cli_verify_env \
           moqr_shards_debug_seal moqr_cli_sgdiag moqr_cli_seallog; do
    n=$(count_sym "$plain" "$sym")
    if [ "$n" != "0" ]; then
        echo "FAIL: plain moq5-relay exports symbol '$sym' ($n)"; fail=1
    fi
done

# --- verify binary must CARRY the seam (else the negatives above are vacuous) ---
if [ "$(count_str "$verify" RELAY_BLOCKED_V0)" = "0" ]; then
    echo "FAIL: moq-relay-verify is missing the RELAY_BLOCKED_V0 record"; fail=1
fi
if [ "$(count_sym "$verify" moqr_cli_blocked)" = "0" ]; then
    echo "FAIL: moq-relay-verify is missing the blockedstats symbols"; fail=1
fi
if [ "$(count_sym "$verify" blocked_aggregate)" = "0" ]; then
    echo "FAIL: moq-relay-verify is missing the bind aggregate seam"; fail=1
fi
if [ "$(count_str "$verify" RELAY_SGDIAG)" = "0" ]; then
    echo "FAIL: moq-relay-verify is missing the SGDIAG diagnostics"; fail=1
fi
if [ "$(count_str "$verify" RELAY_SEALMETA)" = "0" ]; then
    echo "FAIL: moq-relay-verify is missing the SEALMETA window header"; fail=1
fi
if [ "$(count_sym "$verify" moqr_shards_debug_seal)" = "0" ]; then
    echo "FAIL: moq-relay-verify is missing the seal-ingest evidence"; fail=1
fi

# --- measure binary: constraint seam yes, record/debug seam NO ---
if [ "$(count_sym "$measure" moqr_cli_verify_env)" = "0" ]; then
    echo "FAIL: moq-relay-measure is missing the constraint seam"; fail=1
fi
if [ "$(count_str "$measure" RELAY_BLOCKED_V0)" != "0" ]; then
    echo "FAIL: moq-relay-measure contains the blocked record"; fail=1
fi
for sym in moqr_bind_debug moqr_cli_blocked seed_blocked \
           moqr_shards_debug_seal moqr_cli_sgdiag moqr_cli_seallog; do
    n=$(count_sym "$measure" "$sym")
    if [ "$n" != "0" ]; then
        echo "FAIL: moq-relay-measure exports symbol '$sym' ($n)"; fail=1
    fi
done
for pat in RELAY_SGDIAG RELAY_SEALLOG RELAY_SEALMETA; do
    if [ "$(count_str "$measure" "$pat")" != "0" ]; then
        echo "FAIL: moq-relay-measure contains string '$pat'"; fail=1
    fi
done

# --- constraint-seam behavior at the binary boundary ---
# (capacity needs no cert/listener, so these run transport-free)
cfg="$(mktemp)"; trap 'rm -f "$cfg"' EXIT
printf '{"listener":{"port":1,"lanes":2}}' > "$cfg"
cfg1="$(mktemp)"; trap 'rm -f "$cfg" "$cfg1"' EXIT
printf '{"listener":{"port":1}}' > "$cfg1"

# plain binary: the verify environment is INERT (identical ceiling, exit 0)
p0=$("$plain" capacity --config "$cfg") || { echo "FAIL: plain capacity"; fail=1; }
p1=$(MOQR_VERIFY_BIND_MAX_OPEN_SUBGROUPS=1 "$plain" capacity --config "$cfg") \
    || { echo "FAIL: plain capacity with env"; fail=1; }
if [ "$p0" != "$p1" ]; then
    echo "FAIL: verify env changed the PLAIN binary's capacity"; fail=1
fi

# verify + measure binaries: the bind constraint shrinks the described
# ceiling by the IDENTICAL amount (same builder, same constraint — symmetric
# timing). The absolute ceilings legitimately differ by verify-only bytes
# (per-conn debug counters in the test-internals bind, blocked snapshot
# fields), so the pin is the constraint-induced DELTA, not the raw output.
ceiling() { sed -n 's/^relay-state allocation-request ceiling: \([0-9]*\) bytes$/\1/p'; }
d_verify=0; d_measure=0
for tool in "$verify" "$measure"; do
    v0=$("$tool" capacity --config "$cfg" | ceiling)
    v1=$(MOQR_VERIFY_BIND_MAX_OPEN_SUBGROUPS=1 "$tool" capacity --config "$cfg" | ceiling)
    if [ -z "$v0" ] || [ -z "$v1" ]; then
        echo "FAIL: $tool capacity ceiling not parseable"; fail=1; continue
    fi
    if [ "$v1" -ge "$v0" ]; then
        echo "FAIL: bind constraint did not shrink $tool's ceiling"; fail=1
    fi
    if [ "$tool" = "$verify" ]; then d_verify=$((v0 - v1)); else d_measure=$((v0 - v1)); fi
done
if [ "$d_verify" != "$d_measure" ] || [ "$d_verify" = "0" ]; then
    echo "FAIL: constraint shrink differs (verify $d_verify vs measure $d_measure)"; fail=1
fi

# both seam binaries: junk refuses closed; lanes<=1 with a constraint refuses
for tool in "$verify" "$measure"; do
    if MOQR_VERIFY_BIND_MAX_OPEN_SUBGROUPS=08 "$tool" capacity --config "$cfg" \
        >/dev/null 2>&1; then
        echo "FAIL: $tool accepted junk env (leading zero)"; fail=1
    fi
    if MOQR_VERIFY_SESSION_MAX_OPEN_SUBGROUPS=1 "$tool" capacity --config "$cfg1" \
        >/dev/null 2>&1; then
        echo "FAIL: $tool accepted a constraint at lanes=1"; fail=1
    fi
done

# --- Every production library must be free of debug symbols. ---
scan_archive() {
    arch="$1"; shift
    if [ ! -f "$arch" ]; then
        echo "FAIL: production archive missing: $arch"; fail=1
        return
    fi
    for sym in "$@"; do
        n=$(nm "$arch" 2>/dev/null | grep -c "$sym")
        if [ "$n" != "0" ]; then
            echo "FAIL: production archive $arch exports '$sym' ($n)"; fail=1
        fi
    done
}
scan_archive "$core_a" moqr_core_debug moqr_bind_debug moqr_shards_debug
scan_archive "$runtime_a" moqr_core_debug moqr_bind_debug moqr_shards_debug

if [ "$fail" = "0" ]; then
    echo "PASS: relay_verify_hygiene"
fi
exit "$fail"
