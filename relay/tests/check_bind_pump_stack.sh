#!/usr/bin/env bash
set -euo pipefail

src=${1:?moqr_bind.c path required}

fail=0
flat=$(tr '\n' ' ' < "$src")
need() {
    local pattern=$1
    local label=$2
    if ! rg -n -- "$pattern" "$src" >/dev/null; then
        echo "FAIL: missing $label"
        fail=1
    fi
}
need_flat() {
    local pattern=$1
    local label=$2
    if ! printf '%s\n' "$flat" | rg -- "$pattern" >/dev/null; then
        echo "FAIL: missing $label"
        fail=1
    fi
}
reject() {
    local pattern=$1
    local label=$2
    if rg -n -- "$pattern" "$src" >/dev/null; then
        echo "FAIL: found $label"
        fail=1
    fi
}

need '^#define BIND_PUMP_EVENT_BATCH 16u$' "event batch constant"
need '^#define BIND_PUMP_REVOKED_BATCH 16u$' "revoked-grant batch constant"
need '^#define BIND_PUMP_INTENT_BATCH 32u$' "intent batch constant"
need 'moq_event_t[[:space:]]+\*pump_events;' "bind-owned event scratch"
need 'moqr_revoked_grant_t[[:space:]]+\*pump_revoked;' \
     "bind-owned revoked-grant scratch"
need 'moqr_intent_t[[:space:]]+\*pump_intents;' "bind-owned intent scratch"
need 'bool[[:space:]]+in_pump;' "non-reentrant pump guard"
need 'bool[[:space:]]+intent_drain_active;' "intent-drain reentry guard"
need_flat 'moq_session_poll_events\(cn->session,[[:space:]]*b->pump_events,[[:space:]]*BIND_PUMP_EVENT_BATCH\)' \
          "event polling through bind scratch"
need_flat 'moqr_core_peek_revoked_grants\(b->core,[[:space:]]*b->pump_revoked,[[:space:]]*BIND_PUMP_REVOKED_BATCH\)' \
          "revocation peek through bind scratch"
need_flat 'moqr_core_poll_intents\(b->core,[[:space:]]*b->pump_intents,[[:space:]]*BIND_PUMP_INTENT_BATCH\)' \
          "intent polling through bind scratch"
need_flat 'bind_execute_intents\(moqr_bind_t \*b,[[:space:]]*uint64_t now_us\)[[:space:]]*\{[[:space:]]*if[[:space:]]*\(b->intent_drain_active\)[[:space:]]*\{[^}]*return false;' \
          "active intent drain rejects recursive drain"
need_flat 'b->intent_drain_active = true;.*b->intent_drain_active = false;[[:space:]]*return !blocked;' \
          "intent drain flag bracket"
need 'bind_retry_may_drain_intents\(moqr_bind_t \*b\)' \
     "retry helper intent-drain guard"
need_flat 'if[[:space:]]*\(!bind_retry_may_drain_intents\(b\)\)[[:space:]]*\{.*break;.*\(void\)bind_execute_intents\(b, now_us\);' \
          "BIND_CALL_RETRY skips recursive intent drain"
need_flat 'bool[[:space:]]+detach_retried = false;.*detach_retried = true;.*if[[:space:]]*\(detach_retried\)[[:space:]]*\{[[:space:]]*\(void\)bind_execute_intents\(b, now_us\);[[:space:]]*\}' \
          "post-detach intent drain"
reject 'moq_event_t[[:space:]]+[A-Za-z_][A-Za-z0-9_]*\[[[:space:]]*16[[:space:]]*\]' \
       "automatic 16-event pump batch"
reject 'moqr_revoked_grant_t[[:space:]]+[A-Za-z_][A-Za-z0-9_]*\[[[:space:]]*16[[:space:]]*\]' \
       "automatic 16-revoked-grant pump batch"
reject 'moqr_intent_t[[:space:]]+[A-Za-z_][A-Za-z0-9_]*\[[[:space:]]*32[[:space:]]*\]' \
       "automatic 32-intent pump batch"

if [ "$fail" -ne 0 ]; then
    exit 1
fi
echo "PASS: bind pump scratch source gate"
