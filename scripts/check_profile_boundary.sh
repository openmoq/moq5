#!/usr/bin/env bash
#
# Verify that generic session core files do not directly reference
# draft-16 request-admission machinery. These belong behind profile
# ops (profile_d16.c) or in codec/wire layers.
#
# This enforces the invariant that generic session code asks the
# profile to prepare/commit/validate/release requests;
# only the profile knows how that maps to draft-16 request IDs or
# future stream-bound requests.
#
# Exit 0 if clean, 1 if violations found.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Session core files (auto-discovered) plus internal helpers.
# Profile implementation files are excluded — they are allowed to
# use D16 wire vocabulary.
SESSION_CORE=()
for f in "$ROOT"/core/src/session/session*.c "$ROOT"/core/src/session/session_internal.h; do
    [ -f "$f" ] || continue
    SESSION_CORE+=( "${f#"$ROOT/"}" )
done
SESSION_CORE+=( core/src/internal/validate.h )

# Request-admission terms that must not appear in session core.
# These are D16-specific fields/constants moved behind profile ops.
# D16 wire vocabulary that must not appear in session core.
FORBIDDEN='next_local_request_id|peer_next_request_id|requests_blocked_at|request_was_blocked|has_max_request_id|\.max_request_id|MOQ_SETUP_PARAM_|moq_kvp_entry_t|MOQ_MSG_PARAM_|MOQ_AUTH_TOKEN_|moq_d16_|MOQ_D16_|moq_control_envelope_t|moq_control_decode_envelope|#include.*moq/control\.h'

# Lines going through profile ops or defining struct fields are allowed.
EXEMPT='profile-boundary-exempt|s->profile->|typedef struct|bool[[:space:]]*has_max_request_id;|uint64_t[[:space:]]*max_request_id;'

violations=0

for file in "${SESSION_CORE[@]}"; do
    path="$ROOT/$file"
    if [ ! -f "$path" ]; then
        continue
    fi
    hits=$(grep -nE "$FORBIDDEN" "$path" 2>/dev/null \
        | grep -vE "$EXEMPT" || true)
    if [ -n "$hits" ]; then
        echo "VIOLATION in $file:"
        echo "$hits" | sed 's/^/  /'
        violations=$((violations + 1))
    fi
done

echo ""
if [ "$violations" -gt 0 ]; then
    echo "FAILED: $violations file(s) reference D16 wire vocabulary."
    echo ""
    echo "Session core must not use D16 wire types, constants, or codecs."
    echo "All draft-specific logic belongs in profile_d16.c."
    echo "Session core must not include moq/control.h."
    exit 1
else
    echo "PASSED: No D16 wire vocabulary in session core."
    exit 0
fi
