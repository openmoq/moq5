#!/usr/bin/env bash
# Classify every diagnostic of one cargo invocation and fail on any this
# build owns: a thin wrapper over diagnostics.py (the rules are there).
#
#   diagnostics.sh <cargo json stdout> <cargo stderr> <inventory out> <allowlist> [label]
#
# PYTHON names the interpreter (default python3), run in isolated mode.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
JSON=${1:?cargo json stdout}; STDERR=${2:?cargo stderr}; OUT=${3:?inventory out}; ALLOW=${4:?allowlist}
LABEL=${5:-$JSON}
"${PYTHON:-python3}" -I "$HERE/diagnostics.py" --json "$JSON" --stderr "$STDERR" \
    --inventory "$OUT" --allow "$ALLOW" --label "$LABEL"
