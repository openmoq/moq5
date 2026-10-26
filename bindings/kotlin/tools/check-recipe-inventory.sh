#!/bin/sh
# Source-derived inventory of the cross-build recipe's command substitutions.
#
# `1145` found the hand-counted figures in an earlier report wrong. This script
# derives them from the source instead, so an edit that adds or removes a
# substitution without updating the durable record fails here rather than
# silently invalidating a number in a report.
#
# It classifies nothing on its own: the CLASSIFICATION lives in the recipe's
# comments and in chats/libmoq/1148. What this pins is the arithmetic.
set -u
here=$(cd "$(dirname "$0")" && pwd) || { echo "cannot resolve script dir" >&2; exit 2; }
f="$here/build-native-crossbuild.sh"
[ -f "$f" ] || { echo "FAIL[inventory.source]: missing $f" >&2; exit 1; }

# Expected, measured against the reviewed source. Update these together with
# the classification in the report -- never one without the other.
# Measured against the reviewed source. History: Codex measured 37 / 39 / 52 on
# the 1144 source; the 1145 guarded reads and archive inventories moved it to
# 39 / 41 / 56 with one comment token; 1147 reworded that lone comment so NO
# `$(` token remains inside a comment, which makes total == executable exactly
# rather than by coincidence.
WANT_ASSIGN_LINES=39
WANT_ASSIGN_OCCURRENCES=41
WANT_DOLLAR_PAREN=55          # every one of them executable -- see below
WANT_COMMENT_TOKENS=0

fails=0
chk() {  # chk <label> <got> <want>
    if [ "$2" = "$3" ]; then
        printf 'PASS[%s]: %s\n' "$1" "$2"
    else
        printf 'FAIL[%s]: %s, expected %s\n' "$1" "$2" "$3"; fails=$((fails + 1))
    fi
}

# shellcheck disable=SC2016  # literal pattern text; expansion would be wrong
al=$(grep -c '=\$(' "$f") || { echo "FAIL[inventory.read]: cannot read $f" >&2; exit 1; }
# shellcheck disable=SC2016  # literal pattern text; expansion would be wrong
ao=$(grep -o '=\$(' "$f" | awk 'END { print NR }') || { echo "FAIL[inventory.read]: cannot read $f" >&2; exit 1; }
# shellcheck disable=SC2016  # literal pattern text; expansion would be wrong
dp=$(grep -o '\$(' "$f" | awk 'END { print NR }') || { echo "FAIL[inventory.read]: cannot read $f" >&2; exit 1; }
# shellcheck disable=SC2016  # literal pattern text; expansion would be wrong
# A real TOKEN count, not a count of comment LINES: the previous shape happened
# to agree only because the single comment token sat on one full-line comment.
ct=$(grep -o '^[[:space:]]*#.*' "$f" | grep -o '\$(' | awk 'END { print NR }') \
    || { echo "FAIL[inventory.read]: cannot read $f" >&2; exit 1; }

chk inventory.assignment.lines       "$al" "$WANT_ASSIGN_LINES"
chk inventory.assignment.occurrences "$ao" "$WANT_ASSIGN_OCCURRENCES"
chk inventory.dollar_paren.total     "$dp" "$WANT_DOLLAR_PAREN"
chk inventory.comment.tokens         "$ct" "$WANT_COMMENT_TOKENS"
# With zero comment tokens, total and executable must be identical. This is
# asserted, not assumed.
chk inventory.executable.equals_total "$((dp - ct))" "$dp"
printf 'executable command substitutions: %s\n' "$((dp - ct))"
printf 'recipe substitution inventory: %s failure(s)\n' "$fails"
[ "$fails" -eq 0 ] || exit 1
