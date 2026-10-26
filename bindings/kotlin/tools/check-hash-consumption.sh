#!/bin/sh
# Refuses any use of a hash or single-value producer whose failure status another
# command would mask.
#
# The invariant, stated exactly: NO HASH OR REQUIRED-VALUE RESULT IS CONSUMED
# WHERE ANOTHER COMMAND'S STATUS MASKS THE PRODUCER'S FAILURE.
#
# It is NOT "no producer inside a command substitution". Assignment form is
# correct and required -- `_h=$(decl_sha256 "$f") || die ...` carries the
# hasher's status into the assignment, which the shell then reports. What is
# forbidden is a substitution whose value is consumed by some OTHER command,
# because that command's own success is what the shell reports:
#
#     printf 'k=%s\n' "$(decl_sha256 "$f")"     # printf succeeded; the hash may not have
#     : > "$out" "$(decl_sha256 "$f")"          # same masking, different verb
#
# A helper that aborts on failure does not fix this either: `$( )` runs in a
# SUBSHELL, so an `exit` inside it ends only that subshell and leaves the caller
# running with an empty value.
#
# This is a source oracle, so it is exact and fail-closed: an unparseable line,
# an unreadable file, or an empty file list is a failure, never an empty pass.
set -u
LC_ALL=C; export LC_ALL

for t in awk sed grep basename mktemp rm; do
    command -v "$t" >/dev/null 2>&1 || { echo "check-hash-consumption: required tool not found: $t" >&2; exit 2; }
done

usage() { echo "usage: $(basename "$0") FILE [FILE...]" >&2; exit 2; }
[ $# -ge 1 ] || usage
for f in "$@"; do
    case $f in -*) usage ;; esac
    [ -r "$f" ] || { echo "check-hash-consumption: not readable: $f" >&2; exit 2; }
done

scratch=$(mktemp -d "${TMPDIR:-/tmp}/moq5-hashscan-XXXXXXXX") \
    || { echo "check-hash-consumption: cannot create a task-owned scratch" >&2; exit 2; }
# shellcheck disable=SC2329  # invoked from the traps below
cleanup() { rm -rf -- "$scratch"; }
trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

fails=0
report() { echo "FAIL[hash-consumption]: $1"; fails=$((fails + 1)); }

# The producers whose status must never be masked: the hashers, and the
# exactly-one value readers built on top of the raw property accessors.
#
# props_value is deliberately NOT here. It is a raw accessor whose "no such key"
# answer is legitimately an empty string, so callers check the VALUE; the
# required-value contract lives in props_one/prov_of, which are listed. A
# producer is matched only as a whole word immediately inside a substitution, so
# a longer name that merely ends with one of these is not mistaken for it.
PRODUCERS='decl_sha256|hash_of|props_one|prov_of|inputs_digest'

for f in "$@"; do
    # Producers are found first, and every occurrence must then be classified.
    # A line that holds one and matches no accepted shape is a failure, so a
    # shape this oracle does not model cannot pass by falling through.
    if ! awk -v prods="$PRODUCERS" -v file="$f" '
        # strip a trailing comment only when the whole line is a comment: a
        # producer inside a real command must never be excused by a later "#"
        /^[[:space:]]*#/ { next }
        {
            line = $0
            if (line !~ "\\$\\((" prods ")[ )]") next

            body = line
            sub(/^[[:space:]]+/, "", body)

            # (1) the definition of one of the helpers themselves
            if (body ~ "^(" prods ")\\(\\)") next

            # (2) assignment form: NAME=$(producer ...)
            #     The value must begin the assignment -- a substitution buried
            #     later in a word is consumed by the surrounding expansion.
            if (body ~ "^[A-Za-z_][A-Za-z0-9_]*=\\$\\((" prods ")[ )]") {
                # the status must then be consumed on this line, or the line
                # must continue so the check can land on the continuation
                if (body ~ /\|\|/) next
                if (body ~ /\\[[:space:]]*$/) next
                print file ":" NR ": assignment does not check the producer status: " body
                next
            }

            # (3) a conditional that tests the producer directly
            if (body ~ "^(if|while|until)[[:space:]]+.*\\$\\((" prods ")[ )]") next
            if (body ~ "^!?[[:space:]]*(" prods ")[[:space:]]") next

            print file ":" NR ": producer status is masked by another command: " body
        }
    ' "$f" > "$scratch/scan" 2>/dev/null; then
        report "the scanner failed on $f -- an unscannable file is never an empty pass"
        continue
    fi
    while IFS= read -r ln; do
        [ -n "$ln" ] || continue
        report "$ln"
    done < "$scratch/scan"
done

if [ "$fails" -eq 0 ]; then
    echo "PASS[hash-consumption]: $# file(s); every hash and required-value producer is consumed in a status-carrying form"
    exit 0
fi
echo "hash-consumption contract: $fails failure(s)"
exit 1
