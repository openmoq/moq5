#!/usr/bin/env bash
#
# Process-label guard.
#
# Comments, docs, test names, strings, script output and file names describe
# what the code does and why, never how it was developed. This check fails if
# a tracked or untracked (not ignored) file carries:
#
#   - a work-tracking label: S1/S3a, W1-W7, R1/R2, G1-G50, D1-D15, N1-N99;
#   - test-process vocabulary: RED/GREEN (also plural and inside
#     identifiers), neuter/neutered, "finding 4", pre-fix/post-fix;
#   - development slices in any case: "slice 2", "Slice-3", "this slice";
#   - reviewer or tool names: Codex, Claude, Galileo, reviewer;
#   - chat or report references: a comment opening with a four-digit number
#     and a colon, "report 4214", "approved 3224", a chats/ path;
#   - test-process selectors and flags: "--red", "--first-red", a variable
#     declared as red/green ("bool red");
#   - milestone labels used as names: "M10 tests", "M11 ordinary-use",
#     "the direct M1 path" (an M-number followed by a test/fixture/path word;
#     "Apple M1 Max" and "Cortex-M3" do not match);
#   - commit citations: a 7-12 digit hex hash with at least one letter and
#     one digit, directly after "from", "since", "as of", "at", "baseline",
#     "complete" or "commit" (a dependency pin such as "picoquic 7e77d01d" or
#     "e.g. 55b473e2" is not a citation of this tree's history);
#   - any of the labels above in a file name.
#
# scripts/process_labels_allowlist.txt lists the justified true positives
# (protocol text, toolchain flags, local fixture names). Each entry is
#   path|fixed text on the line|reason
# and exempts lines of that file containing the fixed text.
#
# Usage: check_process_labels.sh [--self-test]
#   --self-test checks the patterns against built-in positive and negative
#   examples and exits 0 when every example classifies as expected.
#
# Exit 0 if clean, 1 if hits were found, 2 on usage or internal error.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ALLOW="$SCRIPT_DIR/process_labels_allowlist.txt"

SELF_TEST=0
if [ "$#" -eq 1 ] && [ "$1" = "--self-test" ]; then
    SELF_TEST=1
elif [ "$#" -ne 0 ]; then
    echo "usage: $0 [--self-test]" >&2
    exit 2
fi

if [ ! -f "$ALLOW" ]; then
    echo "process labels: missing $ALLOW" >&2
    exit 2
fi

# Content patterns (POSIX ERE, case-sensitive).
patterns=(
    '(^|[^A-Za-z0-9_])(S[1-9][0-9]?[a-z]?|W[1-7]|R[12]|G([1-9]|[1-4][0-9]|50)|D([1-9]|1[0-5])|N[1-9][0-9]?)([^A-Za-z0-9_]|$)'
    '(^|[^A-Za-z0-9])(RED|GREEN)s?([^A-Za-z0-9]|$)'
    '(^|[^A-Za-z0-9])(red|green)_[A-Za-z0-9]|[A-Za-z0-9]_(red|green)([^A-Za-z0-9]|$)'
    '(^|[^A-Za-z])[Nn]eut(er|ered|ering|ers)([^A-Za-z]|$)'
    '(^|[^A-Za-z])(Codex|Claude|Galileo|[Rr]eviewers?)([^A-Za-z]|$)'
    '(^|[^A-Za-z])[Ff]indings?[ -]?#?[0-9]'
    '(^|[^A-Za-z])[Ss][Ll][Ii][Cc][Ee][Ss]?[ -][0-9]+[A-Za-z]?([^A-Za-z0-9]|$)'
    '(^|[^A-Za-z])[Tt][Hh][Ii][Ss] [Ss][Ll][Ii][Cc][Ee]([^A-Za-z]|$)'
    '(^|[^A-Za-z])([Pp]re|[Pp]ost)-fix([^A-Za-z]|$)'
    '(/\*|//|#|\*)[ ]*[0-9]{4}:'
    '(^|[^A-Za-z])(report|chat|handoff|ruling) [0-9]{3,4}([^0-9]|$)'
    'chats/'
    '(^|[^A-Za-z])[Aa]pproved[ #-]?[0-9]{3,4}([^0-9]|$)'
    '(^|[^A-Za-z0-9_-])--([a-z0-9]+-)*(red|green)([^A-Za-z0-9_-]|$)'
    '(^|[^A-Za-z0-9_])(bool|let|var|int|unsigned|auto)[ ]+(red|green)([ ;=,):]|$)'
    '(^|[^A-Za-z0-9_-])M[1-9][0-9]?[ -](tests?|controls?|fixtures?|ordinary|full-width|path|rows?)([^A-Za-z]|$)'
)
# Commit citations, judged per token (a hash needs a letter and a digit).
citation_awk='{
    line = $0
    while (match(line, /(^|[^A-Za-z])([Ff]rom|[Ss]ince|[Aa]s of|[Aa]t|[Bb]aseline|[Cc]omplete|[Cc]ommit)[ ]+[0-9a-f]+([^0-9A-Za-z_]|$)/)) {
        seg = substr(line, RSTART, RLENGTH)
        line = substr(line, RSTART + RLENGTH)
        n = split(seg, w, /[^0-9A-Za-z]+/)
        tok = ""
        for (k = n; k >= 1; k--) if (w[k] != "") { tok = w[k]; break }
        if (length(tok) >= 7 && length(tok) <= 12 && tok ~ /^[0-9a-f]+$/ && tok ~ /[a-f]/ && tok ~ /[0-9]/) {
            if (FILENAME == "-" || FILENAME == "") printf "commit citation %s\n", tok
            else printf "%s:%d:commit citation %s\n", FILENAME, FNR, tok
            break
        }
    }
}'

if [ "$SELF_TEST" -eq 1 ]; then
    pfile="$(mktemp)"
    printf '%s\n' "${patterns[@]}" > "$pfile"
    fail=0
    classify() {  # classify <expected 1|0> <text>
        local got=0
        if printf '%s\n' "$2" | grep -qE -f "$pfile"; then got=1; fi
        if [ -n "$(printf '%s\n' "$2" | awk "$citation_awk")" ]; then got=1; fi
        if [ "$got" != "$1" ]; then echo "self-test: expected $1 got $got: $2" >&2; fail=1; fi
    }
    # Must be flagged.
    classify 1 '    bool red = argc == 2 && strcmp(argv[1], "--red") == 0;'
    classify 1 'usage: %s [--first-red|--controls]'
    classify 1 '/* M11 ordinary-use controls use public facade APIs. */'
    classify 1 '// Valid ordinary ACK from the native M10 full-width fixture.'
    classify 1 'a single-lane relay is the direct M1 path with no'
    classify 1 '/* Approved 3224 boundary: independent layouts. */'
    classify 1 ' * initial settings. Under approved3218, explicit legacy'
    classify 1 '/* Complete 9dfdd307 media-object declarations. */'
    classify 1 '// Baseline shapes from ba7d9a79. This client uses only'
    classify 1 '/* behaviour as of 1a2b3c4d */'
    # Must not be flagged.
    classify 0 'deferred = true; registered = 1; ignored_reds = 0;'
    classify 0 '    --redirect-output and --greenfield are unrelated flags'
    classify 0 '| Machine | Apple M1 Max, macOS (Darwin 24.6.0), arm64 |'
    classify 0 'built for a Cortex-M3 with arm-none-eabi-gcc'
    classify 0 'draft-ietf-moq-transport-22 section 9.4 and RFC 9000 Section 4.6'
    classify 0 'baseline (picoquic 7e77d01d), whose core no longer calls'
    classify 0 'older core/log cycle (e.g. 55b473e2, whose core calls'
    classify 0 '/* picoquic 33afc776 introduced the error-reporting setter */'
    classify 0 '/* s1 parks ticket 5000. */'
    classify 0 'at 1000000 us the deadline expires; since 2026 the limit'
    classify 0 'the ed25519 key and 0b11111 mask'
    classify 0 'const char *colour = "red"; int greenish = 1;'
    rm -f "$pfile"
    [ "$fail" -eq 0 ] && { echo "process labels: self-test passed"; exit 0; }
    exit 1
fi
# File-name pattern, applied to each path component.
name_pattern='(^|[^A-Za-z0-9])([SsWwGgNn][1-9][0-9]?|[Dd]([1-9]|1[0-5])|[Rr][12])([^A-Za-z0-9]|$)|(^|[^A-Za-z0-9])(red|green|RED|GREEN)([^A-Za-z0-9]|$)'

# Generated, vendored or held-out trees, and this guard's own pattern files.
excluded() {
    case "$1" in
        build/*|build-*/*|.build/*|examples/service/*|tests/vectors/*|fuzz/corpus/*) return 0 ;;
        scripts/check_process_labels.sh|scripts/process_labels_allowlist.txt) return 0 ;;
    esac
    return 1
}

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

cd "$ROOT"
if git -C "$ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git -C "$ROOT" ls-files -z --cached --others --exclude-standard > "$work/files0"
else
    : > "$work/files0"
    while IFS= read -r -d '' path; do
        printf '%s\0' "${path#./}" >> "$work/files0"
    done < <(find . -type f ! -path './.git/*' -print0)
fi

: > "$work/files"
while IFS= read -r -d '' path; do
    excluded "$path" && continue
    [ -f "$path" ] && [ ! -L "$path" ] || continue
    printf '%s\n' "$path" >> "$work/files"
done < "$work/files0"

printf '%s\n' "${patterns[@]}" > "$work/patterns"

# grep exits 1 when a batch has no match. GNU xargs then exits 123, BSD xargs
# exits 1; a real grep error writes to stderr, so judge failure by stderr.
rc=0
tr '\n' '\0' < "$work/files" |
    xargs -0 grep -nIHE -f "$work/patterns" -- > "$work/content" 2> "$work/grep.err" || rc=$?
if [ -s "$work/grep.err" ] || { [ "$rc" -ne 0 ] && [ "$rc" -ne 1 ] && [ "$rc" -ne 123 ]; }; then
    cat "$work/grep.err" >&2
    echo "process labels: grep failed ($rc)" >&2
    exit 2
fi

# File names: each path component, extension stripped; reported as path:0:...
awk -v re="$name_pattern" '{
    n = split($0, comp, "/")
    for (k = 1; k <= n; k++) {
        c = comp[k]; sub(/\.[^.]*$/, "", c)
        if (c ~ re) { printf "%s:0:file name %s\n", $0, comp[k]; break }
    }
}' "$work/files" > "$work/names"

# Commit citations.
# Unlike grep, awk has no "no match" status: any nonzero exit from the
# pipeline is an internal error, with or without stderr.
# PROCESS_LABELS_FAULT_AWK=1 makes the scan exit nonzero silently; it is a
# control for this check only and has no effect unless set.
citation_cmd=(awk "$citation_awk")
if [ "${PROCESS_LABELS_FAULT_AWK:-0}" = 1 ]; then
    citation_cmd=(awk 'BEGIN { exit 3 }')
fi
rc=0
tr '\n' '\0' < "$work/files" | xargs -0 "${citation_cmd[@]}" > "$work/citations" 2> "$work/awk.err" || rc=$?
if [ "$rc" -ne 0 ] || [ -s "$work/awk.err" ]; then
    cat "$work/awk.err" >&2
    echo "process labels: citation scan failed ($rc)" >&2
    exit 2
fi

cat "$work/content" "$work/names" "$work/citations" > "$work/hits"

# Drop allowlisted lines; whatever is left is a hit.
rc=0
awk -v allow="$ALLOW" '
    BEGIN {
        while ((getline line < allow) > 0) {
            if (line ~ /^[ \t]*(#|$)/) continue
            n = split(line, f, "|")
            if (n < 3 || f[1] == "" || f[2] == "" || f[3] == "") {
                print "process labels: malformed allowlist entry: " line > "/dev/stderr"
                bad = 1; continue
            }
            na++; ap[na] = f[1]; at[na] = f[2]
        }
        if (bad) exit 2
    }
    {
        i = index($0, ":"); path = substr($0, 1, i - 1); rest = substr($0, i + 1)
        j = index(rest, ":"); text = substr(rest, j + 1)
        for (k = 1; k <= na; k++)
            if (ap[k] == path && index(text, at[k]) > 0) next
        print; hits++
    }
    END { if (bad) exit 2; exit hits ? 1 : 0 }
' "$work/hits" > "$work/left" || rc=$?
case "$rc" in
    0)  echo "process labels: clean ($(wc -l < "$work/files" | tr -d ' ') files)"
        exit 0 ;;
    1)  cat "$work/left"
        echo "process labels: $(wc -l < "$work/left" | tr -d ' ') hit(s)." \
             "Describe what the code does, not how it was developed." >&2
        exit 1 ;;
    *)  exit 2 ;;
esac
