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
#     and a colon, "report 4214", a chats/ path;
#   - any of the labels above in a file name.
#
# scripts/process_labels_allowlist.txt lists the justified true positives
# (protocol text, toolchain flags, local fixture names). Each entry is
#   path|fixed text on the line|reason
# and exempts lines of that file containing the fixed text.
#
# Usage: check_process_labels.sh   (takes no arguments)
#
# Exit 0 if clean, 1 if hits were found, 2 on usage or internal error.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ALLOW="$SCRIPT_DIR/process_labels_allowlist.txt"

if [ "$#" -ne 0 ]; then
    echo "usage: $0" >&2
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
)
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

cat "$work/content" "$work/names" > "$work/hits"

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
