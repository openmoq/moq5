#!/bin/bash
# Every SUB_DONE intent carries a tagged PUBLISH_DONE terminal, and each
# producer must set it exactly once. Zero writes would queue an uninitialized
# descriptor that emission refuses — the subscriber would get no terminal at
# all; two writes mean a producer was edited twice and one of them is dead.
# This is a SOURCE check because both mistakes are invisible at runtime until a
# specific transition fires.
#
# The producer count is pinned exactly: a deleted producer is as much a defect
# as a miswritten one, and would otherwise pass a "check what you find" scan.
set -u
relay_c="$1"
expected_producers=6
fail=0

# Executable code only, joined into one stream so a producer split across lines
# is still one token. Comments are erased first.
code=$(mktemp)
awk '
BEGIN { inblk = 0 }
{
    line = $0; out = ""; i = 1; n = length(line)
    while (i <= n) {
        c = substr(line, i, 1); d = substr(line, i, 2)
        if (inblk) { if (d == "*/") { inblk = 0; i += 2 } else { i++ } ; continue }
        if (d == "/*") { inblk = 1; i += 2; continue }
        if (d == "//") { break }
        out = out c; i++
    }
    print out
}' "$relay_c" > "$code"
trap 'rm -f "$code"' EXIT

# Count producer tokens without depending on line wrapping: collapse all
# whitespace, then count intent_push(...MOQR_INTENT_SUB_DONE) occurrences.
tokens=$(tr '\n' ' ' < "$code" | tr -s ' ' \
         | grep -o 'intent_push([^)]*MOQR_INTENT_SUB_DONE[^)]*)' | wc -l | tr -d ' ')

if [ "$tokens" -ne "$expected_producers" ]; then
    echo "FAIL: found $tokens SUB_DONE producer tokens, expected exactly $expected_producers"
    echo "      (a deleted or added producer must be reviewed, not silently accepted)"
    fail=1
fi

# Each producer's block must initialize it->pd exactly once. Blocks are located
# on the whitespace-collapsed stream so a multiline producer is handled, then
# bounded by the retire call that ends every producer.
stream=$(tr '\n' ' ' < "$code" | tr -s ' ')
idx=0
while : ; do
    rest=${stream#*MOQR_INTENT_SUB_DONE}
    [ "$rest" = "$stream" ] && break
    stream=$rest
    idx=$((idx + 1))
    # the producer body runs until its sub_retire call
    body=${rest%%sub_retire*}
    [ "$body" = "$rest" ] && body=${rest:0:400}
    writes=$(printf '%s' "$body" | grep -o 'it->pd *=' | wc -l | tr -d ' ')
    if [ "$writes" -ne 1 ]; then
        echo "FAIL: SUB_DONE producer #$idx sets it->pd $writes times, expected exactly 1"
        fail=1
    fi
done

if [ "$fail" -eq 0 ]; then
    echo "  $tokens SUB_DONE producers, each setting it->pd exactly once"
    echo "PASS: sub_done pd initialization"
fi
exit "$fail"
