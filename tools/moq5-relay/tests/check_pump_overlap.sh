#!/usr/bin/env bash
# The concurrent-reader test must launch a cohort, not a queue.
#
# Creating each reader and immediately joining it serialises them: the ledger
# still shows every worker running, publishing and observing every generation,
# so the test passes while proving nothing about concurrency. Nothing at
# runtime separates a cohort from a queue without making elapsed time the
# verdict, so the SHAPE is pinned here.
#
# Two rules do the work, and neither is a spelling special case:
#
#   1. IDENTIFIER OCCURRENCES, not matching lines. The source is tokenised
#      into C identifiers and each name is counted. Line counting fails open on
#      two calls sharing a line; token counting also catches an address-take
#      however it is spaced, because the alias is one more occurrence.
#   2. CANONICAL BODIES. The creating helper and the caller's cohort phase are
#      compared against their exact normalised text, so the ordered
#      partial-create relations -- capture the result, check it, leave the loop
#      before counting a failed thread, report the count, join only that prefix
#      -- cannot be reordered or dropped one at a time.
set -u
src="${1:-}"
cml="${2:-}"
[ -n "$src" ] || { echo "usage: $0 <test_relay_snapshot_cap.c> [CMakeLists.txt]" >&2; exit 2; }
[ -r "$src" ] || { echo "FAIL: cannot read $src" >&2; exit 1; }

failures=0
fail() { echo "FAIL: $*" >&2; failures=$((failures + 1)); }

# Reduce the source to CODE ONLY, with a real lexical state machine.
#
# It must distinguish code, block comments, line comments, string literals,
# character literals and escapes. A scanner that knows only comments will treat
# the bytes `//` inside a string as the start of a comment and silently discard
# the rest of that physical line -- taking real statements with it, which is a
# fail-open for every rule built on top. String and character contents are
# dropped entirely: they hold no identifiers, and dropping them keeps the
# surrounding punctuation intact so tokens cannot merge.
#
# C line splicing is rejected fail-closed rather than emulated: a trailing
# backslash could continue any of these states across lines, and guessing is
# worse than refusing.
if grep -q '\\$' "$src"; then
    echo "FAIL: $src uses line splicing; this gate refuses to guess at it" >&2
    exit 1
fi

code=$(awk '
BEGIN { CODE = 0; BLOCK = 1; STR = 2; CH = 3; state = CODE }
{
    line = $0; out = ""; i = 1; n = length(line)
    while (i <= n) {
        c = substr(line, i, 1)
        d = substr(line, i, 2)
        if (state == BLOCK) {
            if (d == "*/") { state = CODE; i += 2 } else { i++ }
            continue
        }
        if (state == STR) {
            if (c == "\\") { i += 2; continue }
            if (c == "\"") { state = CODE }
            i++
            continue
        }
        if (state == CH) {
            if (c == "\\") { i += 2; continue }
            if (c == "'"'"'") { state = CODE }
            i++
            continue
        }
        if (d == "/*") { state = BLOCK; i += 2; continue }
        if (d == "//") { break }
        if (c == "\"") { state = STR; i++; continue }
        if (c == "'"'"'") { state = CH; i++; continue }
        out = out c
        i++
    }
    # A string or character literal may not span a physical line.
    if (state == STR || state == CH) { state = CODE }
    print out
}' "$src")

# -- identifier occurrence counts -------------------------------------------
# Tokenise: every run of non-identifier characters becomes a separator, so each
# identifier is counted wherever it appears, including several times on one
# line and including `& name` with any spacing.
count_ident() {
    printf '%s\n' "$code" | awk -v want="$1" '
        { gsub(/[^A-Za-z0-9_]/, " "); n = split($0, t, " ");
          for (i = 1; i <= n; i++) if (t[i] == want) c++ }
        END { print c + 0 }'
}

expect_ident() {
    got=$(count_ident "$1")
    [ "$got" -eq "$2" ] ||
        fail "$1 must occur exactly $2 time(s) after comment stripping, found $got"
}

# One raw call of each kind; one definition and one call of each helper.
expect_ident pthread_create 1
expect_ident pthread_join 1
expect_ident spawn_cohort 2
expect_ident join_cohort 2

body_of() {
    printf '%s\n' "$code" |
        awk -v f="^$1\\\\(" 'BEGIN{on=0} $0 ~ f {on=1} on {print} on && /^}/ {exit}'
}
norm() { tr '\n' ' ' | tr -s ' ' | sed -e 's/^ //' -e 's/ $//'; }

# -- each raw call lives in its own helper ----------------------------------
spawn=$(body_of spawn_cohort)
join=$(body_of join_cohort)
[ -n "$spawn" ] || fail "spawn_cohort not found"
[ -n "$join" ] || fail "join_cohort not found"
printf '%s\n' "$join" | grep -q 'pthread_join' ||
    fail "join_cohort does not join the readers"

# -- canonical regions, by digest -------------------------------------------
# Four regions are pinned to their exact normalised text. Inline strings do not
# scale to a whole function body, so each is compared by digest.
#
# `main` and the concurrency function are pinned WHOLE, not merely their tails.
# The lexer drops string contents by design, so a fabricated printf of the
# accepted records is invisible to any token rule -- but it is not invisible to
# the body it must be inserted into. Pinning the whole region closes both
# fabrication and unreachability at once: an added emitter, an added early
# return, or a real receipt left unreachable all change the region.
region_digest() {
    body_of "$1" | norm | shasum -a 256 | cut -d' ' -f1
}
pin_region() {
    got=$(region_digest "$1")
    [ "$got" = "$2" ] ||
        fail "$1 is not its pinned canonical region (digest $got)"
}

pin_region spawn_cohort \
    d2863b662fdbc6c71f63b2377ba336ffc75c97a560e352ca8b354b41fd9cccf2
pin_region join_cohort \
    92b29e622cc7b526f2ecb3c89da275b41df6a6dc0c99780fffb9c0339c30d9cb
pin_region test_broker_survives_concurrent_pumps \
    42d2f9e019f98576d5e7b504188ed27c723b046fc4f7f6a55f2935d7596d3836
pin_region main \
    40e583aae546fb6190c99c3c5737b1653da558c291918e515c9ce3cff5ef354c

# -- closed set of output emitters ------------------------------------------
# The accepted records may have exactly one emitter each, at the authorized
# points. Counting a named set makes an added printf visible directly, and
# names the failure rather than reporting only a changed digest.
for e in printf fprintf puts fputs fwrite putchar write; do
    n=$(count_ident "$e")
    case "$e" in
        printf)  want=2 ;;
        fprintf) want=1 ;;
        *)       want=0 ;;
    esac
    [ "$n" -eq "$want" ] ||
        fail "$e occurs $n time(s), expected $want -- an unexpected output emitter"
done


# -- fixture dimensions ------------------------------------------------------
# A cohort of one has no overlap to prove, and a shrunken generation count
# silently reduces coverage. The replacement list is parsed as a complete token
# sequence: a substring test accepts `4-3`, whose preprocessor value is one
# while the expected text is still present.
pin_define() {
    name="$1"; want="$2"
    defs=$(printf '%s\n' "$code" |
           grep -cE "^[[:space:]]*#[[:space:]]*define[[:space:]]+$name([[:space:]]|\$)")
    [ "$defs" -eq 1 ] ||
        fail "$name must be defined exactly once, found $defs"
    undefs=$(printf '%s\n' "$code" |
             grep -cE "^[[:space:]]*#[[:space:]]*undef[[:space:]]+$name([[:space:]]|\$)")
    [ "$undefs" -eq 0 ] || fail "$name is #undef'd"
    body=$(printf '%s\n' "$code" |
           sed -nE "s/^[[:space:]]*#[[:space:]]*define[[:space:]]+$name[[:space:]]+(.*)\$/\1/p" |
           sed -e 's/[[:space:]]*\$//' -e 's/^[[:space:]]*//')
    [ "$body" = "$want" ] ||
        fail "$name must expand to exactly '$want', found '$body'"
}
pin_define PUMP_THREADS 4
pin_define PUMP_GENS 16

# -- the test actually runs --------------------------------------------------
# Two occurrences: the definition and one call. A preceding semicolon is not
# enough to make a call unconditional -- `if (0) { ; failures += ...; }`
# supplies one while skipping the test entirely -- so the sole invocation is
# pinned to main's OUTER body depth, with its exact statement tokens. Braces
# are counted over the already-lexed source, so nothing inside a comment or a
# literal can shift the depth.
expect_ident test_broker_survives_concurrent_pumps 2

depth_report=$(printf '%s\n' "$code" | awk '
BEGIN { inmain = 0; depth = 0; ntok = 0 }
{
    if (!inmain && $0 ~ /^main\(/) { inmain = 1 }
    if (!inmain) { next }
    line = $0
    # Give every non-identifier character its own token, so punctuation can
    # never stay glued to the name it follows.
    gsub(/[^A-Za-z0-9_]/, " & ", line)
    n = split(line, t, " ")
    for (i = 1; i <= n; i++) {
        if (t[i] == "") { continue }
        ntok++
        tok[ntok] = t[i]
        if (t[i] == "{") { depth++ }
        if (t[i] == "}") { depth-- }
        dep[ntok] = depth
    }
}
END {
    found = 0; bad = 0
    for (k = 1; k <= ntok; k++) {
        if (tok[k] != "test_broker_survives_concurrent_pumps") { continue }
        found++
        # Directly in the main body, spelled failures += name ( ) ;
        if (dep[k] != 1) { bad++; continue }
        if (!(tok[k-1] == "=" && tok[k-2] == "+" && tok[k-3] == "failures")) {
            bad++; continue
        }
        # The statement must START here. Depth alone is not enough: an
        # unbraced `if (0) failures += ...;` sits at the same depth with the
        # same local tokens, so the token before the statement must close a
        # previous statement or open the block, never a condition.
        if (!(tok[k-4] == ";" || tok[k-4] == "{")) { bad++; continue }
        if (!(tok[k+1] == "(" && tok[k+2] == ")" && tok[k+3] == ";")) {
            bad++
        }
    }
    print found " " bad
}')
set -- $depth_report
inv_found="$1"; inv_bad="$2"
[ "$inv_found" -eq 1 ] ||
    fail "the concurrency test is invoked $inv_found time(s) in main, expected 1"
[ "$inv_bad" -eq 0 ] ||
    fail "the concurrency test is not an unconditional 'failures += <call>' statement at main's body depth"

# -- the completion receipt --------------------------------------------------
# Where a statement is written proves nothing about whether execution reaches
# it. The receipt is the runtime evidence, so its own placement is pinned: it
# must be the last thing the concurrency function does, after every ledger
# check and after cleanup, and it must be emitted exactly once from there.
expect_ident PUMP_RECEIPT 2
grep -qE '^#[[:space:]]*define[[:space:]]+PUMP_RECEIPT[[:space:]]+"RECEIPT concurrent_pumps completed"[[:space:]]*$' "$src" ||
    fail "PUMP_RECEIPT is not the pinned receipt text"

conc=$(body_of test_broker_survives_concurrent_pumps | norm)
# String contents are dropped by the lexer above, so the format literal is
# absent here; its exact text is pinned separately against the raw source.
want_tail='moqr_cli_snapshot_destroy(&snap); moqr_broker_destroy(&b); printf(, PUMP_RECEIPT, (unsigned)PUMP_GENS, (unsigned)PUMP_THREADS); return failures; }'
case "$conc" in
    *"$want_tail") ;;
    *) fail "the completion receipt is not emitted last, after the ledger checks and cleanup" ;;
esac

# -- the CTest wiring --------------------------------------------------------
# The receipt only has authority if the suite actually checks it. There must be
# exactly ONE relay_snapshot_cap registration and it must be the checked
# oracle: a shell-availability branch that registered the bare executable would
# restore the false green the receipt exists to prevent.
if [ -n "$cml" ]; then
    [ -r "$cml" ] || fail "cannot read $cml"
    # Word-bounded: relay_snapshot_cap_oracle is a different test.
    regs=$(grep -cE 'add_test\(NAME relay_snapshot_cap[^_A-Za-z0-9]' "$cml")
    [ "$regs" -eq 1 ] ||
        fail "relay_snapshot_cap is registered $regs times, expected 1"
    cflat=$(tr '\n' ' ' < "$cml" | tr -s ' ')
    case "$cflat" in
        *"add_test(NAME relay_snapshot_cap COMMAND \${CMAKE_COMMAND} -DMOQR_BIN=\$<TARGET_FILE:test_relay_snapshot_cap> -P \${CMAKE_CURRENT_SOURCE_DIR}/tests/run_snapshot_cap.cmake)"*) ;;
        *) fail "relay_snapshot_cap does not run through the checked CMake oracle" ;;
    esac
    grep -qE 'add_test\(NAME relay_snapshot_cap[[:space:]]+COMMAND[[:space:]]+test_relay_snapshot_cap' "$cml" &&
        fail "a bare relay_snapshot_cap registration still exists"

    # The oracle only has authority if its OWN behaviour is tested, so the
    # self-test registration is pinned too: singular, and naming the exact
    # oracle file, the exact driver and the CMake executable it drives them
    # with. Without this, removing just that registration would leave the suite
    # with no behavioural discriminator after a future oracle weakening.
    #
    # Platform boundary, stated rather than implied: this self-test is a shell
    # script and is registered inside the project's existing MOQR_WIRE_BASH
    # block, exactly like the other source guards. The PRODUCTION
    # relay_snapshot_cap test is unconditional and needs no shell.
    selftests=$(grep -cE 'add_test\(NAME relay_snapshot_cap_oracle[^_A-Za-z0-9]' "$cml")
    [ "$selftests" -eq 1 ] ||
        fail "relay_snapshot_cap_oracle is registered $selftests times, expected 1"
    case "$cflat" in
        *"add_test(NAME relay_snapshot_cap_oracle COMMAND \${MOQR_WIRE_BASH} \${CMAKE_CURRENT_SOURCE_DIR}/tests/check_snapshot_cap_oracle.sh \${CMAKE_CURRENT_SOURCE_DIR}/tests/run_snapshot_cap.cmake \${CMAKE_COMMAND})"*) ;;
        *) fail "relay_snapshot_cap_oracle does not drive the exact oracle with the exact self-test" ;;
    esac

    # The configured-graph authority, and the configure-time assertion that is
    # its trust root.
    #
    # relay_ctest_graph audits the generated CTest graph, but it cannot prove it
    # was not itself removed from that graph -- a disabled or conditioned-away
    # authority simply never runs, and ctest reports green. The assertion runs
    # at configure time instead, so it fails the build before any test graph is
    # executed. Pinning it here is what stops the assertion from being deleted
    # in the same edit that disables the authority.
    graphregs=$(grep -cE 'add_test\(NAME relay_ctest_graph[^_A-Za-z0-9]' "$cml")
    [ "$graphregs" -eq 1 ] ||
        fail "relay_ctest_graph is registered $graphregs times, expected 1"

    # Registered unconditionally: the verifier is a cmake -P script, so gating
    # it on a shell would make the production check optional on exactly the
    # platforms least able to lose it.
    case "$cflat" in
        *"if(MOQR_WIRE_BASH) add_test(NAME relay_ctest_graph"*)
            fail "relay_ctest_graph is gated on MOQR_WIRE_BASH" ;;
    esac

    # Every expected command element is supplied explicitly by CMake. Identity
    # must not be inferred from a basename pattern: a lookalike executable
    # satisfies containment while running something else entirely.
    for atom in \
        '-DMOQR_CTEST=${CMAKE_CTEST_COMMAND}' \
        '-DMOQR_CMAKE=${CMAKE_COMMAND}' \
        '-DMOQR_PROD_BIN=$<TARGET_FILE:test_relay_snapshot_cap>' \
        '-DMOQR_ORACLE=${CMAKE_CURRENT_SOURCE_DIR}/tests/run_snapshot_cap.cmake' \
        '-DMOQR_SELFTEST_DRIVER=${CMAKE_CURRENT_SOURCE_DIR}/tests/check_snapshot_cap_oracle.sh' \
        '-DMOQR_GRAPH_SCRIPT=${CMAKE_CURRENT_SOURCE_DIR}/tests/check_ctest_graph.cmake'
    do
        case "$cflat" in
            *"$atom"*) ;;
            *) fail "relay_ctest_graph does not pass the exact identity $atom" ;;
        esac
    done

    # This guard's OWN registration, pinned exactly. Presence by name is not
    # enough: rewriting the command to `${CMAKE_COMMAND} -E true` leaves the
    # test registered, enabled and green while running nothing at all.
    pumpregs=$(grep -cE 'add_test\(NAME relay_pump_overlap[^_A-Za-z0-9]' "$cml")
    [ "$pumpregs" -eq 1 ] ||
        fail "relay_pump_overlap is registered $pumpregs times, expected 1"
    case "$cflat" in
        *"add_test(NAME relay_pump_overlap COMMAND \${MOQR_WIRE_BASH} \${CMAKE_CURRENT_SOURCE_DIR}/tests/check_pump_overlap.sh \${CMAKE_CURRENT_SOURCE_DIR}/tests/test_relay_snapshot_cap.c \${CMAKE_CURRENT_SOURCE_DIR}/CMakeLists.txt)"*) ;;
        *) fail "relay_pump_overlap does not run this guard against the exact subject and CMakeLists" ;;
    esac

    # The graph authority must authenticate that command too, so the atoms it
    # compares against are pinned here.
    for atom in \
        '-DMOQR_PUMP_GUARD=${CMAKE_CURRENT_SOURCE_DIR}/tests/check_pump_overlap.sh' \
        '-DMOQR_PUMP_SUBJECT=${CMAKE_CURRENT_SOURCE_DIR}/tests/test_relay_snapshot_cap.c' \
        '-DMOQR_CMAKELISTS=${CMAKE_CURRENT_SOURCE_DIR}/CMakeLists.txt'
    do
        case "$cflat" in
            *"$atom"*) ;;
            *) fail "relay_ctest_graph does not pass the exact identity $atom" ;;
        esac
    done

    # The trust cycle is broken by running this guard FROM configure, not by one
    # CTest entry vouching for another. That call is pinned here: without it,
    # rewriting how CTest invokes this guard would silence it entirely.
    case "$cflat" in
        *"COMMAND \"\${MOQR_WIRE_BASH}\" \"\${CMAKE_CURRENT_SOURCE_DIR}/tests/check_pump_overlap.sh\" \"\${CMAKE_CURRENT_SOURCE_DIR}/tests/test_relay_snapshot_cap.c\" \"\${CMAKE_CURRENT_SOURCE_DIR}/CMakeLists.txt\""*) ;;
        *) fail "the configure-time trust root does not execute this guard" ;;
    esac
    case "$cflat" in
        *'if(NOT pin_status EQUAL 0)'*) ;;
        *) fail "the configure-time guard run does not fail closed on a nonzero status" ;;
    esac

    grep -q 'function(moqr_assert_authority_chain)' "$cml" ||
        fail "the configure-time authority assertion is missing"
    # Deferred, so it observes the FINAL property values: asserting at the call
    # site would leave a later set_tests_properties override unseen.
    case "$cflat" in
        *"cmake_language(DEFER CALL moqr_assert_authority_chain)"*) ;;
        *) fail "the authority assertion is not deferred to end of directory scope" ;;
    esac
    for req in relay_ctest_graph relay_snapshot_cap; do
        case "$cflat" in
            *"set(required $req relay_snapshot_cap)"*|*"set(required relay_ctest_graph $req)"*) ;;
            *) fail "the authority assertion does not require $req" ;;
        esac
    done
    case "$cflat" in
        *"list(APPEND required relay_snapshot_cap_oracle relay_pump_overlap)"*) ;;
        *) fail "the authority assertion does not require the shell-gated tests when a shell exists" ;;
    esac
    case "$cflat" in
        *"get_test_property(\${t} DISABLED disabled)"*) ;;
        *) fail "the authority assertion does not read the final DISABLED property" ;;
    esac
fi

if [ "$failures" -ne 0 ]; then
    echo "FAIL: $failures reader-cohort violation(s)" >&2
    exit 1
fi
echo "PASS: lexer, counts, canonical bodies, dimensions, invocation, receipt and wiring pinned"
exit 0
