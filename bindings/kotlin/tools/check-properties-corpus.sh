#!/bin/sh
# Cross-parser corpus for the canonical properties subset.
#
# The declaration authority claims that for every value it ADMITS, the shell
# reader and java.util.Properties produce byte-identical content. This gate
# tests that claim directly instead of asserting it:
#
#   * POSITIVE rows are written to a temp file, read by both parsers, and their
#     exact UTF-8 bytes and lengths compared. Any difference fails by name.
#   * NEGATIVE rows must be rejected by the grammar with a named file/line
#     diagnostic. A negative row that parses is a fail-open.
#
# The Java side is compiled on demand from tools/PropsEcho.java. No JDK is a
# named FAILURE, never a skip -- a corpus that silently does not run would be
# indistinguishable from one that passes.
#
# Usage: check-properties-corpus.sh [<java-home>]

set -u
here=$(cd "$(dirname "$0")" && pwd)
. "$here/lib-declaration.sh"

fails=0
rows=0
pass() { rows=$((rows + 1)); printf 'PASS[corpus.%s]: %s\n' "$1" "$2"; }
fail() { rows=$((rows + 1)); printf 'FAIL[corpus.%s]: %s\n' "$1" "$2"; fails=$((fails + 1)); }

# The oracle JDK is an INPUT, not whatever PATH happens to offer: the Gradle
# task passes Gradle's own java.home, so the corpus compares against the same
# runtime the build uses. When a home is supplied both bin/java and bin/javac
# must exist, and the diagnostic names which one is missing.
JH=${1:-${JAVA_HOME:-}}
if [ -n "$JH" ]; then
    _miss=""
    [ -x "$JH/bin/java" ]  || _miss="bin/java"
    [ -x "$JH/bin/javac" ] || _miss="${_miss:+$_miss and }bin/javac"
    if [ -n "$_miss" ]; then
        fail toolchain "the supplied Java home '$JH' has no $_miss"
        printf '\nproperties corpus: %d failure(s)\n' "$fails"
        exit 1
    fi
    JAVA="$JH/bin/java"; JAVAC="$JH/bin/javac"
elif command -v java >/dev/null 2>&1 && command -v javac >/dev/null 2>&1; then
    JAVA=$(command -v java); JAVAC=$(command -v javac)
else
    fail toolchain "no JDK with javac: the corpus compares against the REAL java.util.Properties and cannot be skipped"
    printf '\nproperties corpus: %d failure(s)\n' "$fails"
    exit 1
fi

work=$(mktemp -d) || { fail workdir "cannot create a temp dir"; exit 1; }
trap 'rm -rf "$work"' EXIT INT TERM
# The oracle is project tooling, and this project does not exempt tooling from
# the zero-warning rule: full lint, warnings promoted to errors.
"$JAVAC" -Xlint:all -Werror -d "$work" "$here/PropsEcho.java" 2>"$work/javac.err" || {
    fail toolchain "PropsEcho.java did not compile warning-clean under -Xlint:all -Werror: $(head -3 "$work/javac.err")"
    printf '\nproperties corpus: %d failure(s)\n' "$fails"
    exit 1
}

# hex of a shell string's UTF-8 bytes, so the two sides compare like for like
hexof() { printf '%s' "$1" | od -An -tx1 -v | tr -d ' \n'; }


# --- ONE diagnostic checker for every negative family ----------------------
# A negative row must not pass merely because "some failure happened". Two
# earlier revisions were too loose, and both are closed here:
#   * `FAIL.properties.grammar` matched with three wildcard dots, and `[0-9]*`
#     admitted an EMPTY line number;
#   * the basename was interpolated into an ERE, so the dot in `.properties`
#     matched any character and `nul_byteXproperties` satisfied a check that
#     claimed to pin the exact file.
# Nothing that must be exact is compared as a regex any more. The literal
# prefix and the literal basename are both matched with `case` and stripped;
# only the numeric line and the body -- fields that genuinely need patterns --
# reach grep.
#
# diag_ok <outfile> <basename> <line-regex> [<byte-hex> <column>]
diag_ok() {
    _out=$1; _base=$2; _linere=$3; _byte=${4:-}; _col=${5:-}
    _first=$(head -1 "$_out")

    case "$_first" in
        "FAIL[properties.grammar]: "*) ;;
        *) printf '%s' "unanchored or missing prefix: $_first"; return 1 ;;
    esac
    _rest=${_first#"FAIL[properties.grammar]: "}

    # Literal basename, then the separator colon. No regex, so no metacharacter
    # in a filename can stand in for another byte.
    case "$_rest" in
        "$_base:"*) ;;
        *) printf '%s' "expected basename '$_base', got: $_rest"; return 1 ;;
    esac
    _rest=${_rest#"$_base:"}

    if [ -n "$_byte" ]; then
        _want="^$_linere: byte 0x$_byte at column $_col "
    else
        _want="^$_linere: "
    fi
    if printf '%s' "$_rest" | LC_ALL=C grep -Eq "$_want"; then
        return 0
    fi
    printf '%s' "after basename '$_base:' expected /$_want/, got: $_rest"
    return 1
}

# positive <name> <key> <literal line>
positive() {
    _name=$1; _key=$2; _line=$3
    _f="$work/$_name.properties"
    printf '%s\n' "$_line" > "$_f"
    if ! decl_validate_grammar "$_f" >"$work/g.out" 2>&1; then
        fail "$_name" "the grammar REJECTED an admitted row: $(head -1 "$work/g.out")"
        return
    fi
    _shell=$(props_value "$_f" "$_key")
    _shex=$(hexof "$_shell")
    _jline=$("$JAVA" -cp "$work" PropsEcho "$_f" "$_key")
    _jhex=$(printf '%s' "$_jline" | awk -F'\t' '{print $3}')
    if [ "$_shex" = "$_jhex" ]; then
        pass "$_name" "shell and java.util.Properties agree, bytes $_shex"
    else
        fail "$_name" "shell bytes $_shex != java bytes $_jhex"
    fi
}

# negative <name> <literal line...>  (may be given several lines)
negative() {
    _name=$1; shift
    _f="$work/$_name.properties"
    : > "$_f"
    for _l in "$@"; do printf '%s\n' "$_l" >> "$_f"; done
    if decl_validate_grammar "$_f" >"$work/g.out" 2>&1; then
        fail "$_name" "the grammar ADMITTED a row it must reject (fail-open)"
        return
    fi
    if _why=$(diag_ok "$work/g.out" "$_name.properties" '[1-9][0-9]*'); then
        pass "$_name" "$(head -1 "$work/g.out" | sed 's/^FAIL\[properties.grammar\]: //' | cut -c1-72)"
    else
        fail "$_name" "$_why"
    fi
}

# Literal control characters are written as octal escapes, never typed into
# this source: a literal tab in a test row is invisible in review and trips
# the repository's own whitespace gate.
tabc=$(printf '\011')

# --- POSITIVE: admitted values must decode identically on both sides -------
positive plain          k                'k=value'
positive url_escaped    distributionUrl  'distributionUrl=https\://services.gradle.org/distributions/gradle-9.5.1-bin.zip'
positive url_literal    u                'u=https://example.com/a/b.zip'
positive embedded_eq    k                'k=a=b=c'
positive embedded_colon k                'k=a:b:c'
positive internal_space k                'k=two words here'
positive digits_dots    k                'k=9.5.1'
positive sha_hex        k                'k=bafc141b619ad6350fd975fc903156dd5c151998cc8b058e8c1044ab5f7b031f'
positive dashy_key      a-b.c_d          'a-b.c_d=v'
positive empty_value    k                'k='
positive sdk_dir_row    sdk.dir          'sdk.dir=/opt/android-sdk'

# --- NEGATIVE: every form whose two decodings could differ -----------------
negative trailing_space   'k=value '
negative leading_space    'k= value'
negative trailing_tab     "k=value${tabc}"
negative colon_separator  'k:value'
negative space_separator  'k value'
negative escaped_key      'k\.x=value'
negative unicode_key      'k\u002ex=value'
negative key_lead_space   ' k=value'
negative key_trail_space  'k =value'
negative esc_equals       'k=a\=b'
negative esc_backslash    'k=a\\b'
negative esc_tab          'k=a\tb'
negative esc_newline      'k=a\nb'
negative esc_return       'k=a\rb'
negative esc_formfeed     'k=a\fb'
negative esc_ordinary     'k=a\qb'
negative esc_unicode      'k=a\u003db'
# One backslash, written as an octal escape so no literal backslash appears
# in this source line -- a trailing literal backslash inside quotes reads as
# an escape attempt to both humans and linters.
bs=$(printf '\134')
negative continuation     "k=a${bs}" 'b=2'

# Byte-level rows. Every forbidden byte is produced by an octal escape, so no
# non-ASCII or control byte is ever typed into this repository file.
# negative_bytes <name> <printf-octal> <byte-hex> <column> [<prefix-lines>]
negative_bytes() {
    _name=$1; _octal=$2; _bytehex=$3; _bcol=$4; _pre=${5:-0}
    _f="$work/$_name.properties"
    # The byte goes STRAIGHT into the file. Routing it through a command
    # substitution would silently drop a NUL -- the byte this row exists to
    # test -- and the row would then pass by testing nothing. %b keeps the
    # format string a literal (so no suppression is needed) while still
    # interpreting the escape in the argument.
    : > "$_f"
    _ln=1
    _i=0
    while [ "$_i" -lt "$_pre" ]; do
        _i=$((_i + 1))
        printf 'pad%s=v\n' "$_i" >> "$_f"
        _ln=$((_ln + 1))
    done
    printf 'k=va%blue\n' "$_octal" >> "$_f"
    if decl_validate_grammar "$_f" >"$work/g.out" 2>&1; then
        fail "$_name" "the grammar ADMITTED a forbidden byte (fail-open)"
    elif _why=$(diag_ok "$work/g.out" "$_name.properties" "$_ln" "$_bytehex" "$_bcol"); then
        pass "$_name" "$(head -1 "$work/g.out" | sed 's/^FAIL\[properties.grammar\]: //' | cut -c1-64)"
    else
        fail "$_name" "$_why"
    fi
}
negative_bytes utf8_multibyte  '\0303\0251' c3 5
negative_bytes lone_high_byte  '\0300' c0 5
negative_bytes del_byte        '\0177' 7f 5
negative_bytes form_feed       '\0014' 0c 5
negative_bytes vertical_tab    '\0013' 0b 5
negative_bytes nul_byte        '\0000' 00 5
# The same byte three lines in, so the reported line cannot be a constant.
negative_bytes nul_line3       '\0000' 00 5 2


# --- structural rows that must be accepted --------------------------------
for row in "blank_lines::" "comment_hash:# a comment:" "comment_bang:! a comment:"; do
    name=${row%%:*}; rest=${row#*:}; body=${rest%%:*}
    f="$work/$name.properties"
    printf '\n%s\n\nk=v\n' "$body" > "$f"
    if decl_validate_grammar "$f" >"$work/g.out" 2>&1; then
        pass "$name" "accepted alongside a real declaration"
    else
        fail "$name" "rejected: $(head -1 "$work/g.out")"
    fi
done

# --- the reader must not NORMALISE, independently of the grammar ----------
# Defence in depth, and it needs saying why it is separate: the grammar now
# rejects surrounding whitespace, so no ADMITTED row can reach a trimming
# reader -- which means the positive rows above cannot detect one. This section
# therefore feeds the reader directly, on input the grammar would refuse, and
# requires it to report exactly what java.util.Properties reports. A reader that
# trims would satisfy every other row in this file and still be wrong the moment
# the grammar were relaxed.
reader_matches_java() {
    _name=$1; _key=$2; _line=$3
    _f="$work/reader_$_name.properties"
    printf '%s\n' "$_line" > "$_f"
    _shex=$(hexof "$(props_value "$_f" "$_key")")
    _jhex=$("$JAVA" -cp "$work" PropsEcho "$_f" "$_key" | awk -F'\t' '{print $3}')
    if [ "$_shex" = "$_jhex" ]; then
        pass "reader.$_name" "reader reports java's bytes without normalising ($_shex)"
    else
        fail "reader.$_name" "reader normalised: shell $_shex != java $_jhex"
    fi
}
reader_matches_java trailing_space k 'k=value '
reader_matches_java trailing_tab   k "k=value${tabc}"

# --- the CHECKED-IN files themselves, compared key by key -----------------
root=$(cd "$here/.." && pwd)
compare_real() {
    _f=$1; shift
    [ -f "$_f" ] || return 0
    for _k in "$@"; do
        _shex=$(hexof "$(props_value "$_f" "$_k")")
        _jhex=$("$JAVA" -cp "$work" PropsEcho "$_f" "$_k" | awk -F'\t' '{print $3}')
        if [ "$_shex" = "$_jhex" ]; then
            pass "real.$(basename "$_f").$_k" "identical bytes"
        else
            fail "real.$(basename "$_f").$_k" "shell $_shex != java $_jhex"
        fi
    done
}
compare_real "$root/gradle/toolchain.properties" \
    gradle.version gradle.distribution.url gradle.distribution.sha256 \
    gradle.wrapper.jar.sha256 jdk.major agp.version sdk.ndk
compare_real "$root/gradle.properties" org.gradle.warning.mode
compare_real "$root/gradle/gradle-daemon-jvm.properties" toolchainVersion
compare_real "$root/gradle/wrapper/gradle-wrapper.properties" \
    distributionUrl distributionSha256Sum

# The row count is itself an oracle. Losing a row would otherwise look exactly
# like passing, and the count must not depend on the host: every row above is
# synthetic or reads a file this repository owns -- the optional, untracked
# local.properties is deliberately NOT one of them, so a developer who has one
# and a CI host that does not get the identical number.
CORPUS_EXPECTED_ROWS=52
if [ "$rows" -ne "$CORPUS_EXPECTED_ROWS" ]; then
    fail inventory "ran $rows rows, expected exactly $CORPUS_EXPECTED_ROWS; a row was added or lost"
fi

printf '\nproperties corpus: %d row(s), %d failure(s)\n' "$rows" "$fails"
[ "$fails" -eq 0 ] || exit 1
exit 0
