# shellcheck shell=sh
# MoQ5 Android/Kotlin bindings -- the ONE declaration authority.
#
# Sourced by both tools/gradlew-verified (before Gradle launches) and
# tools/verify-toolchain.sh (inside the build). Having a single implementation
# is the point: two parsers would drift, and the pre-wrapper layer would then
# accept a declaration the in-build layer rejects, or the reverse.
#
# Two properties of every reader below:
#   * keys are compared STRUCTURALLY -- each line is split at its first '=' and
#     the key compared literally. Interpolating a key into a pattern is what let
#     `gradle.version` match a misspelled `gradleXversion`.
#   * cardinality is checked. Java and Gradle property loading is LAST-WINS, so
#     a duplicated key silently changes the effective value while a first-match
#     reader still sees the old one. Required keys must appear exactly once.
#
# This file defines functions only; it runs nothing on its own.

# --- canonical grammar, enforced BEFORE any value is read ------------------
# The readers below recognise `key=value` only. Java property files accept far
# more -- `key:value`, `key value`, escaped and \uXXXX-escaped keys, and
# logical-line continuation -- and Gradle, settings.gradle.kts and the wrapper
# all load these files with java.util.Properties. So counting one spelling is
# NOT effective cardinality: a second `distributionUrl:` line is invisible to a
# `=`-only reader while being the value the wrapper actually uses.
#
# Rather than reimplement Java's escape grammar (easy to get subtly wrong, and
# the bug would be silent), this repository declares a strict SUBSET and
# rejects anything outside it by file and line. Inside the subset the shell
# reader and java.util.Properties cannot disagree.
#
# The subset -- KEYS:
#   * blank lines, and comments whose first non-blank character is '#' or '!';
#   * every other logical line uses a literal '=' as its first separator;
#   * the key is one or more of [A-Za-z0-9._-] with NO surrounding whitespace.
#     Leading whitespace is REJECTED rather than stripped: normalising it would
#     mean the checked-in file has one spelling and the contract another.
#
# The subset -- VALUES. Constraining keys alone was not enough, and the gap was
# fail-open: java.util.Properties keeps a trailing space, this reader stripped
# it, so `org.gradle.warning.mode=fail ` satisfied the mandatory `== fail` check
# while Gradle received a different value. Java also decodes value escapes this
# reader does not (`a\=b`, `a\u003db`, `a\tb` all differ). So:
#   * every byte on the line is printable ASCII (0x20-0x7E) or a horizontal
#     tab. java.util.Properties.load(InputStream) decodes the file as
#     ISO-8859-1 while this reader consumes the host bytes, so a raw UTF-8
#     'e-acute' is U+00E9 here and U+00C3 U+00A9 there. Rather than pick a
#     re-encoding, every byte outside printable ASCII is rejected -- including
#     DEL, NUL, vertical tab and form feed, which Java also treats as leading
#     property whitespace. The checked-in files need nothing else;
#   * no carriage return anywhere on the line;
#   * the value may not begin or end with a space or tab -- REJECTED, not
#     trimmed, so no whitespace difference can survive into a comparison;
#   * the ONLY backslash sequence permitted is `\:`, which both parsers decode
#     to ':'. Every other backslash -- `\=`, `\\`, `\t`, `\n`, `\r`, `\f`,
#     `\uXXXX`, an escaped ordinary character -- is rejected by name rather than
#     passed through with two meanings. `\:` is admitted because it is the only
#     escape the checked-in files use (both distribution URLs) and because it is
#     decoded identically on both sides;
#   * ':' , '=' and spaces INSIDE the value are literal and need no escape.
# Continuation is rejected under Java's own rule (an odd number of trailing
# backslashes), so no logical line spans two physical lines.
decl_validate_grammar() {
    _f=$1
    [ -f "$_f" ] || return 0
    _base=$(basename "$_f")

    # NUL first, and WITHOUT command substitution: a NUL cannot survive being
    # carried through a shell variable, so asking one to do it would lose the
    # very byte under test. `od` turns the file into DECIMAL BYTE VALUES, so the
    # byte itself never leaves the pipe, and awk tracks physical line and column
    # across that stream -- the NUL therefore fails in the same shape as every
    # other byte, with basename, line, byte and column.
    if ! LC_ALL=C od -An -v -tu1 < "$_f" |
         LC_ALL=C awk -v file="$_base" '
            BEGIN { line = 1; col = 1 }
            {
                for (i = 1; i <= NF; i++) {
                    b = $i + 0
                    if (b == 0) {
                        printf "FAIL[properties.grammar]: %s:%d: byte 0x00 at column %d is outside printable ASCII; java.util.Properties decodes this file as ISO-8859-1 while the shell reader sees host bytes, so such a byte has two meanings\n", file, line, col
                        # `exit 1` in a rule still runs END, whose own exit
                        # would OVERRIDE this status -- so the flag, not the
                        # exit code, is what END reports.
                        found = 1
                        exit 1
                    }
                    if (b == 10) { line++; col = 1 } else { col++ }
                }
            }
            END { exit (found ? 1 : 0) }'; then
        return 1
    fi

    # LC_ALL=C so the byte classes below cannot change with the host locale.
    LC_ALL=C awk -v file="$_base" '
        function ord(c,   i) {
            if (_ord_init == 0) {
                for (i = 0; i < 256; i++) _ord[sprintf("%c", i)] = i
                _ord_init = 1
            }
            return _ord[c]
        }
        # A logical line continues when it ends with an ODD number of
        # backslashes; that is exactly Java'"'"'s rule, and it is rejected rather
        # than interpreted.
        {
            line = $0
            if (line ~ /\r/) {
                printf "FAIL[properties.grammar]: %s:%d: carriage return in a declaration line\n", file, NR
                bad++
                next
            }
            # Printable ASCII (0x20-0x7E) or horizontal tab. Anything else --
            # a raw non-ASCII byte, DEL, vertical tab, form feed -- is where the
            # two parsers stop agreeing, so it is rejected rather than admitted.
            if (line ~ /[^ -~\t]/) {
                for (bi = 1; bi <= length(line); bi++) {
                    bc = substr(line, bi, 1)
                    if (bc !~ /[ -~\t]/) {
                        printf "FAIL[properties.grammar]: %s:%d: byte 0x%02x at column %d is outside printable ASCII; java.util.Properties decodes this file as ISO-8859-1 while the shell reader sees host bytes, so such a byte has two meanings\n", file, NR, ord(bc), bi
                        break
                    }
                }
                bad++
                next
            }
            n = 0
            for (i = length(line); i >= 1; i--) {
                if (substr(line, i, 1) == "\\") n++; else break
            }
            if (n % 2 == 1) {
                printf "FAIL[properties.grammar]: %s:%d: line continuation is not permitted; every declaration must be one line\n", file, NR
                bad++
                next
            }
            if (line ~ /^[ \t]*$/) next
            if (line ~ /^[ \t]*[#!]/) next

            eq = index(line, "=")
            if (eq == 0) {
                printf "FAIL[properties.grammar]: %s:%d: no \x27=\x27 separator; \x27:\x27 and whitespace separators are valid Java property syntax and are not permitted here\n", file, NR
                bad++
                next
            }
            key = substr(line, 1, eq - 1)
            # No normalisation: surrounding whitespace is rejected, not stripped.
            if (key !~ /^[A-Za-z0-9._-]+$/) {
                printf "FAIL[properties.grammar]: %s:%d: key \x27%s\x27 is outside the permitted set [A-Za-z0-9._-] with no surrounding whitespace (escapes, colons and whitespace in keys are not permitted)\n", file, NR, key
                bad++
                next
            }

            val = substr(line, eq + 1)
            if (val ~ /^[ \t]/ || val ~ /[ \t]$/) {
                printf "FAIL[properties.grammar]: %s:%d: value of \x27%s\x27 has leading or trailing whitespace; java.util.Properties keeps trailing whitespace and skips leading whitespace, so it is rejected rather than trimmed\n", file, NR, key
                bad++
                next
            }
            # Walk the value: the only escape this subset admits is `\:`.
            for (i = 1; i <= length(val); i++) {
                c = substr(val, i, 1)
                if (c != "\\") continue
                nxt = substr(val, i + 1, 1)
                if (nxt == ":") { i++; continue }
                printf "FAIL[properties.grammar]: %s:%d: value of \x27%s\x27 contains the escape \x27\\%s\x27; only \x27\\:\x27 is admitted, because every other Java escape decodes differently here\n", file, NR, key, nxt
                bad++
                break
            }
        }
        END { exit (bad > 0) }
    ' "$_f"
}

# --- exact-key readers -----------------------------------------------------
# props_count <file> <key>  -> number of non-comment definitions of <key>
props_count() {
    [ -f "$1" ] || { printf '0'; return; }
    awk -v want="$2" -F= '
        /^[[:space:]]*#/ { next }
        /^[[:space:]]*!/ { next }
        NF < 2 { next }
        { k = $1
          if (k == want) n++ }
        END { print n + 0 }' "$1"
}
# props_value <file> <key>  -> the FIRST definition's value, `\:` decoded to
# ':' exactly as java.util.Properties does. NOTHING is trimmed or normalised:
# the grammar has already rejected any value whose two decodings could differ,
# so trimming here could only mask a difference, never fix one.
props_value() {
    [ -f "$1" ] || return 0
    awk -v want="$2" -F= '
        /^[[:space:]]*#/ { next }
        /^[[:space:]]*!/ { next }
        NF < 2 { next }
        { k = $1
          if (k != want) next
          sub(/^[^=]*=/, "", $0)
          print; exit }' "$1" | sed 's/\\:/:/g'
}

# --- grammar ---------------------------------------------------------------
decl_is_version() { printf '%s' "$1" | grep -Eq '^[0-9]+(\.[0-9]+)*$'; }
decl_is_sha256()  { printf '%s' "$1" | grep -Eq '^[0-9a-f]{64}$'; }

# --- SHA-256, portable and fail-closed -------------------------------------
# One helper for every call site. A Linux host commonly has sha256sum and no
# shasum; a macOS host has shasum. Having no usable hasher is a named FAILURE,
# never a skipped check, and the output is validated as a digest rather than
# trusted for its shape.
decl_sha256() {
    _f=$1
    [ -f "$_f" ] || { printf ''; return 1; }
    if command -v sha256sum >/dev/null 2>&1; then
        _h=$(sha256sum "$_f" 2>/dev/null | cut -d' ' -f1)
    elif command -v shasum >/dev/null 2>&1; then
        _h=$(shasum -a 256 "$_f" 2>/dev/null | cut -d' ' -f1)
    else
        printf ''
        return 2          # no hasher at all -- the caller names this
    fi
    decl_is_sha256 "$_h" || { printf ''; return 1; }
    printf '%s' "$_h"
}

# --- required declarations -------------------------------------------------
DECL_REQUIRED_PINS="gradle.version gradle.distribution.sha256 gradle.distribution.url
gradle.wrapper.jar.sha256 jdk.major agp.version agp.minGradle agp.minJdk
sdk.platform sdk.buildTools agp.minBuildTools sdk.cmdlineTools sdk.ndk"

DECL_VERSION_PINS="gradle.version jdk.major agp.version agp.minGradle agp.minJdk
sdk.buildTools agp.minBuildTools sdk.cmdlineTools sdk.ndk"

DECL_SHA_PINS="gradle.distribution.sha256 gradle.wrapper.jar.sha256"

DECL_WRAPPER_KEYS="distributionUrl distributionSha256Sum"

# Every failure below is printed by this one function. An earlier revision
# passed an emitter BY NAME and called it indirectly; that works in shell but
# neither a reader nor a linter can follow it, and both call sites wanted the
# same format anyway.
# Named reasons go to STDERR; decl_preflight's return value goes to STDOUT.
# Keeping them on separate streams is what lets a caller capture the count
# without swallowing the reason -- an earlier revision redirected stdout and
# silently lost every named diagnostic.
decl_fail() { printf 'FAIL[%s]: %s\n' "$1" "$2" >&2; }

# decl_check_one <file> <key> <checkname>
# Exactly one non-empty definition, or a named failure.
decl_check_one() {
    _file=$1; _key=$2; _check=$3
    _n=$(props_count "$_file" "$_key")
    if [ "$_n" -eq 0 ]; then
        decl_fail "$_check" "pin '$_key' is missing from $(basename "$_file")"
        return 1
    elif [ "$_n" -gt 1 ]; then
        decl_fail "$_check" \
            "pin '$_key' is declared $_n times in $(basename "$_file"); exactly one is required (property loading is last-wins, so a duplicate silently changes the effective value)"
        return 1
    elif [ -z "$(props_value "$_file" "$_key")" ]; then
        decl_fail "$_check" "pin '$_key' is empty in $(basename "$_file")"
        return 1
    fi
    return 0
}

# decl_preflight <root>
# The complete declaration contract, independent of Gradle: every required pin
# with correct cardinality and grammar, the project-owned property files, the
# wrapper property keys, and the wrapper script itself. Returns the number of
# failures. Both entry points run exactly this.
decl_preflight() {
    _root=$1
    _fails=0
    _props="$_root/gradle/toolchain.properties"

    if [ ! -f "$_props" ]; then
        decl_fail toolchain.declaration "missing $_props"
        return 1
    fi

    # Grammar first, for EVERY file this authority consumes -- including the
    # optional local.properties, which is not excused merely because it has no
    # canonical sdk.dir line. Nothing below reads a value until this passes.
    for _gf in "$_props" \
               "$_root/gradle.properties" \
               "$_root/gradle/gradle-daemon-jvm.properties" \
               "$_root/gradle/wrapper/gradle-wrapper.properties" \
               "$_root/local.properties"; do
        [ -f "$_gf" ] || continue
        decl_validate_grammar "$_gf" >&2 || _fails=$((_fails + 1))
    done
    if [ "$_fails" -ne 0 ]; then
        printf '%s' "$_fails"
        return "$_fails"
    fi

    for _k in $DECL_REQUIRED_PINS; do
        decl_check_one "$_props" "$_k" toolchain.declaration || _fails=$((_fails + 1))
    done
    [ "$_fails" -eq 0 ] || { printf '%s' "$_fails"; return "$_fails"; }

    for _k in $DECL_VERSION_PINS; do
        _v=$(props_value "$_props" "$_k")
        decl_is_version "$_v" || {
            decl_fail toolchain.version "pin '$_k' is not a numeric dotted version: '$_v'"
            _fails=$((_fails + 1))
        }
    done
    for _k in $DECL_SHA_PINS; do
        _v=$(props_value "$_props" "$_k")
        decl_is_sha256 "$_v" || {
            decl_fail toolchain.version "pin '$_k' is not a SHA-256 hex digest: '$_v'"
            _fails=$((_fails + 1))
        }
    done

    # gradle.properties: project-owned, so its required key is exactly-once too.
    _gp="$_root/gradle.properties"
    if [ ! -f "$_gp" ]; then
        decl_fail gradle.properties "missing $_gp"
        _fails=$((_fails + 1))
    else
        if [ "$(props_count "$_gp" org.gradle.java.home)" -ne 0 ]; then
            decl_fail gradle.properties.javaHome \
                "gradle.properties pins org.gradle.java.home, which is an installation path"
            _fails=$((_fails + 1))
        fi
        if decl_check_one "$_gp" org.gradle.warning.mode gradle.properties.warningMode; then
            _wm=$(props_value "$_gp" org.gradle.warning.mode)
            [ "$_wm" = "fail" ] || {
                decl_fail gradle.properties.warningMode \
                    "org.gradle.warning.mode is '$_wm', must be 'fail'"
                _fails=$((_fails + 1))
            }
        else
            _fails=$((_fails + 1))
        fi
    fi

    # Daemon JVM criterion: project-owned, exactly-once, and equal to jdk.major.
    _dj="$_root/gradle/gradle-daemon-jvm.properties"
    if [ ! -f "$_dj" ]; then
        decl_fail jdk.criteria "missing $_dj"
        _fails=$((_fails + 1))
    elif decl_check_one "$_dj" toolchainVersion jdk.criteria; then
        _dv=$(props_value "$_dj" toolchainVersion)
        _jm=$(props_value "$_props" jdk.major)
        [ "$_dv" = "$_jm" ] || {
            decl_fail jdk.criteria "daemon criterion is '$_dv', declared jdk.major '$_jm'"
            _fails=$((_fails + 1))
        }
    else
        _fails=$((_fails + 1))
    fi

    # Wrapper properties: structural, not "the strings happened to differ".
    _wp="$_root/gradle/wrapper/gradle-wrapper.properties"
    if [ ! -f "$_wp" ]; then
        decl_fail gradle.wrapper "missing $_wp"
        _fails=$((_fails + 1))
    else
        for _k in $DECL_WRAPPER_KEYS; do
            decl_check_one "$_wp" "$_k" gradle.wrapper.properties || _fails=$((_fails + 1))
        done
    fi

    # The wrapper script itself must exist and be executable.
    if [ ! -f "$_root/gradlew" ]; then
        decl_fail gradle.wrapper "missing $_root/gradlew"
        _fails=$((_fails + 1))
    elif [ ! -x "$_root/gradlew" ]; then
        decl_fail gradle.wrapper "$_root/gradlew is not executable"
        _fails=$((_fails + 1))
    fi

    # local.properties is OPTIONAL. Absent is fine; present-but-duplicated is
    # not, because Gradle would take the last value while a reader takes the
    # first.
    _lp="$_root/local.properties"
    if [ -f "$_lp" ]; then
        _n=$(props_count "$_lp" sdk.dir)
        if [ "$_n" -gt 1 ]; then
            decl_fail sdk.declaration \
                "local.properties declares sdk.dir $_n times; exactly one is required"
            _fails=$((_fails + 1))
        elif [ "$_n" -eq 1 ] && [ -z "$(props_value "$_lp" sdk.dir)" ]; then
            decl_fail sdk.declaration "local.properties declares an empty sdk.dir"
            _fails=$((_fails + 1))
        fi
    fi

    printf '%s' "$_fails"
    return "$_fails"
}
