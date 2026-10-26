# shellcheck shell=sh
# Gradle-module classification for the native DSO source digest.
#
# This is the ONE implementation. The permanent gate (check-input-roots.sh) and
# its self-test (selftest-input-roots.sh) both source this file and call the
# same function, so the self-test exercises the shipping classifier rather than
# a second copy of the algorithm that could drift from it.
#
# Every external command whose output contributes to a verdict has its status
# checked, and no such command sits in a pipeline: a pipeline reports only its
# LAST command's status, so a failing producer inside one silently becomes an
# empty result -- which is how a failing `sort` turned an undecidable
# classification into "ok" (chats/libmoq/1189 R2).

# The complete external-command set this library uses. `rm` and `mktemp` are
# included because cleanup and scratch creation are part of a correct run: a
# missing `rm` previously let the gate print three PASS rows, leak residue and
# exit 0.
MOQ_MODULES_TOOLS="find grep sort sed printf mktemp rm"

# The Gradle root project, relative to the repo. Every OTHER directory carrying
# a build script is a module whose sources must stay out of the native digest.
MOQ_ROOT_PROJECT="bindings/kotlin"
MOQ_BUILD_SCRIPT="build.gradle.kts"

# moq_modules_preflight <program-name>
# Verifies every required tool BEFORE any of them is used. Prints a named
# diagnostic and returns non-zero otherwise.
moq_modules_preflight() {
    _mp_prog=$1
    for _mp_t in $MOQ_MODULES_TOOLS; do
        if ! command -v "$_mp_t" >/dev/null 2>&1; then
            echo "$_mp_prog: required tool not found: $_mp_t" >&2
            return 1
        fi
    done
    return 0
}

# moq_classify_root <repo> <relative-root> <scratch-dir>
#
# Echoes exactly one verdict and returns 0, or returns non-zero when the root
# cannot be classified. A non-zero return means UNDECIDABLE and must be treated
# as a failure by the caller; it never means "ok".
#
#   absent                    the path is not a directory
#   under-module:<module>     the root is at or under a non-root module
#   contains-module:<module>  the root contains a non-root module
#   ok                        neither
moq_classify_root() {
    _cr=$1; _rel=$2; _sc=$3
    [ -d "$_sc" ] || return 1
    [ -d "$_cr/$_rel" ] || { printf 'absent'; return 0; }

    # (a) the root itself, and each ancestor up to the repo. Pure shell: no
    #     external command, so nothing here can fail silently.
    _d=$_rel
    while :; do
        if [ "$_d" != "$MOQ_ROOT_PROJECT" ] && [ -f "$_cr/$_d/$MOQ_BUILD_SCRIPT" ]; then
            printf 'under-module:%s' "$_d"
            return 0
        fi
        case $_d in
            */*) _d=${_d%/*} ;;
            *)   break ;;
        esac
    done

    # (b) any module nested BELOW the root. Each stage writes a file and its
    #     status is checked before the next stage reads it.
    _f_found="$_sc/found"
    _f_rel="$_sc/rel"
    _f_other="$_sc/other"
    _f_sorted="$_sc/sorted"

    find "$_cr/$_rel" -type f -name "$MOQ_BUILD_SCRIPT" -print > "$_f_found" 2>/dev/null || return 1
    sed "s|^$_cr/||; s|/$MOQ_BUILD_SCRIPT\$||" "$_f_found" > "$_f_rel" 2>/dev/null || return 1

    # The root project's own build script is the one legal hit. grep exits 1
    # when nothing remains, which is the legitimate "no module" answer; any
    # status above 1 is a real failure.
    grep -vxF "$MOQ_ROOT_PROJECT" "$_f_rel" > "$_f_other" 2>/dev/null
    _g=$?
    [ "$_g" -le 1 ] || return 1

    sort -o "$_f_sorted" "$_f_other" 2>/dev/null || return 1
    [ -e "$_f_sorted" ] || return 1

    # Emptiness is decided BEFORE the read, because the two answers are not the
    # same fact: an empty result is the legitimate "no nested module", while a
    # non-empty result the read cannot consume is a producer failure. Treating a
    # failed read as empty is how an unreadable result becomes "ok".
    #
    # The read uses the shell's own `read`, so no external command stands
    # between the sorted file and the verdict.
    if [ -s "$_f_sorted" ]; then
        # The status alone cannot decide: `read` also reports non-zero for a
        # final line with no trailing newline. The file is known non-empty
        # here, so an EMPTY result is the failure -- the read consumed nothing.
        _first=""
        IFS= read -r _first < "$_f_sorted" 2>/dev/null
        [ -n "$_first" ] || return 1
        printf 'contains-module:%s' "$_first"
        return 0
    fi

    printf 'ok'
    return 0
}
