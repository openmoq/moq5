# shellcheck shell=sh
# One fail-closed SHA-256 implementation, shared by the producers and gates that
# were each doing it differently and wrongly.
#
# The defect this exists to remove: `shasum "$f" | sed -n 's/.../p'` observes the
# PARSER's status, never the hasher's. A `shasum` that printed a valid digest and
# then exited nonzero left the pipeline reporting success, and a gate built on it
# passed with every row green. Status is therefore checked BEFORE any parsing
# here, and no output is trusted until it has been validated.
#
# Rejected by name, never silently: a missing hasher, a nonzero status, any
# stderr, empty output, more than one output record, and anything that is not
# exactly 64 lowercase hexadecimal digits.

MOQ_HASH_TOOLS="sha256sum shasum"

# moq_hash_available -- echoes the resolved hasher, or fails naming the search.
moq_hash_available() {
    if command -v sha256sum >/dev/null 2>&1; then printf 'sha256sum'; return 0; fi
    if command -v shasum    >/dev/null 2>&1; then printf 'shasum';    return 0; fi
    echo "no SHA-256 tool found: tried $MOQ_HASH_TOOLS" >&2
    return 1
}

# moq_hash_file <path> [<scratch-dir>]
# Echoes the digest and returns 0, or prints a named diagnostic and returns 1.
moq_hash_file() {
    _mh_f=$1
    _mh_sc=${2:-${TMPDIR:-/tmp}}
    [ -f "$_mh_f" ] || { echo "not a file: $_mh_f" >&2; return 1; }

    _mh_tool=$(moq_hash_available) || return 1
    _mh_o="$_mh_sc/.moqhash.$$.out"
    _mh_e="$_mh_sc/.moqhash.$$.err"

    # No pipeline: the hasher's own status is what is observed.
    case $_mh_tool in
        sha256sum) sha256sum   -- "$_mh_f" > "$_mh_o" 2> "$_mh_e" ;;
        shasum)    shasum -a 256 -- "$_mh_f" > "$_mh_o" 2> "$_mh_e" ;;
    esac
    _mh_st=$?

    if [ "$_mh_st" -ne 0 ]; then
        echo "$_mh_tool exited $_mh_st on $_mh_f: $(sed -n '1p' "$_mh_e" 2>/dev/null)" >&2
        rm -f "$_mh_o" "$_mh_e"; return 1
    fi
    if [ -s "$_mh_e" ]; then
        echo "$_mh_tool wrote a diagnostic on $_mh_f: $(sed -n '1p' "$_mh_e")" >&2
        rm -f "$_mh_o" "$_mh_e"; return 1
    fi
    _mh_n=$(awk 'END { print NR }' "$_mh_o" 2>/dev/null)
    if [ "${_mh_n:-0}" -ne 1 ]; then
        echo "$_mh_tool produced ${_mh_n:-0} record(s) for $_mh_f; exactly 1 is required" >&2
        rm -f "$_mh_o" "$_mh_e"; return 1
    fi
    _mh_d=$(awk 'NR == 1 { print $1 }' "$_mh_o" 2>/dev/null)
    rm -f "$_mh_o" "$_mh_e"
    case $_mh_d in
        *[!0-9a-f]* | '') echo "$_mh_tool produced a malformed digest for $_mh_f" >&2; return 1 ;;
    esac
    if [ ${#_mh_d} -ne 64 ]; then
        echo "$_mh_tool produced a ${#_mh_d}-character digest for $_mh_f; 64 are required" >&2
        return 1
    fi
    printf '%s' "$_mh_d"
    return 0
}

# moq_is_sha256 <value> -- exactly 64 lowercase hex digits.
moq_is_sha256() {
    case ${1:-} in
        *[!0-9a-f]* | '') return 1 ;;
    esac
    [ ${#1} -eq 64 ]
}
