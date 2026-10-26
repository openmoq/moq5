# Staging one coherent picotls prefix per ABI. Sourced by the topology recipe
# and by its synthetic fixture; the checker verifies the result independently.
#
# LibMoQ's CMake takes source-tree picoquic's picotls from ONE prefix:
# MOQ_PICOTLS_PREFIX is the authority for both its headers and its archives.
# The Phase 0.2 dependency tree keeps the two apart -- the headers in the pinned
# picotls checkout, the archives per ABI -- so a build stages one root per ABI:
#
#   <prefix>/include/...   byte-identical copies of every accepted header
#   <prefix>/lib/<a>       byte-identical copies of the ABI's picotls archives
#
# and records, per file, its path inside the prefix, its SHA-256 and the
# accepted source it was copied from. The copies are not a second dependency
# authority: the final link still reads the accepted archives, and the checker
# proves every copy equal to its source.
#
# Requires decl_sha256 and decl_is_sha256 (lib-declaration.sh).

# The picotls archives a prefix carries: exactly the ones the link pins.
PTLS_PREFIX_ARCHIVES="libpicotls-core.a libpicotls-minicrypto.a libpicotls-openssl.a"

# ptls_prefix_fail <message> -- report and fail.
ptls_prefix_fail() { printf 'ptls-prefix: %s\n' "$1" >&2; return 1; }

# ptls_prefix_hash <file> -- echo a validated SHA-256, or fail.
ptls_prefix_hash() {
    [ -f "$1" ] || return 1
    _pph=$(decl_sha256 "$1") || return 1
    decl_is_sha256 "$_pph" || return 1
    printf '%s' "$_pph"
}

# ptls_stage_prefix <include-src> <archive-dir> <prefix> <manifest>
#
# Creates <prefix>, which must not exist; copies the complete header tree of
# <include-src> and the PTLS_PREFIX_ARCHIVES of <archive-dir> into it; verifies
# that the staged tree holds exactly those files, each byte-identical to its
# source; then writes <manifest>, one line per file:
#
#   <path inside the prefix> TAB <sha256> TAB <accepted source path>
#
# Fails closed: a missing source, a non-regular entry in the source header tree
# or in the staged tree, a missing or extra staged file, or a hash difference is
# a failure, and no manifest is written.
ptls_stage_prefix() {
    _pinc=$1; _parc=$2; _ppre=$3; _pman=$4
    [ -d "$_pinc" ] || { ptls_prefix_fail "no picotls header tree at $_pinc"; return 1; }
    [ -e "$_ppre" ] && { ptls_prefix_fail "the prefix already exists: $_ppre"; return 1; }
    _pspec=$(find "$_pinc" ! -type d ! -type f -print 2>/dev/null) \
        || { ptls_prefix_fail "cannot inventory $_pinc"; return 1; }
    [ -z "$_pspec" ] || { ptls_prefix_fail "non-regular entry in the header tree: $(printf '%s\n' "$_pspec" | sed -n '1p')"; return 1; }
    for _pa in $PTLS_PREFIX_ARCHIVES; do
        [ -f "$_parc/$_pa" ] || { ptls_prefix_fail "no accepted archive $_parc/$_pa"; return 1; }
    done

    mkdir -p "$_ppre/include" "$_ppre/lib" || { ptls_prefix_fail "cannot create $_ppre"; return 1; }
    # Each inventory producer writes to a file and its status is checked BEFORE
    # anything parses the list: in a pipeline the status would be the last
    # command's, and a failed find that had already emitted part of the tree
    # would pass as a complete inventory.
    _pwant="$_ppre.want"
    ( cd "$_pinc" && find . -type f -print ) > "$_pwant.raw" \
        || { ptls_prefix_fail "the inventory of $_pinc failed"; return 1; }
    { sed 's|^\./|include/|' "$_pwant.raw"
      for _pa in $PTLS_PREFIX_ARCHIVES; do printf 'lib/%s\n' "$_pa"; done; } > "$_pwant.unsorted" \
        || { ptls_prefix_fail "cannot build the staging list"; return 1; }
    LC_ALL=C sort "$_pwant.unsorted" > "$_pwant" || { ptls_prefix_fail "cannot sort the staging list"; return 1; }

    while IFS= read -r _prel; do
        case $_prel in
            include/*) _psrc="$_pinc/${_prel#include/}" ;;
            lib/*)     _psrc="$_parc/${_prel#lib/}" ;;
            *) ptls_prefix_fail "unclassified staging entry: $_prel"; return 1 ;;
        esac
        mkdir -p "$(dirname "$_ppre/$_prel")" || { ptls_prefix_fail "cannot create the directory for $_prel"; return 1; }
        cp "$_psrc" "$_ppre/$_prel" || { ptls_prefix_fail "cannot copy $_psrc"; return 1; }
    done < "$_pwant"

    # The staged tree must hold exactly the listed files, and only files.
    _pspec=$(find "$_ppre" ! -type d ! -type f -print 2>/dev/null) \
        || { ptls_prefix_fail "cannot inventory the staged prefix"; return 1; }
    [ -z "$_pspec" ] || { ptls_prefix_fail "non-regular entry in the staged prefix: $(printf '%s\n' "$_pspec" | sed -n '1p')"; return 1; }
    _pgot="$_ppre.got"
    ( cd "$_ppre" && find . -type f -print ) > "$_pgot.raw" \
        || { ptls_prefix_fail "the inventory of the staged prefix failed"; return 1; }
    sed 's|^\./||' "$_pgot.raw" > "$_pgot.unsorted" || { ptls_prefix_fail "cannot read the staged inventory"; return 1; }
    LC_ALL=C sort "$_pgot.unsorted" > "$_pgot" || { ptls_prefix_fail "cannot sort the staged inventory"; return 1; }
    cmp -s "$_pwant" "$_pgot" || { ptls_prefix_fail "the staged prefix does not hold exactly the accepted files"; return 1; }

    : > "$_pman.n" || { ptls_prefix_fail "cannot start $_pman"; return 1; }
    while IFS= read -r _prel; do
        case $_prel in
            include/*) _psrc="$_pinc/${_prel#include/}" ;;
            *)         _psrc="$_parc/${_prel#lib/}" ;;
        esac
        _phs=$(ptls_prefix_hash "$_psrc") || { ptls_prefix_fail "cannot hash $_psrc"; return 1; }
        _phd=$(ptls_prefix_hash "$_ppre/$_prel") || { ptls_prefix_fail "cannot hash the staged $_prel"; return 1; }
        [ "$_phs" = "$_phd" ] || { ptls_prefix_fail "the staged $_prel differs from $_psrc"; return 1; }
        printf '%s\t%s\t%s\n' "$_prel" "$_phd" "$_psrc" >> "$_pman.n" \
            || { ptls_prefix_fail "cannot write $_pman"; return 1; }
    done < "$_pwant"
    rm -f "$_pwant" "$_pwant.raw" "$_pwant.unsorted" "$_pgot" "$_pgot.raw" "$_pgot.unsorted" \
        || { ptls_prefix_fail "cannot tidy the staging lists"; return 1; }
    mv "$_pman.n" "$_pman" || { ptls_prefix_fail "cannot finalise $_pman"; return 1; }
}
