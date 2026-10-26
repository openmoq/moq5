# shellcheck shell=sh
# Minimal, exact ELF64 program-header helpers for the alignment fixtures.
#
# The alignment gate must be provable, and the only honest way to produce a
# packaged DSO whose FIRST LOAD is correctly aligned while a LATER one is not is
# to write that byte pattern deliberately. Relinking cannot do it: the linker
# gives every LOAD the same page size. So these helpers read and patch the
# program header table directly.
#
# Everything is fail-closed: an unreadable file, a non-ELF64 magic, a header
# that does not parse, or an out-of-range index returns non-zero rather than a
# value the caller could mistake for a measurement.

# elf_u16 <file> <offset>  -- little-endian 16-bit at byte offset
elf_u16() {
    _ef=$1; _eo=$2
    [ -r "$_ef" ] || return 1
    _b=$(dd if="$_ef" bs=1 skip="$_eo" count=2 2>/dev/null | od -An -tu1) || return 1
    # shellcheck disable=SC2086  # the split into byte words is the parse
    set -- $_b
    [ $# -eq 2 ] || return 1
    printf '%s' "$(( $1 + ($2 << 8) ))"
}

# elf_u64 <file> <offset>  -- little-endian 64-bit at byte offset
elf_u64() {
    _ef=$1; _eo=$2
    [ -r "$_ef" ] || return 1
    _b=$(dd if="$_ef" bs=1 skip="$_eo" count=8 2>/dev/null | od -An -tu1) || return 1
    # shellcheck disable=SC2086  # the split into byte words is the parse
    set -- $_b
    [ $# -eq 8 ] || return 1
    # assembled in two halves so the shell's signed arithmetic is never asked to
    # hold a value it cannot represent
    _lo=$(( $1 + ($2 << 8) + ($3 << 16) + ($4 << 24) ))
    _hi=$(( $5 + ($6 << 8) + ($7 << 16) + ($8 << 24) ))
    [ "$_hi" -eq 0 ] || return 1   # offsets beyond 4 GiB are not our fixtures
    printf '%s' "$_lo"
}

# elf_is_elf64_le <file> -- 0 when the magic and class/endianness are ours
elf_is_elf64_le() {
    _ef=$1
    _m=$(dd if="$_ef" bs=1 count=6 2>/dev/null | od -An -tx1 | tr -d ' \n') || return 1
    [ "$_m" = "7f454c460201" ] || return 1
    return 0
}

# elf_phoff/elf_phentsize/elf_phnum <file>
elf_phoff()     { elf_is_elf64_le "$1" || return 1; elf_u64 "$1" 32; }
elf_phentsize() { elf_is_elf64_le "$1" || return 1; elf_u16 "$1" 54; }
elf_phnum()     { elf_is_elf64_le "$1" || return 1; elf_u16 "$1" 56; }

# elf_ph_type <file> <index> -- p_type of program header <index>
elf_ph_type() {
    _ef=$1; _ei=$2
    _po=$(elf_phoff "$_ef") || return 1
    _pe=$(elf_phentsize "$_ef") || return 1
    _pn=$(elf_phnum "$_ef") || return 1
    [ "$_ei" -ge 0 ] && [ "$_ei" -lt "$_pn" ] || return 1
    _b=$(dd if="$_ef" bs=1 skip=$(( _po + _ei * _pe )) count=4 2>/dev/null | od -An -tu1) || return 1
    # shellcheck disable=SC2086  # the split into byte words is the parse
    set -- $_b
    [ $# -eq 4 ] || return 1
    printf '%s' "$(( $1 + ($2 << 8) + ($3 << 16) + ($4 << 24) ))"
}

# elf_load_indices <file> -- indices of PT_LOAD (1) headers, one per line
elf_load_indices() {
    _ef=$1
    _pn=$(elf_phnum "$_ef") || return 1
    _i=0
    while [ "$_i" -lt "$_pn" ]; do
        _t=$(elf_ph_type "$_ef" "$_i") || return 1
        [ "$_t" = "1" ] && printf '%s\n' "$_i"
        _i=$(( _i + 1 ))
    done
    return 0
}

# elf_set_ph_align <file> <index> <value> -- patch p_align in place
# p_align is the last 8 bytes of a 56-byte ELF64 program header entry.
elf_set_ph_align() {
    _ef=$1; _ei=$2; _ev=$3
    _po=$(elf_phoff "$_ef") || return 1
    _pe=$(elf_phentsize "$_ef") || return 1
    _pn=$(elf_phnum "$_ef") || return 1
    [ "$_pe" -eq 56 ] || return 1
    [ "$_ei" -ge 0 ] && [ "$_ei" -lt "$_pn" ] || return 1
    _at=$(( _po + _ei * _pe + 48 ))
    # eight little-endian bytes, written without disturbing the rest of the file
    _i=0
    while [ "$_i" -lt 8 ]; do
        _byte=$(( (_ev >> (_i * 8)) & 255 ))
        _oct=$(printf '%03o' "$_byte")
        # the escape is built as DATA and emitted through a constant format, so
        # no value ever reaches printf as a format string
        printf '%b' "\\0$_oct" \
            | dd of="$_ef" bs=1 seek=$(( _at + _i )) count=1 conv=notrunc 2>/dev/null || return 1
        _i=$(( _i + 1 ))
    done
    return 0
}
