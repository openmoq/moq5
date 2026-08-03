#!/usr/bin/env bash
#
# wtquic_network_dep_fingerprint.sh - THE dependency fingerprint for the
# .wtquicNetwork runtime proof's link inputs. One implementation,
# consumed by check_wtquic_network_runtime.sh (to key its SwiftPM scratch path)
# and queried by check_wtquic_network_harness.sh (never re-derived there).
#
# The fingerprint covers the RESOLVED pkg-config closure of
# libmoq-service:
#   - the resolved --static --cflags/--libs argument string itself
#     (so .pc content/flag changes re-key even without archive changes),
#   - the CONTENTS of every static archive the closure's -l entries
#     resolve to across its -L directories (so replacing any archive
#     re-keys the link).
# Entries that resolve to no static archive (system dylibs, frameworks)
# do not contribute; ZERO resolved archives is an explicit failure,
# never an accidental empty-input hash.
#
# Usage: PKG_CONFIG_PATH=... wtquic_network_dep_fingerprint.sh
# Prints one line: a 16-hex fingerprint. Nonzero exit on any failure.

set -euo pipefail

die() { printf '[wtquic_network_dep_fingerprint] ERROR: %s\n' "$*" >&2; exit 1; }

command -v pkg-config >/dev/null 2>&1 || die "pkg-config not found"
[ -n "${PKG_CONFIG_PATH:-}" ] || die "PKG_CONFIG_PATH is not set"

flags=$(pkg-config --static --cflags --libs libmoq-service 2>/dev/null) \
    || die "pkg-config could not resolve libmoq-service (check PKG_CONFIG_PATH)"

# Collect -L directories and -l names from the RESOLVED closure.
# pkg-config SHELL-ESCAPES its output (a space in a prefix path comes
# out as "\ "), and plain word splitting does not reinterpret those
# backslashes — a relocated prefix containing spaces would shatter
# into truncated tokens. POSIX xargs parses exactly that escaping
# (backslash- and quote-aware) WITHOUT any shell evaluation, so each
# -L path survives as one argument, in linker order. (NUL-delimited
# re-emission via printf keeps the tokens intact on the way back in;
# paths with embedded newlines are out of scope.)
args=()
while IFS= read -r -d '' tok; do
    [ -n "$tok" ] && args+=("$tok")
done < <(printf '%s' "$flags" | xargs printf '%s\0' 2>/dev/null)
[ "${#args[@]}" -gt 0 ] || die "could not parse the closure flags: $flags"
libdirs=()
libnames=()
for tok in "${args[@]}"; do
    case "$tok" in
        -L*) libdirs+=("${tok#-L}") ;;
        -l*) libnames+=("${tok#-l}") ;;
    esac
done
[ "${#libnames[@]}" -gt 0 ] || die "closure resolved no -l entries: $flags"

# Resolve each -l to a static archive across the closure's -L dirs
# (first match wins, like the linker's search order).
archives=()
for name in "${libnames[@]}"; do
    for dir in "${libdirs[@]}"; do
        if [ -f "$dir/lib$name.a" ]; then
            archives+=("$dir/lib$name.a")
            break
        fi
    done
done
[ "${#archives[@]}" -gt 0 ] \
    || die "closure resolved ZERO static archives (dirs: ${libdirs[*]-})"

{
    printf '%s\n' "$flags"
    shasum "${archives[@]}"
} | shasum | cut -c1-16
