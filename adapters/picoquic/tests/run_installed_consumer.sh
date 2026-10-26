#!/usr/bin/env bash
#
# Runs one installed-tree consumer program and proves which LibMoQ/PicoQUIC
# images it loaded.
#
# usage: run_installed_consumer.sh [--expect-no-image] <installed libdir> <program> [args...]
#
# The package's metadata owns compile and link flags; finding a SHARED library
# in a private, nonstandard prefix at run time is the consumer's own policy.
# Public Linux libraries carry no RPATH/RUNPATH, and an executable's RUNPATH
# does not apply to the libraries it loads, so on Linux this runner gives the
# program's runtime invocation exactly the installed libdir as LD_LIBRARY_PATH
# and nothing inherited. It never touches configure, link, or exported
# metadata. On macOS the installed images' @rpath names resolve through the
# program's own LC_RPATH, so no loader variable is set.
#
# The install is SHARED when <libdir> holds a shared libmoq-core. Then:
#   - the program must need at least one LibMoQ/PicoQUIC image;
#   - the loader trace must resolve every dependency ("=> not found" fails);
#   - every LibMoQ/PicoQUIC image the program or those images need must be
#     loaded, and the loaded FILE -- the canonical path after every symlink,
#     not just its directory -- must be in <libdir>: a build-tree, source-tree
#     or system copy, or a prefix symlink to one, fails by name;
#   - the program itself must exit 0.
# --expect-no-image states the opposite for one program of a shared install
# (one that links only a dependency the consumer built from source): it must
# need, and the loader must load, exactly zero LibMoQ/PicoQUIC images, under
# the same isolated search path.
# Otherwise the install is STATIC: the program must need no LibMoQ/PicoQUIC
# image at all, and must exit 0.
set -euo pipefail

expect_none=0
if [ "${1-}" = --expect-no-image ]; then
    expect_none=1
    shift
fi
if [ "$#" -lt 2 ]; then
    echo "FAIL: usage: run_installed_consumer.sh [--expect-no-image] <installed libdir> <program> [args...]" >&2
    exit 2
fi
libdir_arg="$1"; program="$2"; shift 2

if [ ! -d "$libdir_arg" ]; then
    echo "FAIL: installed libdir '$libdir_arg' is not a directory" >&2
    exit 1
fi
if [ ! -x "$program" ]; then
    echo "FAIL: consumer program '$program' is not an executable file" >&2
    exit 1
fi
libdir="$(cd "$libdir_arg" && pwd -P)"

os="$(uname -s)"
case "$os" in
    Darwin) suffix=.dylib ;;
    *)      suffix=.so ;;
esac
if [ -e "$libdir/libmoq-core$suffix" ]; then mode=shared; else mode=static; fi

is_ours() {  # <file name>
    case "$1" in
        libmoq*|libpico*) return 0 ;;
    esac
    return 1
}

# needed_of <image>: the image's direct library dependencies, one file name
# per line (ELF DT_NEEDED; Mach-O load commands, with @rpath/ and directories
# stripped).
needed_of() {
    local out
    case "$os" in
        Darwin)
            out="$(otool -L "$1")"
            # The first line names the image itself; a dylib also lists its
            # own install name first, which is not a dependency.
            printf '%s\n' "$out" | sed -n '2,$s/^[[:space:]]*\([^[:space:]].*\) (compatibility version.*/\1/p' \
                | sed 's#.*/##' | { if [ "${1##*.}" = dylib ]; then sed 1d; else cat; fi; }
            ;;
        *)
            out="$(readelf -d "$1")"
            printf '%s\n' "$out" | sed -n 's/.*(NEEDED).*\[\(.*\)\]$/\1/p'
            ;;
    esac
}

has_line() {  # <exact line> <text>; grep reads the whole value, so no writer races it
    local rc=0
    grep -xF -- "$1" <<<"$2" >/dev/null || rc=$?
    case "$rc" in
        0) return 0 ;;
        1) return 1 ;;
        *) echo "FAIL: grep exited $rc while matching '$1'" >&2; exit 1 ;;
    esac
}

canonical_file() {  # <path>: the absolute path with every symlink resolved
    local out
    case "$os" in
        Darwin) out="$(realpath -- "$1")" ;;
        *)      out="$(readlink -f -- "$1")" ;;
    esac
    if [ -z "$out" ] || [ ! -f "$out" ]; then
        echo "FAIL: cannot resolve the loaded image $1 to a file" >&2
        exit 1
    fi
    printf '%s\n' "$out"
}

ours_needed() {  # <image>: the LibMoQ/PicoQUIC subset of needed_of
    local n list
    list="$(needed_of "$1")"
    while IFS= read -r n; do
        [ -n "$n" ] || continue
        if is_ours "$n"; then printf '%s\n' "$n"; fi
    done <<<"$list"
}

direct="$(ours_needed "$program")"

if [ "$mode" = static ]; then
    if [ -n "$direct" ]; then
        echo "FAIL: static install, but $program needs shared LibMoQ/PicoQUIC image(s): $(printf '%s' "$direct" | tr '\n' ' ')" >&2
        exit 1
    fi
    echo "== runtime: static install, no shared LibMoQ/PicoQUIC image to load =="
    case "$os" in
        Darwin) env -u DYLD_LIBRARY_PATH -u DYLD_FALLBACK_LIBRARY_PATH -u DYLD_INSERT_LIBRARIES "$program" "$@" ;;
        *)      env -u LD_LIBRARY_PATH -u LD_PRELOAD -u LD_AUDIT "$program" "$@" ;;
    esac
    exit 0
fi

if [ "$expect_none" = 1 ]; then
    if [ -n "$direct" ]; then
        echo "FAIL: $program is expected to need no LibMoQ/PicoQUIC image, but needs: $(printf '%s' "$direct" | tr '\n' ' ')" >&2
        exit 1
    fi
elif [ -z "$direct" ]; then
    echo "FAIL: shared install at $libdir, but $program needs no LibMoQ/PicoQUIC image" >&2
    exit 1
fi

rc=0
case "$os" in
    Darwin)
        # The trace is the real run's: the program's stdout stays on this
        # runner's stdout, and only the loader trace (stderr) is captured.
        { trace="$(env -u DYLD_LIBRARY_PATH -u DYLD_FALLBACK_LIBRARY_PATH -u DYLD_INSERT_LIBRARIES \
                       DYLD_PRINT_LIBRARIES=1 "$program" "$@" 2>&1 1>&3)" || rc=$?; } 3>&1
        if [ "$rc" != 0 ]; then
            printf '%s\n' "$trace" >&2
            echo "FAIL: the consumer exited $rc" >&2
            exit 1
        fi
        loaded="$(printf '%s\n' "$trace" | sed -n 's/^dyld\[[0-9]*\]: <[^>]*> //p')"
        ;;
    *)
        case "$libdir" in
            *:*) echo "FAIL: installed libdir '$libdir' contains ':', which LD_LIBRARY_PATH would split" >&2
                 exit 1 ;;
        esac
        trace="$(env -u LD_PRELOAD -u LD_AUDIT LD_LIBRARY_PATH="$libdir" \
                     LD_TRACE_LOADED_OBJECTS=1 "$program" 2>&1)" || rc=$?
        case "$trace" in
            *"=> not found"*)
                printf '%s\n' "$trace" >&2
                echo "FAIL: the loader could not resolve every dependency of $program" >&2
                exit 1 ;;
        esac
        if [ "$rc" != 0 ]; then
            printf '%s\n' "$trace" >&2
            echo "FAIL: the loader trace of $program exited $rc" >&2
            exit 1
        fi
        loaded="$(printf '%s\n' "$trace" | sed -n 's/^[[:space:]]*[^[:space:]]* => \(\/.*\) (0x[0-9a-f]*)$/\1/p')"
        ;;
esac

# Every LibMoQ/PicoQUIC image loaded must come from the installed libdir.
reals=""
images=""
n=0
while IFS= read -r path; do
    [ -n "$path" ] || continue
    base="$(basename "$path")"
    is_ours "$base" || continue
    # The loaded file itself, every symlink resolved: a prefix entry that
    # points outside the prefix is an outside image, whatever path the
    # loader reports.
    real="$(canonical_file "$path")"
    if [ "$(dirname "$real")" != "$libdir" ]; then
        echo "FAIL: the consumer loaded $path (file $real), outside the installed libdir $libdir" >&2
        exit 1
    fi
    n=$((n + 1))
    reals="$reals$real"$'\n'
    images="$images$path"$'\n'
done <<<"$loaded"

if [ "$expect_none" = 1 ]; then
    if [ "$n" != 0 ]; then
        echo "FAIL: $program is expected to load no LibMoQ/PicoQUIC image, but loaded $n: $(printf '%s' "$images" | tr '\n' ' ')" >&2
        exit 1
    fi
fi

# And every LibMoQ/PicoQUIC image needed -- by the program or by any of those
# images -- must be among them. A needed name is matched as the file it names
# in <libdir>, every symlink resolved, so a versioned name that the loader
# reports by its link target (as dyld does) still matches.
while IFS= read -r image; do
    [ -n "$image" ] || continue
    need="$(ours_needed "$image")"
    while IFS= read -r want; do
        [ -n "$want" ] || continue
        if [ ! -e "$libdir/$want" ]; then
            echo "FAIL: $(basename "$image") needs $want, but $libdir has no $want" >&2
            exit 1
        fi
        if ! has_line "$(canonical_file "$libdir/$want")" "$reals"; then
            echo "FAIL: $(basename "$image") needs $want, but the loader trace shows no $want from $libdir" >&2
            exit 1
        fi
    done <<<"$need"
done <<<"$program"$'\n'"$images"

if [ "$os" != Darwin ]; then
    env -u LD_PRELOAD -u LD_AUDIT LD_LIBRARY_PATH="$libdir" "$program" "$@" || rc=$?
    if [ "$rc" != 0 ]; then
        echo "FAIL: the consumer exited $rc" >&2
        exit 1
    fi
fi
if [ "$expect_none" = 1 ]; then
    echo "== runtime: shared install, $(basename "$program") loads no LibMoQ/PicoQUIC image, as expected =="
else
    echo "== runtime: $n LibMoQ/PicoQUIC image(s), all from $libdir =="
fi
