# Sourced by the qualification scripts: where an installed SDK keeps its
# pkg-config files and libraries, the loader path its closure needs, the
# link mode it was installed with, and the link-search assertion the
# consumer gates make.
#
#   moq5_sdk_pkgconfig <prefix>       the directory holding libmoq-service.pc,
#                                     or for an SDK without the service the
#                                     one holding libmoq-core.pc (plus
#                                     MOQ5_SDK_PKG_CONFIG_PATH, see below)
#   moq5_sdk_libdir <prefix>          the SDK's own library directory
#   moq5_sdk_top_package <prefix>     the SDK's highest tier package:
#                                     libmoq-service when it has one, else
#                                     libmoq-core
#   moq5_sdk_runtime_path <prefix> [package]
#                                     every -L directory of the package's
#                                     pkg-config closure (default: the top
#                                     package), both link modes, colon-joined
#   moq5_sdk_export_loader <prefix>   prepend that path to DYLD_LIBRARY_PATH
#                                     and LD_LIBRARY_PATH
#   moq5_sdk_link_mode <prefix>       static or shared, by the installed
#                                     service library (the core library for
#                                     an SDK without the service)
#   moq5_sdk_export_link_mode <prefix> state that mode to the pkg-config crate
#                                     for every tier package it probes
#                                     (LIBMOQ_<TIER>_STATIC / _DYNAMIC for the
#                                     core, sim, media and service), each
#                                     opposite variable cleared, and set
#                                     MOQ5_SDK_LINK_MODE; it changes the
#                                     caller's environment, so it is called
#                                     directly, never inside $(...)
#   moq5_sdk_version <prefix>         the SDK's version (its core package's)
#   moq5_sdk_copy_pc <prefix> <dir> <package...>
#                                     copies of the named packages of the
#                                     SDK in <dir>, each with its prefix
#                                     variable set to the prefix pkg-config
#                                     resolves for it, so the copies still
#                                     describe the SDK's own files
#   moq5_sdk_owned_view <prefix> <dir> [package left out...]
#                                     copies of every SDK-owned package
#                                     (libmoq.pc and libmoq-*.pc) in <dir>:
#                                     one directory and one version, as one
#                                     SDK, for a fixture that stands one of
#                                     them in
#   moq5_sdk_assert_link_search <log> <prefix>
#                                     every rustc-link-search directory a
#                                     verbose build log emitted is exactly a
#                                     directory of the closure, and the
#                                     SDK's own library directory is one
#
# cargo adds only directories inside the target tree to the loader path of
# the binaries it runs, so a shared SDK installed elsewhere (every SDK here)
# needs the loader path set explicitly; for a static SDK the path is unused.
# PKG_CONFIG names the pkg-config executable (default pkg-config).
# MOQ5_SDK_PKG_CONFIG_PATH names the pkg-config directories of the SDK's
# dependency prefixes when its closure spans them (the WTQuic managed SDK
# Requires the wtquic install's own .pc files). The closure is read as the
# pkg-config crate reads it, system library directories included and in
# both link modes (a static closure adds the .pc files' Libs.private
# directories), so what the rows compare against is what the build saw.

# The tier packages moq5-sys probes, by the variable stem the pkg-config
# crate derives from each name.
MOQ5_SDK_TIER_STEMS="LIBMOQ_CORE LIBMOQ_SIM LIBMOQ_MEDIA LIBMOQ_SERVICE"

moq5_sdk_pkgconfig() {
    local d pc
    for pc in libmoq-service.pc libmoq-core.pc; do
        for d in lib/pkgconfig lib64/pkgconfig; do
            if [ -f "$1/$d/$pc" ]; then
                echo "$1/$d${MOQ5_SDK_PKG_CONFIG_PATH:+:$MOQ5_SDK_PKG_CONFIG_PATH}"
                return 0
            fi
        done
    done
    echo "sdk_paths.sh: no libmoq-service.pc or libmoq-core.pc under $1" >&2
    return 1
}

moq5_sdk_top_package() {
    local pc
    pc=$(moq5_sdk_pkgconfig "$1") || return 1
    if [ -f "${pc%%:*}/libmoq-service.pc" ]; then echo libmoq-service; else echo libmoq-core; fi
}

moq5_sdk_libdir() {
    local pc
    pc=$(moq5_sdk_pkgconfig "$1") || return 1
    (cd "$(dirname "${pc%%:*}")" && pwd)
}

moq5_sdk_runtime_path() {
    local pc package
    pc=$(moq5_sdk_pkgconfig "$1") || return 1
    package=${2:-$(moq5_sdk_top_package "$1")} || return 1
    {
        PKG_CONFIG_ALLOW_SYSTEM_LIBS=1 PKG_CONFIG_PATH="$pc" "${PKG_CONFIG:-pkg-config}" \
            --libs-only-L "$package" || return 1
        PKG_CONFIG_ALLOW_SYSTEM_LIBS=1 PKG_CONFIG_PATH="$pc" "${PKG_CONFIG:-pkg-config}" \
            --static --libs-only-L "$package" || return 1
    } |
        tr ' ' '\n' | sed -n 's/^-L//p' |
        while read -r dir; do
            # A directory that does not exist holds nothing to load or link.
            if [ -n "$dir" ] && [ -d "$dir" ]; then (cd "$dir" && pwd); fi
        done | awk '!seen[$0]++' | paste -sd: -
}

moq5_sdk_export_loader() {
    local path
    path=$(moq5_sdk_runtime_path "$1") || return 1
    export DYLD_LIBRARY_PATH="$path${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
    export LD_LIBRARY_PATH="$path${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
}

# A shared library (either platform suffix) links shared, as the linker
# itself would prefer it; only a static archive links static. The service
# library decides, or for an SDK without the service the core library.
moq5_sdk_link_mode() {
    local libdir lib
    libdir=$(moq5_sdk_libdir "$1") || return 1
    for lib in libmoq-service libmoq-core; do
        if [ -e "$libdir/$lib.dylib" ] || [ -e "$libdir/$lib.so" ]; then
            echo shared; return 0
        elif [ -e "$libdir/$lib.a" ]; then
            echo static; return 0
        fi
    done
    echo "sdk_paths.sh: no libmoq-service or libmoq-core library under $libdir" >&2
    return 1
}

moq5_sdk_export_link_mode() {
    local mode stem
    mode=$(moq5_sdk_link_mode "$1") || return 1
    for stem in $MOQ5_SDK_TIER_STEMS; do
        case $mode in
            shared) export "${stem}_DYNAMIC=1"; unset "${stem}_STATIC" ;;
            static) export "${stem}_STATIC=1"; unset "${stem}_DYNAMIC" ;;
        esac
    done
    MOQ5_SDK_LINK_MODE=$mode
}

moq5_sdk_version() {
    local pc
    pc=$(moq5_sdk_pkgconfig "$1") || return 1
    PKG_CONFIG_PATH="$pc" "${PKG_CONFIG:-pkg-config}" --modversion libmoq-core
}

moq5_sdk_copy_pc() {
    local prefix=$1 dir=$2 pc package resolved
    shift 2
    pc=$(moq5_sdk_pkgconfig "$prefix") || return 1
    mkdir -p "$dir"
    for package in "$@"; do
        resolved=$(PKG_CONFIG_PATH="$pc" "${PKG_CONFIG:-pkg-config}" --variable=prefix "$package") || return 1
        sed "s#^prefix=.*#prefix=$resolved#" "${pc%%:*}/$package.pc" > "$dir/$package.pc" || return 1
    done
}

moq5_sdk_owned_view() {
    local prefix=$1 dir=$2 pc file name left skip packages=()
    shift 2
    pc=$(moq5_sdk_pkgconfig "$prefix") || return 1
    for file in "${pc%%:*}"/libmoq.pc "${pc%%:*}"/libmoq-*.pc; do
        [ -f "$file" ] || continue
        name=${file##*/}; name=${name%.pc}
        skip=no
        for left in "$@"; do
            if [ "$left" = "$name" ]; then skip=yes; fi
        done
        if [ "$skip" = no ]; then packages+=("$name"); fi
    done
    moq5_sdk_copy_pc "$prefix" "$dir" "${packages[@]}"
}

moq5_sdk_assert_link_search() {
    local log=$1 prefix=$2 libdir closure dir resolved found=0 any=0
    libdir=$(moq5_sdk_libdir "$prefix") || return 1
    closure=$(moq5_sdk_runtime_path "$prefix") || return 1
    while read -r dir; do
        [ -n "$dir" ] || continue
        any=1
        if ! resolved=$(cd "$dir" 2>/dev/null && pwd); then
            echo "sdk_paths.sh: link-search directory does not exist: $dir" >&2
            return 1
        fi
        case ":$closure:" in
            *":$resolved:"*) ;;
            *) echo "sdk_paths.sh: link-search directory outside the SDK closure: $dir" >&2; return 1 ;;
        esac
        [ "$resolved" = "$libdir" ] && found=1
    done < <(grep -o 'rustc-link-search=native=[^ "]*' "$log" | sed 's/^rustc-link-search=native=//' | sort -u)
    if [ "$any" = 0 ]; then
        echo "sdk_paths.sh: the log carries no rustc-link-search line (not a -vv build log?)" >&2
        return 1
    fi
    if [ "$found" = 0 ]; then
        echo "sdk_paths.sh: the build did not link from the SDK's library directory $libdir" >&2
        return 1
    fi
}
