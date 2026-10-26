# Sourced by the qualification scripts: where an installed SDK keeps its
# pkg-config files and libraries, the loader path its closure needs, the
# link mode it was installed with, and the link-search assertion the
# consumer gates make.
#
#   moq5_sdk_pkgconfig <prefix>       the directory holding libmoq-service.pc
#                                     (plus MOQ5_SDK_PKG_CONFIG_PATH, see below)
#   moq5_sdk_libdir <prefix>          the SDK's own library directory
#   moq5_sdk_runtime_path <prefix>    every -L directory of the service's
#                                     pkg-config closure, both link modes,
#                                     colon-joined
#   moq5_sdk_export_loader <prefix>   prepend that path to DYLD_LIBRARY_PATH
#                                     and LD_LIBRARY_PATH
#   moq5_sdk_link_mode <prefix>       static or shared, by the installed
#                                     service library
#   moq5_sdk_export_link_mode <prefix> state that mode to the pkg-config crate
#                                     (LIBMOQ_SERVICE_STATIC / _DYNAMIC), the
#                                     other variable cleared, and set
#                                     MOQ5_SDK_LINK_MODE; it changes the
#                                     caller's environment, so it is called
#                                     directly, never inside $(...)
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

moq5_sdk_pkgconfig() {
    local d
    for d in lib/pkgconfig lib64/pkgconfig; do
        if [ -f "$1/$d/libmoq-service.pc" ]; then
            echo "$1/$d${MOQ5_SDK_PKG_CONFIG_PATH:+:$MOQ5_SDK_PKG_CONFIG_PATH}"
            return 0
        fi
    done
    echo "sdk_paths.sh: no libmoq-service.pc under $1" >&2
    return 1
}

moq5_sdk_libdir() {
    local pc
    pc=$(moq5_sdk_pkgconfig "$1") || return 1
    (cd "$(dirname "${pc%%:*}")" && pwd)
}

moq5_sdk_runtime_path() {
    local pc
    pc=$(moq5_sdk_pkgconfig "$1") || return 1
    {
        PKG_CONFIG_ALLOW_SYSTEM_LIBS=1 PKG_CONFIG_PATH="$pc" "${PKG_CONFIG:-pkg-config}" \
            --libs-only-L libmoq-service || return 1
        PKG_CONFIG_ALLOW_SYSTEM_LIBS=1 PKG_CONFIG_PATH="$pc" "${PKG_CONFIG:-pkg-config}" \
            --static --libs-only-L libmoq-service || return 1
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

# A shared service library (either platform suffix) links shared, as the
# linker itself would prefer it; only a static archive links static.
moq5_sdk_link_mode() {
    local libdir
    libdir=$(moq5_sdk_libdir "$1") || return 1
    if [ -e "$libdir/libmoq-service.dylib" ] || [ -e "$libdir/libmoq-service.so" ]; then
        echo shared
    elif [ -e "$libdir/libmoq-service.a" ]; then
        echo static
    else
        echo "sdk_paths.sh: no libmoq-service library under $libdir" >&2
        return 1
    fi
}

moq5_sdk_export_link_mode() {
    local mode
    mode=$(moq5_sdk_link_mode "$1") || return 1
    case $mode in
        shared) export LIBMOQ_SERVICE_DYNAMIC=1; unset LIBMOQ_SERVICE_STATIC ;;
        static) export LIBMOQ_SERVICE_STATIC=1; unset LIBMOQ_SERVICE_DYNAMIC ;;
    esac
    MOQ5_SDK_LINK_MODE=$mode
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
