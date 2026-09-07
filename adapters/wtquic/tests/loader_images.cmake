# Which shared objects a process actually loaded, and whether that set is the
# expected one.
#
# The mechanism is the host loader's own tracing, so it differs per platform;
# a Darwin parser applied to Linux output would silently see nothing and turn
# a quiet success into a false verdict. An unsupported host FAILS here rather
# than skipping.
#
# The decisions are kept in functions so a selftest can drive them with
# captured fixture text instead of a real process.

# The loader-tracing environment for this host, or "" when unsupported.
function(loader_trace_env out)
    if(APPLE)
        set(${out} "DYLD_PRINT_LIBRARIES=1" PARENT_SCOPE)
    elseif(CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux")
        set(${out} "LD_DEBUG=libs" PARENT_SCOPE)
    else()
        set(${out} "" PARENT_SCOPE)
    endif()
endfunction()

# Whether the selected artifact is a runtime-loaded library on the supported
# host. The filename comes from an imported target, not from a directory scan;
# this distinguishes a selected libfoo.a from libfoo.so/libfoo.dylib when a
# prefix happens to provide both.
function(loader_artifact_is_shared path out)
    get_filename_component(_name "${path}" NAME)
    set(_shared 0)
    if(_name MATCHES "\\.dylib$" OR _name MATCHES "\\.so(\\.[0-9]+)*$")
        set(_shared 1)
    endif()
    set(${out} "${_shared}" PARENT_SCOPE)
endfunction()

# Parse one loader trace into a list of absolute image paths.
#   loader_parse_images(<text> <platform: Darwin|Linux> <out>)
# Darwin: `dyld[pid]: <UUID> /abs/path`
# Linux : `NNNN: calling init: /abs/path`
function(loader_parse_images text platform out)
    set(_imgs "")
    string(REPLACE "\r" "" text "${text}")
    # a semicolon in the trace would otherwise become a list separator
    string(REPLACE ";" "__SEMI__" text "${text}")
    string(REPLACE "\n" ";" _lines "${text}")
    foreach(_l IN LISTS _lines)
        set(_p "")
        if(platform STREQUAL "Darwin")
            if(_l MATCHES "^dyld\\[[0-9]+\\]:[ \t]+<[^>]*>[ \t]+(/.*)$")
                set(_p "${CMAKE_MATCH_1}")
            endif()
        elseif(platform STREQUAL "Linux")
            if(_l MATCHES "calling init:[ \t]+(/.*)$")
                set(_p "${CMAKE_MATCH_1}")
            endif()
        else()
            message(FATAL_ERROR "loader_parse_images: unsupported platform "
                                "'${platform}'")
        endif()
        if(NOT _p STREQUAL "")
            string(STRIP "${_p}" _p)
            list(APPEND _imgs "${_p}")
        endif()
    endforeach()
    set(${out} "${_imgs}" PARENT_SCOPE)
endfunction()

# Require that a loaded-image list contains EXACTLY the expected files, by
# canonical path, once each; every other image must be outside the set of
# paths the caller says it owns.
#
#   loader_require_images(<images> <expected paths> <owned dirs> <label> <out>)
#
# `out` is "" on success or the reason it failed. A path is compared as a
# path, never as a regular expression.
# Is this image one of the dependency families this gate is responsible for?
# Unrelated system and runtime objects are none of its business.
function(loader_is_relevant path out)
    get_filename_component(_n "${path}" NAME)
    if(_n MATCHES "^libmoq-" OR _n MATCHES "^libwtquic" OR
       _n MATCHES "^libmsquic")
        set(${out} 1 PARENT_SCOPE)
    else()
        set(${out} 0 PARENT_SCOPE)
    endif()
endfunction()

# Does `path` lie inside `dir`, respecting path-component boundaries so that
# `/p/lib-other` is NOT inside `/p/lib`?
function(loader_path_within path dir out)
    get_filename_component(_p "${path}" REALPATH)
    get_filename_component(_d "${dir}" REALPATH)
    string(REGEX REPLACE "/+$" "" _d "${_d}")
    string(LENGTH "${_d}/" _n)
    string(LENGTH "${_p}" _pn)
    set(_v 0)
    if(_pn GREATER _n OR _pn EQUAL _n)
        string(SUBSTRING "${_p}" 0 ${_n} _head)
        if(_head STREQUAL "${_d}/")
            set(_v 1)
        endif()
    endif()
    set(${out} "${_v}" PARENT_SCOPE)
endfunction()

function(loader_require_images images expected owned label out)
    set(_why "")
    foreach(_want IN LISTS expected)
        get_filename_component(_want "${_want}" REALPATH)
        set(_seen 0)
        foreach(_img IN LISTS images)
            get_filename_component(_c "${_img}" REALPATH)
            if(_c STREQUAL "${_want}")
                math(EXPR _seen "${_seen} + 1")
            endif()
        endforeach()
        if(_seen EQUAL 0)
            set(_why "${label}: required image was never loaded: ${_want}")
            break()
        elseif(_seen GREATER 1)
            set(_why "${label}: image loaded ${_seen} times: ${_want}")
            break()
        endif()
    endforeach()
    if(_why STREQUAL "")
        # Any additional image of a dependency family this gate owns is
        # refused WHEREVER it came from -- a second copy from an unrelated
        # directory is exactly the mixed-dependency case to catch. Images from
        # a directory we own are refused too, even if some future family name
        # is not in the relevant list.
        foreach(_img IN LISTS images)
            get_filename_component(_c "${_img}" REALPATH)
            set(_ok 0)
            foreach(_want IN LISTS expected)
                get_filename_component(_w "${_want}" REALPATH)
                if(_c STREQUAL "${_w}")
                    set(_ok 1)
                endif()
            endforeach()
            if(NOT _ok)
                loader_is_relevant("${_c}" _rel)
                set(_in_owned 0)
                foreach(_dir IN LISTS owned)
                    loader_path_within("${_c}" "${_dir}" _w1)
                    if(_w1)
                        set(_in_owned 1)
                    endif()
                endforeach()
                if(_rel)
                    set(_why
                        "${label}: an additional dependency image loaded: ${_c}")
                elseif(_in_owned)
                    set(_why
                        "${label}: an unexpected image loaded from an owned directory: ${_c}")
                endif()
            endif()
            if(NOT _why STREQUAL "")
                break()
            endif()
        endforeach()
    endif()
    set(${out} "${_why}" PARENT_SCOPE)
endfunction()

# A loader trace must carry ONLY image records: an unexpected warning or note
# in the same stream is a result nobody asked for.
function(loader_only_image_records text platform out)
    set(_why "")
    string(REPLACE "\r" "" text "${text}")
    string(REPLACE ";" "__SEMI__" text "${text}")
    string(REPLACE "\n" ";" _lines "${text}")
    foreach(_l IN LISTS _lines)
        string(STRIP "${_l}" _s)
        if(_s STREQUAL "")
            continue()
        endif()
        set(_ok 0)
        # The RECORD KIND decides, and only then is the remainder treated as
        # opaque data: a perfectly valid image whose PATH contains the word
        # "error" must be accepted, while a diagnostic record must not.
        set(_kind "")
        if(platform STREQUAL "Darwin")
            if(_s MATCHES "^dyld\\[[0-9]+\\]:[ \t]+(.*)$")
                set(_rest "${CMAKE_MATCH_1}")
                if(_rest MATCHES "^<[^>]*>[ \t]+/")
                    set(_kind "image")
                elseif(_rest MATCHES "^move loaded to delayed:")
                    set(_kind "delayed")
                endif()
            endif()
        elseif(platform STREQUAL "Linux")
            if(_s MATCHES "^[ \t]*[0-9]+:[ \t]*(.*)$")
                set(_rest "${CMAKE_MATCH_1}")
                if(_rest MATCHES "^calling init:[ \t]+/")
                    set(_kind "image")
                elseif(_rest STREQUAL "")
                    set(_kind "trace")
                else()
                    foreach(_k "find library=" "trying file=" "binding file"
                               "calling fini:" "initialize program:"
                               "transferring control:" "search path="
                               "search cache=" "generating link map" "symbol=")
                        string(LENGTH "${_k}" _kn)
                        string(LENGTH "${_rest}" _rn)
                        if(_rn GREATER_EQUAL _kn)
                            string(SUBSTRING "${_rest}" 0 ${_kn} _h)
                            if(_h STREQUAL "${_k}")
                                set(_kind "trace")
                            endif()
                        endif()
                    endforeach()
                endif()
            endif()
        endif()
        if(NOT _kind STREQUAL "")
            set(_ok 1)
        endif()
        if(NOT _ok)
            string(REPLACE "__SEMI__" ";" _s "${_s}")
            set(_why "unexpected non-image output in the loader trace: ${_s}")
            break()
        endif()
    endforeach()
    set(${out} "${_why}" PARENT_SCOPE)
endfunction()

# Is this the specific PRE-MAIN missing-library failure, as opposed to an
# ordinary application/TLS failure, a signal death, or a missing executable?
#   loader_is_missing_library(<rc> <output> <platform> <out>)
function(loader_is_missing_library rc text platform out)
    set(_v 0)
    if(NOT rc EQUAL 0)
        if(platform STREQUAL "Darwin")
            if(text MATCHES "Library not loaded" AND
               text MATCHES "Reason: (no LC_RPATH's found|tried:)")
                set(_v 1)
            endif()
        elseif(platform STREQUAL "Linux")
            if(text MATCHES "error while loading shared libraries" AND
               text MATCHES "cannot open shared object file")
                set(_v 1)
            endif()
        endif()
    endif()
    # a missing or non-executable program is not this failure
    if(text MATCHES "No such file or directory: " OR
       text MATCHES "command not found")
        set(_v 0)
    endif()
    set(${out} "${_v}" PARENT_SCOPE)
endfunction()
