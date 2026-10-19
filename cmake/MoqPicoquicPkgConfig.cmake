# <path> made absolute, normal and, as far as it exists, symlink-free: its
# longest existing ancestor is resolved and the rest appended as written, so
# a not-yet-created install prefix compares like an existing one.
function(_moq_pc_canonical path out_var)
    cmake_path(ABSOLUTE_PATH path NORMALIZE OUTPUT_VARIABLE _p)
    set(_head "${_p}")
    set(_tail "")
    while(NOT EXISTS "${_head}")
        cmake_path(GET _head FILENAME _leaf)
        cmake_path(GET _head PARENT_PATH _parent)
        if(_parent STREQUAL _head)
            break()
        endif()
        if(_tail STREQUAL "")
            set(_tail "${_leaf}")
        else()
            set(_tail "${_leaf}/${_tail}")
        endif()
        set(_head "${_parent}")
    endwhile()
    if(EXISTS "${_head}")
        file(REAL_PATH "${_head}" _head)
    endif()
    if(_tail STREQUAL "")
        set(${out_var} "${_head}" PARENT_SCOPE)
    else()
        cmake_path(APPEND _head "${_tail}" OUTPUT_VARIABLE _joined)
        set(${out_var} "${_joined}" PARENT_SCOPE)
    endif()
endfunction()

# How a .pc names an installed provider directory <dir>: "${prefix}/<rel>"
# when it lies inside CMAKE_INSTALL_PREFIX, its fixed absolute path when it
# lies outside (an external install the .pc cannot relocate); "" when it is
# exactly <prefix>/<same_as> (LibMoQ's own directory of that kind, which the
# .pc already names).
function(_moq_pc_provider_dir dir same_as out_var)
    _moq_pc_canonical("${dir}" _d)
    _moq_pc_canonical("${CMAKE_INSTALL_PREFIX}" _prefix)
    file(RELATIVE_PATH _rel "${_prefix}" "${_d}")
    if(_rel MATCHES "^\\.\\.(/|$)" OR IS_ABSOLUTE "${_rel}")
        set(${out_var} "${_d}" PARENT_SCOPE)
    elseif(_rel STREQUAL "${same_as}")
        set(${out_var} "" PARENT_SCOPE)
    elseif(_rel STREQUAL "")
        set(${out_var} "\${prefix}" PARENT_SCOPE)
    else()
        set(${out_var} "\${prefix}/${_rel}" PARENT_SCOPE)
    endif()
endfunction()

# The real target behind picoquic::<name> when it is an IMPORTED (installed
# package) target; "" when it is absent or built here.
function(_moq_picoquic_imported name out_var)
    set(${out_var} "" PARENT_SCOPE)
    if(NOT TARGET picoquic::${name})
        return()
    endif()
    get_target_property(_real picoquic::${name} ALIASED_TARGET)
    if(NOT _real)
        set(_real picoquic::${name})
    endif()
    get_target_property(_imp ${_real} IMPORTED)
    if(_imp)
        set(${out_var} "${_real}" PARENT_SCOPE)
    endif()
endfunction()

# The library directory an installed picoquic package's picoquic-core
# archive lies in, as _moq_pc_provider_dir names it against LibMoQ's own
# libdir; "" when picoquic is built here, its archive is in LibMoQ's libdir,
# or its location is not known.
function(_moq_picoquic_libdir_flag out_var)
    set(${out_var} "" PARENT_SCOPE)
    _moq_picoquic_imported(picoquic-core _real)
    if(NOT _real)
        return()
    endif()
    get_target_property(_loc ${_real} IMPORTED_LOCATION)
    if(NOT _loc)
        get_target_property(_cfgs ${_real} IMPORTED_CONFIGURATIONS)
        foreach(_cfg IN LISTS _cfgs)
            get_target_property(_loc ${_real} IMPORTED_LOCATION_${_cfg})
            if(_loc)
                break()
            endif()
        endforeach()
    endif()
    if(NOT _loc)
        return()
    endif()
    get_filename_component(_dir "${_loc}" DIRECTORY)
    _moq_pc_provider_dir("${_dir}" "${CMAKE_INSTALL_LIBDIR}" _named)
    if(_named)
        set(${out_var} "-L${_named}" PARENT_SCOPE)
    endif()
endfunction()

# Build the pkg-config link closure for the configured PicoQUIC provider.
# PicoQUIC does not ship pkg-config metadata, so consumers need its archives,
# picotls, and the selected TLS provider spelled out here, each before what it
# depends on. picoquic-core needs nothing from picoquic-log; pass WITH_LOG when
# the caller's own archives do (picohttp-core does), so picoquic-log is listed
# ahead of the picoquic-core it depends on.
function(moq_picoquic_pkgconfig_libs out_var)
    cmake_parse_arguments(_pc "WITH_LOG" "" "" ${ARGN})
    set(_libs "")

    # An installed picoquic package's archives are where it put them:
    # name that directory unless it is LibMoQ's own libdir (built here, the
    # archives are expected there).
    _moq_picoquic_libdir_flag(_pq_libdir_flag)
    if(_pq_libdir_flag)
        string(APPEND _libs " ${_pq_libdir_flag}")
    endif()

    if(_pc_WITH_LOG)
        string(APPEND _libs " -lpicoquic-log")
    endif()
    string(APPEND _libs " -lpicoquic-core")

    if(MOQ_PTLS_LIB_DIR)
        string(APPEND _libs " -L${MOQ_PTLS_LIB_DIR}")
    endif()

    set(_has_openssl OFF)
    if((DEFINED WITH_OPENSSL AND WITH_OPENSSL) OR
       MOQ_ADAPTER_PICOQUIC_NEEDS_OPENSSL)
        set(_has_openssl ON)
    endif()
    if(_has_openssl)
        string(APPEND _libs " -lpicotls-openssl")
    endif()
    string(APPEND _libs " -lpicotls-core -lpicotls-minicrypto")

    if(DEFINED WITH_MBEDTLS AND WITH_MBEDTLS)
        set(_mbed_dirs "")
        foreach(_mbed_var MBEDTLS_LIBRARY MBEDTLS_X509 MBEDTLS_CRYPTO)
            if(DEFINED ${_mbed_var} AND NOT "${${_mbed_var}}" STREQUAL "")
                get_filename_component(_mbed_dir "${${_mbed_var}}" DIRECTORY)
                list(APPEND _mbed_dirs "${_mbed_dir}")
            endif()
        endforeach()
        list(REMOVE_DUPLICATES _mbed_dirs)
        foreach(_mbed_dir IN LISTS _mbed_dirs)
            string(APPEND _libs " -L${_mbed_dir}")
        endforeach()
        string(APPEND _libs " -lmbedtls -lmbedx509 -lmbedcrypto")
    endif()

    if(_has_openssl)
        find_package(OpenSSL QUIET)
        if(OPENSSL_FOUND AND OPENSSL_CRYPTO_LIBRARY)
            get_filename_component(_openssl_dir "${OPENSSL_CRYPTO_LIBRARY}" DIRECTORY)
            string(APPEND _libs " -L${_openssl_dir}")
        endif()
        string(APPEND _libs " -lssl -lcrypto")
    endif()

    string(APPEND _libs " -lpthread")
    string(STRIP "${_libs}" _libs)
    set(${out_var} "${_libs}" PARENT_SCOPE)
endfunction()

# The include roots a consumer needs, beyond LibMoQ's own includedir, for
# the PicoQUIC headers (core, and HTTP/3 and WebTransport) the public
# adapter headers include, as pkg-config Cflags. PicoQUIC's install layout is its own: it
# need not share LibMoQ's CMAKE_INSTALL_INCLUDEDIR.
#   - An installed picoquic package: the include directories its imported
#     targets declare, written under ${prefix} when they lie inside
#     CMAKE_INSTALL_PREFIX (installed together) and as they are otherwise
#     (an external install: a fixed path, which this .pc cannot relocate).
#   - A picoquic source tree built here: nothing of it is installed with
#     LibMoQ, and the link closure above expects it installed into the same
#     prefix (its archives come from ${libdir}). Its headers are then where
#     its own install puts them by default, include/ under ${prefix}.
# A root equal to LibMoQ's own includedir is not repeated.
function(moq_picoquic_pkgconfig_cflags out_var)
    set(_roots "")
    set(_imported OFF)
    foreach(_name picohttp-core picoquic-core)
        _moq_picoquic_imported(${_name} _real)
        if(NOT _real)
            continue()
        endif()
        set(_imported ON)
        get_target_property(_dirs ${_real} INTERFACE_INCLUDE_DIRECTORIES)
        if(_dirs)
            list(APPEND _roots ${_dirs})
        endif()
    endforeach()
    set(_flags "")
    if(_imported)
        set(_named_roots "")
        foreach(_dir IN LISTS _roots)
            _moq_pc_provider_dir("${_dir}" "${CMAKE_INSTALL_INCLUDEDIR}" _named)
            if(_named)
                list(APPEND _named_roots "${_named}")
            endif()
        endforeach()
        list(REMOVE_DUPLICATES _named_roots)
        foreach(_named IN LISTS _named_roots)
            string(APPEND _flags " -I${_named}")
        endforeach()
    elseif(NOT CMAKE_INSTALL_INCLUDEDIR STREQUAL "include")
        string(APPEND _flags " -I\${prefix}/include")
    endif()
    string(STRIP "${_flags}" _flags)
    set(${out_var} "${_flags}" PARENT_SCOPE)
endfunction()
