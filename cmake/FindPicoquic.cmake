# FindPicoquic.cmake
#
# Provides picoquic::picoquic-core for moq-adapter-picoquic, and -- when the
# WebTransport adapter is built (MOQ_BUILD_ADAPTER_PICO_WT; the managed helper
# MOQ_BUILD_PICO_WT_MANAGED requires it) -- picoquic::picohttp-core for HTTP/3.
#
# HTTP/WebTransport is an EXPLICIT requirement of this module: when WT is built,
# it guarantees picoquic::picohttp-core or fails here with a precise message. It
# must never report success with only the core target and let the pico_wt
# adapter discover the missing HTTP support later (the WT dependency contract
# lives here, not as a late "target not found" in adapters/pico_wt).
#
# Resolution order:
# 1. Already-defined target (parent add_subdirectory or prior find)
# 2. Installed config package (picoquic-config.cmake in prefix)
# 3. Source-tree mode (local dev):
#      -DMOQ_PICOQUIC_SOURCE_DIR=/path/to/picoquic
#      -DMOQ_PICOTLS_PREFIX=/path/to/picotls/build

# WebTransport needs picoquic's HTTP/3 library (picohttp-core). Managed WT
# requires the base WT adapter (enforced in the top-level CMakeLists), so this
# one flag covers both.
set(_MOQ_PQ_NEED_HTTP ${MOQ_BUILD_ADAPTER_PICO_WT})

# A drain-capable adapter (raw threaded helper or managed WebTransport) compiles
# adapters/common/moq_pq_stream_backlog.h against picoquic's PRIVATE
# picoquic_internal.h from MOQ_PICOQUIC_SOURCE_DIR. The picoquic LIBRARY it links
# MUST be that same source revision: linking an installed picoquic of a different
# revision is a silent ABI mismatch (it reads picoquic_stream_head_t's layout).
# So in that mode the source tree is authoritative -- the installed CONFIG
# package is bypassed entirely (see "Installed config package" below), which
# prevents the "headers from source, library from install" split build.
set(_MOQ_PQ_REQUIRE_SOURCE OFF)
if((MOQ_BUILD_PQ_THREADED OR MOQ_BUILD_PICO_WT_MANAGED) AND MOQ_PICOQUIC_SOURCE_DIR)
    set(_MOQ_PQ_REQUIRE_SOURCE ON)
endif()

# Normalize a non-namespaced picohttp-core to picoquic::picohttp-core. ALIAS
# targets are global, so either spelling resolves to the namespaced name the
# pico_wt adapter and the install export reference.
function(_moq_pq_alias_http)
    if(TARGET picohttp-core AND NOT TARGET picoquic::picohttp-core)
        add_library(picoquic::picohttp-core ALIAS picohttp-core)
    endif()
endfunction()

# Require picoquic::picohttp-core, or fail with an actionable message. We do not
# synthesize an imported picohttp-core: its transitive link interface (h3zero +
# webtransport over picoquic-log and picoquic-core, picotls, OpenSSL) is build-config
# dependent and cannot be reconstructed reliably from an arbitrary prefix, so a
# silently-wrong target would only move the failure to link time.
function(_moq_pq_require_http where)
    _moq_pq_alias_http()
    if(TARGET picoquic::picohttp-core)
        return()
    endif()
    message(FATAL_ERROR
        "libmoq WebTransport support is enabled (MOQ_BUILD_ADAPTER_PICO_WT=ON), "
        "which requires picoquic's HTTP/3 library, but ${where} provides "
        "picoquic without an exported picohttp-core / picoquic::picohttp-core "
        "target.\n"
        "Fix one of:\n"
        "  - reinstall picoquic built with -DBUILD_HTTP=ON and a complete CMake "
        "export (it must install AND export picohttp-core, i.e. h3zero + "
        "webtransport), then point CMAKE_PREFIX_PATH at that prefix; or\n"
        "  - build libmoq against the picoquic source tree with "
        "-DMOQ_PICOQUIC_SOURCE_DIR=/path/to/picoquic (libmoq forces "
        "BUILD_HTTP=ON there).")
endfunction()

# Normalize the picotls headers (PTLS_INCLUDE_DIR) and the picotls library
# directory (MOQ_PTLS_LIB_DIR) once, reliably, in every mode -- picoquic links
# picotls but installs neither its headers, its libraries, nor a .pc, so libmoq
# must locate both itself.
#
#   PTLS_INCLUDE_DIR  -- dir with picotls.h / picotls/openssl.h. Needed by the
#       cert-verifier helper (moq_picoquic_verify.c) and the managed edge tests.
#       In source-tree mode it is selected before picoquic is added; in
#       CONFIG / already-available mode it is resolved here.
#   MOQ_PTLS_LIB_DIR  -- dir with libpicotls-*.a. Needed so the generated .pc
#       files can bake an absolute -L for the picotls libraries. This must NOT
#       be derived from PTLS_OPENSSL_LIBRARY: picoquic's FindPTLS sets that to a
#       bare target name ("picotls-openssl") when picotls builds in-tree -- a
#       non-cache var in picoquic's subdirectory scope, invisible at top-level
#       .pc time -- and to an absolute path only when found prebuilt. Resolve
#       the directory directly from the MOQ_PICOTLS_PREFIX hint instead.
#
# Both are cache vars (globally visible after find_package(Picoquic)). Absent a
# usable MOQ_PICOTLS_PREFIX hint they stay unset and consumers fall back to
# their own resolution / an actionable error.
#
# With FROM_PREFIX (installed CONFIG package or an existing picoquic target)
# and a MOQ_PICOTLS_PREFIX, both follow that prefix under the same derived /
# explicit rule as source-tree mode (see _moq_pq_ptls_select). A picoquic
# target that links a picotls built in this tree follows that picotls instead
# (_moq_pq_ptls_built_by). In source-tree mode they were already selected
# when picoquic was added.
function(_moq_pq_resolve_ptls)
    if("${ARGV0}" STREQUAL "FROM_PREFIX" AND MOQ_PICOTLS_PREFIX)
        _moq_pq_ptls_dirs_from_prefix()
        return()
    endif()
    if(NOT PTLS_INCLUDE_DIR OR NOT EXISTS "${PTLS_INCLUDE_DIR}/picotls.h")
        find_path(PTLS_INCLUDE_DIR
            NAMES picotls.h
            HINTS
                ${MOQ_PICOTLS_PREFIX}/include
                ${MOQ_PICOTLS_PREFIX}
                ${MOQ_PICOTLS_PREFIX}/../include
            DOC "picotls include directory (picotls.h)")
    endif()
    if(NOT MOQ_PTLS_LIB_DIR OR NOT EXISTS "${MOQ_PTLS_LIB_DIR}")
        if(PTLS_OPENSSL_LIBRARY AND EXISTS "${PTLS_OPENSSL_LIBRARY}")
            # picoquic's FindPTLS found a prebuilt picotls: trust its absolute
            # path's directory.
            get_filename_component(_moq_ptls_dir "${PTLS_OPENSSL_LIBRARY}" DIRECTORY)
            set(MOQ_PTLS_LIB_DIR "${_moq_ptls_dir}"
                CACHE PATH "picotls library directory (libpicotls-*.a)" FORCE)
        else()
            find_library(MOQ_PTLS_CORE_LIBRARY
                NAMES picotls-core
                HINTS
                    ${MOQ_PICOTLS_PREFIX}
                    ${MOQ_PICOTLS_PREFIX}/lib
                    ${MOQ_PICOTLS_PREFIX}/../build
                    ${PTLS_PREFIX}/lib
                DOC "picotls core library (libpicotls-core.a)")
            if(MOQ_PTLS_CORE_LIBRARY)
                get_filename_component(_moq_ptls_dir "${MOQ_PTLS_CORE_LIBRARY}" DIRECTORY)
                set(MOQ_PTLS_LIB_DIR "${_moq_ptls_dir}"
                    CACHE PATH "picotls library directory (libpicotls-*.a)" FORCE)
            endif()
        endif()
    endif()
endfunction()

# -- picotls from MOQ_PICOTLS_PREFIX (source-tree mode) -------------------
#
# picoquic's FindPTLS hints its headers at ${PTLS_PREFIX}/include/picotls, one
# level below where picotls/minicrypto.h resolves, then falls back to picotls
# checkouts beside the top-level source and build trees. It also keeps any
# PTLS_* cache entry that is already set. Left to it, a build can pair this
# prefix's libraries with another picotls's headers, and a reconfigure onto a
# different prefix keeps the previous prefix's selections. So the selections
# are made here, from the currently named prefix, before picoquic is added.
#
# Layout of MOQ_PICOTLS_PREFIX, checked in this order:
#   installed prefix   <prefix>/include/picotls/minicrypto.h, libraries in
#                      <prefix>/lib or <prefix>/lib64
#   build directory    libpicotls-core directly in <prefix> and headers in
#                      <prefix>/../include (a picotls checkout's build dir)
# Anything else is refused.
#
# Each selected cache entry (PTLS_INCLUDE_DIR, PTLS_{CORE,MINICRYPTO,OPENSSL,
# FUSION}_LIBRARY, MOQ_PTLS_LIB_DIR) is either derived or explicit:
#   derived   chosen by this module (recorded in _MOQ_PQ_PTLS_SELECTED_<var>)
#             or by a find module's default search; it is re-selected from
#             the current prefix on every configure, and dropped when the
#             prefix does not provide it.
#   explicit  anything else the caller set (-D, -C, a changed value). It is
#             kept only if it names what the prefix provides, or supplies
#             something the prefix lacks; an explicit selection that differs
#             from the prefix's own file stops the configure, so two picotls
#             trees are never combined. Once seen, an entry stays explicit
#             (recorded in _MOQ_PQ_PTLS_EXPLICIT_<var>) until the caller
#             removes it (-U<var>): a find_path/find_library that meets an
#             untyped -D entry retypes it and replaces its help string with
#             the find default, which would otherwise read as derived on the
#             next configure.
set(_MOQ_PQ_PTLS_DERIVED_HELP
    "Path to a file."
    "Path to a library."
    "picotls include directory (picotls.h)"
    "picotls include directory (picotls/minicrypto.h)"
    "picotls library directory (libpicotls-*.a)"
    "picotls core library"
    "picotls minicrypto library"
    "picotls openssl library"
    "picotls fusion library")

function(_moq_pq_ptls_find_lib name dirs out_var)
    unset(_MOQ_PQ_PTLS_PROBE)
    unset(_MOQ_PQ_PTLS_PROBE CACHE)
    find_library(_MOQ_PQ_PTLS_PROBE NAMES picotls-${name} PATHS ${dirs} NO_DEFAULT_PATH)
    set(found "${_MOQ_PQ_PTLS_PROBE}")
    unset(_MOQ_PQ_PTLS_PROBE CACHE)
    if(found MATCHES "-NOTFOUND$")
        set(found "")
    endif()
    set(${out_var} "${found}" PARENT_SCOPE)
endfunction()

# Sets <out_var> TRUE when cache entry `var` is an explicit caller selection
# (see above), FALSE when it is derived or absent. The value judged is the
# variable's visible value, or [VALUE <value>] (its cache entry's, say).
function(_moq_pq_ptls_classify var out_var)
    cmake_parse_arguments(_c "" "VALUE" "" ${ARGN})
    set(record _MOQ_PQ_PTLS_SELECTED_${var})
    set(explicit_record _MOQ_PQ_PTLS_EXPLICIT_${var})
    set(current "${${var}}")
    if(DEFINED _c_VALUE)
        set(current "${_c_VALUE}")
    endif()
    set(explicit FALSE)
    if(current STREQUAL "" OR current MATCHES "-NOTFOUND$")
        unset(${explicit_record} CACHE)
    elseif(DEFINED CACHE{${explicit_record}})
        set(explicit TRUE)
    else()
        if(DEFINED CACHE{${record}})
            if(NOT current STREQUAL "$CACHE{${record}}")
                set(explicit TRUE)
            endif()
        else()
            get_property(help CACHE ${var} PROPERTY HELPSTRING)
            if(NOT help IN_LIST _MOQ_PQ_PTLS_DERIVED_HELP)
                set(explicit TRUE)
            endif()
        endif()
    endif()
    set(${out_var} ${explicit} PARENT_SCOPE)
endfunction()

# Selects cache entry `var` (of `type`) from `candidate`, the selected
# picotls's own file or "" when it lacks one. `what` names it in diagnostics;
# _moq_pq_ptls_authority, when the caller sets it, names the selected picotls.
function(_moq_pq_ptls_select var type candidate required what doc)
    set(record _MOQ_PQ_PTLS_SELECTED_${var})
    set(explicit_record _MOQ_PQ_PTLS_EXPLICIT_${var})
    set(current "${${var}}")
    if(NOT DEFINED _moq_pq_ptls_authority)
        set(_moq_pq_ptls_authority "MOQ_PICOTLS_PREFIX=${MOQ_PICOTLS_PREFIX}")
    endif()
    _moq_pq_ptls_classify(${var} explicit)

    if(explicit)
        if(candidate)
            get_filename_component(chosen "${current}" REALPATH)
            get_filename_component(provided "${candidate}" REALPATH)
            if(NOT chosen STREQUAL provided)
                message(FATAL_ERROR
                    "${var}=${current} conflicts with "
                    "${_moq_pq_ptls_authority}, whose ${what} is "
                    "${candidate}. Name one picotls: drop the override "
                    "(-U${var}) or select the picotls it belongs to.")
            endif()
        elseif(NOT EXISTS "${current}")
            message(FATAL_ERROR "${var}=${current} does not exist")
        endif()
        unset(${record} CACHE)
        set(${explicit_record} "${current}" CACHE INTERNAL "")
        return()
    endif()

    unset(${explicit_record} CACHE)
    if(candidate)
        set(${var} "${candidate}" CACHE ${type} "${doc}" FORCE)
        set(${record} "${candidate}" CACHE INTERNAL "")
    else()
        unset(${var} CACHE)
        unset(${record} CACHE)
        if(required)
            message(FATAL_ERROR
                "${_moq_pq_ptls_authority} provides no ${what}; "
                "pass -D${var}=<path> to name it explicitly.")
        endif()
    endif()
endfunction()

# Reads the layout of MOQ_PICOTLS_PREFIX into _moq_ptls_tree,
# _moq_ptls_include_dir, _moq_ptls_lib_dirs and _moq_ptls_build_form, or
# refuses a prefix that is neither layout.
function(_moq_pq_ptls_layout)
    set(prefix "${MOQ_PICOTLS_PREFIX}")
    get_filename_component(parent "${prefix}/.." ABSOLUTE)
    _moq_pq_ptls_find_lib(core "${prefix}" core_in_prefix)
    if(EXISTS "${prefix}/include/picotls/minicrypto.h")
        set(build_form FALSE)
        set(tree "${prefix}")
        set(include_dir "${prefix}/include")
        set(lib_dirs "${prefix}/lib" "${prefix}/lib64")
    elseif(core_in_prefix AND EXISTS "${parent}/include/picotls/minicrypto.h")
        set(build_form TRUE)
        set(tree "${parent}")
        set(include_dir "${parent}/include")
        set(lib_dirs "${prefix}")
    else()
        message(FATAL_ERROR
            "MOQ_PICOTLS_PREFIX=${prefix} is not a picotls tree: expected an "
            "installed prefix (include/picotls/minicrypto.h with lib/ or "
            "lib64/) or a picotls build directory (libpicotls-core in it, "
            "headers in ../include).")
    endif()
    set(_moq_ptls_tree "${tree}" PARENT_SCOPE)
    set(_moq_ptls_include_dir "${include_dir}" PARENT_SCOPE)
    set(_moq_ptls_lib_dirs "${lib_dirs}" PARENT_SCOPE)
    set(_moq_ptls_build_form ${build_form} PARENT_SCOPE)
endfunction()

# Selects the two entries libmoq itself reads, PTLS_INCLUDE_DIR (verifier
# helper, managed edge tests) and MOQ_PTLS_LIB_DIR (generated .pc files), from
# MOQ_PICOTLS_PREFIX. Used where picoquic is already built (installed CONFIG
# package or an existing target), so the PTLS_*_LIBRARY entries belong to
# that picoquic and are left alone.
function(_moq_pq_ptls_dirs_from_prefix)
    _moq_pq_ptls_layout()
    _moq_pq_ptls_select(PTLS_INCLUDE_DIR PATH "${_moq_ptls_include_dir}" TRUE
        "headers (picotls/minicrypto.h)"
        "picotls include directory (picotls/minicrypto.h)")
    _moq_pq_ptls_find_lib(core "${_moq_ptls_lib_dirs}" core)
    set(lib_dir "")
    if(core)
        get_filename_component(lib_dir "${core}" DIRECTORY)
    endif()
    _moq_pq_ptls_select(MOQ_PTLS_LIB_DIR PATH "${lib_dir}" TRUE
        "library directory (libpicotls-core)"
        "picotls library directory (libpicotls-*.a)")
endfunction()

# Sets PTLS_PREFIX and every selection above from MOQ_PICOTLS_PREFIX, for a
# picoquic about to be built from source. Returns _moq_ptls_build_form (TRUE
# for the build-directory layout).
function(_moq_pq_ptls_from_prefix)
    _moq_pq_ptls_layout()
    set(build_form ${_moq_ptls_build_form})
    set(include_dir "${_moq_ptls_include_dir}")
    set(lib_dirs "${_moq_ptls_lib_dirs}")
    set(PTLS_PREFIX "${_moq_ptls_tree}" CACHE PATH "" FORCE)

    _moq_pq_ptls_select(PTLS_INCLUDE_DIR PATH "${include_dir}" TRUE
        "headers (picotls/minicrypto.h)"
        "picotls include directory (picotls/minicrypto.h)")

    if(NOT DEFINED WITH_OPENSSL OR WITH_OPENSSL)
        set(openssl_required TRUE)
    else()
        set(openssl_required FALSE)
    endif()
    foreach(lib core minicrypto openssl fusion)
        string(TOUPPER "PTLS_${lib}_LIBRARY" var)
        _moq_pq_ptls_find_lib(${lib} "${lib_dirs}" candidate)
        set(required FALSE)
        if(lib STREQUAL "core" OR lib STREQUAL "minicrypto" OR
           (lib STREQUAL "openssl" AND openssl_required))
            set(required TRUE)
        endif()
        _moq_pq_ptls_select(${var} FILEPATH "${candidate}" ${required}
            "picotls-${lib} library" "picotls ${lib} library")
    endforeach()

    get_filename_component(lib_dir "${PTLS_CORE_LIBRARY}" DIRECTORY)
    _moq_pq_ptls_select(MOQ_PTLS_LIB_DIR PATH "${lib_dir}" TRUE
        "library directory" "picotls library directory (libpicotls-*.a)")

    set(_moq_ptls_build_form ${build_form} PARENT_SCOPE)
endfunction()

# -- picotls built in this tree (PICOQUIC_FETCH_PTLS) ----------------------
#
# With PICOQUIC_FETCH_PTLS picoquic builds picotls itself through
# FetchContent. That picotls is identified by the build, not by global
# FetchContent state: _moq_pq_ptls_built_by reports the source of the
# picotls-core target a given picoquic target actually links (directly or
# through an alias), and only such a target selects it. Its selections follow
# it under the derived/explicit rule above:
#   PTLS_INCLUDE_DIR   <picotls source>/include. When FindPicoquic adds
#                      picoquic, MoqPicotlsProjectPolicy.cmake selects it
#                      inside picotls's project(), before picoquic's FindPTLS
#                      needs it; it is checked again against the targets.
#   MOQ_PTLS_LIB_DIR   the one directory the picotls archives the
#                      pkg-config closure names (core, minicrypto, and
#                      openssl when built) are written to, for the
#                      configuration being built. Where that directory is not
#                      known at configure time -- multi-config generators,
#                      generator expressions, targets written to different
#                      directories, unsupported artifact types -- nothing is
#                      selected, MOQ_PTLS_LIB_DIR_UNRESOLVED records why, and
#                      an explicit MOQ_PTLS_LIB_DIR is refused because it
#                      cannot be checked. The generated pkg-config closure
#                      then names the picotls libraries without a search
#                      directory for them: not a complete export.
#   PTLS_*_LIBRARY     no file selection: picoquic links the picotls
#                      targets. Derived cache entries are dropped; an explicit
#                      one naming anything else is refused. The values the
#                      fetched FindPTLS sets (the target names), cached or
#                      inherited from an enclosing directory, are accepted.
# A MOQ_PICOTLS_PREFIX that names another picotls tree is refused.

# Sets <out_var> to the real (non-alias) name of target <name>, or "" when
# <name> is not a target.
function(_moq_pq_real_target name out_var)
    set(${out_var} "" PARENT_SCOPE)
    if(NOT TARGET "${name}")
        return()
    endif()
    get_target_property(aliased "${name}" ALIASED_TARGET)
    if(aliased)
        set(${out_var} "${aliased}" PARENT_SCOPE)
    else()
        set(${out_var} "${name}" PARENT_SCOPE)
    endif()
endfunction()

# Sets <out_var> to the source directory of the in-tree picotls-core that
# <target> links, or "" when it links none (an imported or prebuilt-linked
# picoquic, or a picoquic target that does not exist).
function(_moq_pq_ptls_built_by target out_var)
    set(${out_var} "" PARENT_SCOPE)
    _moq_pq_real_target("${target}" target)
    if(NOT target OR NOT TARGET picotls-core)
        return()
    endif()
    get_target_property(imported ${target} IMPORTED)
    get_target_property(ptls_imported picotls-core IMPORTED)
    if(imported OR ptls_imported)
        return()
    endif()
    set(items "")
    foreach(prop LINK_LIBRARIES INTERFACE_LINK_LIBRARIES)
        get_target_property(value ${target} ${prop})
        if(value)
            list(APPEND items ${value})
        endif()
    endforeach()
    foreach(item IN LISTS items)
        string(REGEX REPLACE "^\\$<LINK_ONLY:(.*)>$" "\\1" item "${item}")
        _moq_pq_real_target("${item}" real)
        if(real STREQUAL "picotls-core")
            get_target_property(source picotls-core SOURCE_DIR)
            set(${out_var} "${source}" PARENT_SCOPE)
            return()
        endif()
    endforeach()
endfunction()

# Validates a picotls source tree and that a MOQ_PICOTLS_PREFIX names it too.
function(_moq_pq_ptls_check_source source)
    if(NOT EXISTS "${source}/include/picotls/minicrypto.h")
        message(FATAL_ERROR
            "the picotls picoquic builds (PICOQUIC_FETCH_PTLS) has no "
            "include/picotls/minicrypto.h: ${source}")
    endif()
    get_filename_component(real "${source}" REALPATH)
    if(MOQ_PICOTLS_PREFIX)
        get_filename_component(prefix "${MOQ_PICOTLS_PREFIX}" REALPATH)
        get_filename_component(prefix_parent "${MOQ_PICOTLS_PREFIX}/.." REALPATH)
        if(NOT prefix STREQUAL real AND NOT prefix_parent STREQUAL real)
            message(FATAL_ERROR
                "MOQ_PICOTLS_PREFIX=${MOQ_PICOTLS_PREFIX} names a different picotls "
                "than the one picoquic builds with PICOQUIC_FETCH_PTLS (${real}). "
                "Point both at the same picotls, or drop MOQ_PICOTLS_PREFIX.")
        endif()
    endif()
endfunction()

# Selects PTLS_INCLUDE_DIR from the picotls source tree <source>.
function(_moq_pq_ptls_headers_from_source source)
    _moq_pq_ptls_check_source("${source}")
    get_filename_component(real "${source}" REALPATH)
    set(_moq_pq_ptls_authority
        "the picotls picoquic builds (PICOQUIC_FETCH_PTLS, ${real})")
    _moq_pq_ptls_select(PTLS_INCLUDE_DIR PATH "${real}/include" TRUE
        "headers (picotls/minicrypto.h)"
        "picotls include directory (picotls/minicrypto.h)")
endfunction()

# Sets <out_var> TRUE when <value> names target <target> (itself or an alias).
function(_moq_pq_names_target value target out_var)
    set(${out_var} FALSE PARENT_SCOPE)
    if(value STREQUAL target)
        set(${out_var} TRUE PARENT_SCOPE)
        return()
    endif()
    _moq_pq_real_target("${value}" real)
    if(real STREQUAL target)
        set(${out_var} TRUE PARENT_SCOPE)
    endif()
endfunction()

# PTLS_<lib>_LIBRARY when picoquic links picotls target <target>: the cache
# entry is judged first (an inherited normal variable must not hide it), then
# any normal variable visible from an enclosing directory.
function(_moq_pq_ptls_fetched_library var target)
    if(DEFINED CACHE{${var}})
        set(cached "$CACHE{${var}}")
        _moq_pq_names_target("${cached}" ${target} matches)
        if(NOT matches)
            set(explicit FALSE)
            if(NOT cached STREQUAL "" AND NOT cached MATCHES "-NOTFOUND$")
                _moq_pq_ptls_classify(${var} explicit VALUE "${cached}")
            endif()
            if(explicit)
                message(FATAL_ERROR
                    "${var}=${cached} is set, but picoquic links the picotls "
                    "targets it builds (${_moq_pq_ptls_authority}); drop it "
                    "(-U${var}).")
            endif()
            unset(${var} CACHE)
            unset(_MOQ_PQ_PTLS_SELECTED_${var} CACHE)
            unset(_MOQ_PQ_PTLS_EXPLICIT_${var} CACHE)
        endif()
    endif()
    if(DEFINED ${var})
        set(value "${${var}}")
        if(NOT value STREQUAL "" AND NOT value MATCHES "-NOTFOUND$")
            _moq_pq_names_target("${value}" ${target} matches)
            if(NOT matches)
                message(FATAL_ERROR
                    "${var}=${value}, visible from an enclosing directory, names "
                    "something other than the picotls target picoquic links "
                    "(${target}, ${_moq_pq_ptls_authority}); unset it where it "
                    "is set.")
            endif()
        endif()
    endif()
endfunction()

# Sets <out_dir> to the directory target <target>'s linkable artifact is
# written to for the configuration being built, or leaves it "" and sets
# <out_reason> when that is not known at configure time.
function(_moq_pq_ptls_output_dir target out_dir out_reason)
    set(${out_dir} "" PARENT_SCOPE)
    set(${out_reason} "" PARENT_SCOPE)
    get_property(multi_config GLOBAL PROPERTY GENERATOR_IS_MULTI_CONFIG)
    if(multi_config)
        set(${out_reason} "multi-config generator" PARENT_SCOPE)
        return()
    endif()
    get_target_property(type ${target} TYPE)
    if(type STREQUAL "STATIC_LIBRARY")
        set(kind ARCHIVE)
    elseif(type STREQUAL "SHARED_LIBRARY")
        if(DEFINED CMAKE_IMPORT_LIBRARY_SUFFIX AND NOT CMAKE_IMPORT_LIBRARY_SUFFIX STREQUAL "")
            set(kind ARCHIVE)
        else()
            set(kind LIBRARY)
        endif()
    else()
        set(${out_reason} "${target} is a ${type}" PARENT_SCOPE)
        return()
    endif()
    set(dir "")
    if(CMAKE_BUILD_TYPE)
        string(TOUPPER "${CMAKE_BUILD_TYPE}" config)
        get_target_property(dir ${target} ${kind}_OUTPUT_DIRECTORY_${config})
    endif()
    if(NOT dir)
        get_target_property(dir ${target} ${kind}_OUTPUT_DIRECTORY)
    endif()
    get_target_property(binary_dir ${target} BINARY_DIR)
    if(NOT dir)
        set(dir "${binary_dir}")
    endif()
    if(dir MATCHES "\\$<")
        set(${out_reason} "${target}'s output directory is a generator expression" PARENT_SCOPE)
        return()
    endif()
    get_filename_component(dir "${dir}" ABSOLUTE BASE_DIR "${binary_dir}")
    set(${out_dir} "${dir}" PARENT_SCOPE)
endfunction()

# Every selection from the in-tree picotls at <source> (picotls-core's
# source directory).
function(_moq_pq_ptls_from_built source)
    _moq_pq_ptls_headers_from_source("${source}")
    get_filename_component(real "${source}" REALPATH)
    set(_moq_pq_ptls_authority
        "the picotls picoquic builds (PICOQUIC_FETCH_PTLS, ${real})")
    foreach(lib core minicrypto openssl fusion)
        string(TOUPPER "PTLS_${lib}_LIBRARY" var)
        _moq_pq_ptls_fetched_library(${var} picotls-${lib})
    endforeach()
    unset(PTLS_PREFIX CACHE)

    set(dirs "")
    set(reason "")
    foreach(target picotls-core picotls-minicrypto picotls-openssl)
        if(NOT TARGET ${target})
            if(target STREQUAL "picotls-openssl")
                continue()
            endif()
            set(reason "no ${target} target")
            break()
        endif()
        _moq_pq_ptls_output_dir(${target} dir reason)
        if(reason)
            break()
        endif()
        list(APPEND dirs "${dir}")
    endforeach()
    list(REMOVE_DUPLICATES dirs)
    list(LENGTH dirs count)
    if(NOT reason AND NOT count EQUAL 1)
        set(reason "the picotls archives are written to different directories (${dirs})")
    endif()
    if(reason)
        _moq_pq_ptls_classify(MOQ_PTLS_LIB_DIR explicit)
        if(explicit)
            message(FATAL_ERROR
                "MOQ_PTLS_LIB_DIR=${MOQ_PTLS_LIB_DIR} cannot be checked against "
                "${_moq_pq_ptls_authority}: ${reason}. Drop it (-UMOQ_PTLS_LIB_DIR).")
        endif()
        unset(MOQ_PTLS_LIB_DIR CACHE)
        unset(_MOQ_PQ_PTLS_SELECTED_MOQ_PTLS_LIB_DIR CACHE)
        set(MOQ_PTLS_LIB_DIR_UNRESOLVED "${reason}" CACHE INTERNAL
            "why no picotls library directory is selected")
    else()
        unset(MOQ_PTLS_LIB_DIR_UNRESOLVED CACHE)
        _moq_pq_ptls_select(MOQ_PTLS_LIB_DIR PATH "${dirs}" FALSE
            "library directory (picotls archives)"
            "picotls library directory (libpicotls-*.a)")
    endif()
endfunction()

# Adds MOQ_PICOQUIC_SOURCE_DIR with LibMoQ's project hooks: picoquic's
# project() runs MoqPicoquicProjectPolicy.cmake (link-line de-duplication
# policies) and, with PICOQUIC_FETCH_PTLS, picotls's project() runs
# MoqPicotlsProjectPolicy.cmake (LibMoQ's picotls policy and header
# selection). Each composes with the caller's CMAKE_PROJECT_<name>_INCLUDE --
# a file, a module name or a list of them -- which it includes first, handed
# over as MOQ_PICO{QUIC,TLS}_CALLER_PROJECT_INCLUDE. The hook variables are set
# only in this function's scope, which the added directories inherit, so the
# caller's bindings (unset, cache-only, normal, normal over cache) are never
# touched.
function(_moq_pq_add_picoquic_source)
    set(MOQ_PICOQUIC_CALLER_PROJECT_INCLUDE "${CMAKE_PROJECT_picoquic_INCLUDE}")
    set(CMAKE_PROJECT_picoquic_INCLUDE
        "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/MoqPicoquicProjectPolicy.cmake")
    if(PICOQUIC_FETCH_PTLS)
        set(MOQ_PICOTLS_CALLER_PROJECT_INCLUDE "${CMAKE_PROJECT_picotls_INCLUDE}")
        set(CMAKE_PROJECT_picotls_INCLUDE
            "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/MoqPicotlsProjectPolicy.cmake")
    endif()
    add_subdirectory("${MOQ_PICOQUIC_SOURCE_DIR}"
        "${CMAKE_BINARY_DIR}/_deps/picoquic" EXCLUDE_FROM_ALL)
endfunction()

# One OpenSSL link identity for source-tree picoquic and the adapter.
#
# picoquic-core links the OpenSSL it finds as the raw artifact paths in
# ${OPENSSL_LIBRARIES}; moq-adapter-picoquic links the OpenSSL::SSL /
# OpenSSL::Crypto imported targets. The two spellings of one artifact are two
# link items, so CMake's link de-duplication (CMP0156/CMP0179, applied per
# linker capability) cannot merge them: a static OpenSSL then reaches a
# consumer's link line twice, which capability-aware linkers report as
# duplicate libraries. The adapter keeps its target dependency (its verifier
# uses OpenSSL directly); picoquic-core is given the same targets instead.
#
# Applies only when the adapter will link OpenSSL: the adapter is built and
# its verifier backend resolves to openssl (the rule in
# adapters/picoquic/CMakeLists.txt: "openssl", or "auto" unless picoquic is
# configured WITH_OPENSSL=OFF). OpenSSL-off, mbedTLS, installed or
# pre-existing picoquic targets are left as they are.
function(_moq_pq_openssl_identity_wanted out_var)
    set(${out_var} FALSE PARENT_SCOPE)
    if(NOT MOQ_BUILD_ADAPTER_PICOQUIC)
        return()
    endif()
    if(DEFINED WITH_OPENSSL AND NOT WITH_OPENSSL)
        return()
    endif()
    string(TOLOWER "${MOQ_PICOQUIC_VERIFY_BACKEND}" _backend)
    if(_backend STREQUAL "openssl" OR _backend STREQUAL "auto")
        set(${out_var} TRUE PARENT_SCOPE)
    endif()
endfunction()

# Find OpenSSL in this (top-level) directory before picoquic is added, so the
# imported targets are visible to the picoquic subtree and to the adapter.
# FindOpenSSL creates OpenSSL::SSL / OpenSSL::Crypto only when they do not
# exist yet, so the later find_package(OpenSSL) calls in both reuse them. The
# selection inputs are the ones picoquic's own call uses (the same cache
# entries and OPENSSL_ROOT_DIR). Called as a function, so the module's
# OPENSSL_* result variables stay local; imported targets and cache entries
# are directory- and project-wide by nature.
function(_moq_pq_openssl_targets)
    find_package(OpenSSL REQUIRED)
endfunction()

# In <target>'s LINK_LIBRARIES and INTERFACE_LINK_LIBRARIES, respell each item
# that is exactly the artifact an OpenSSL imported target resolves to (also
# inside $<LINK_ONLY:...>) as that target. Only imported targets with a
# single, configuration-independent IMPORTED_LOCATION are used; every other
# item, its position and its wrapper stay as they are.
function(_moq_pq_openssl_respell target)
    set(_from "")
    set(_to "")
    foreach(_imp IN ITEMS OpenSSL::SSL OpenSSL::Crypto)
        if(NOT TARGET ${_imp})
            continue()
        endif()
        get_target_property(_cfgs ${_imp} IMPORTED_CONFIGURATIONS)
        get_target_property(_loc ${_imp} IMPORTED_LOCATION)
        if(_cfgs OR NOT _loc)
            continue()
        endif()
        list(APPEND _from "${_loc}")
        list(APPEND _to "${_imp}")
    endforeach()
    if(NOT _from)
        return()
    endif()
    foreach(_prop IN ITEMS LINK_LIBRARIES INTERFACE_LINK_LIBRARIES)
        get_target_property(_items ${target} ${_prop})
        if(NOT _items)
            continue()
        endif()
        set(_out "")
        set(_changed FALSE)
        foreach(_item IN LISTS _items)
            set(_new "${_item}")
            list(FIND _from "${_item}" _i)
            if(_i GREATER -1)
                list(GET _to ${_i} _new)
            elseif(_item MATCHES "^\\$<LINK_ONLY:([^<>]+)>$")
                list(FIND _from "${CMAKE_MATCH_1}" _i)
                if(_i GREATER -1)
                    list(GET _to ${_i} _imp)
                    set(_new "$<LINK_ONLY:${_imp}>")
                endif()
            endif()
            if(NOT _new STREQUAL _item)
                set(_changed TRUE)
            endif()
            list(APPEND _out "${_new}")
        endforeach()
        if(_changed)
            set_property(TARGET ${target} PROPERTY ${_prop} "${_out}")
        endif()
    endforeach()
endfunction()

# -- Already available --------------------------------------------------
if(TARGET picoquic::picoquic-core OR TARGET picoquic-core)
    if(TARGET picoquic-core AND NOT TARGET picoquic::picoquic-core)
        add_library(picoquic::picoquic-core ALIAS picoquic-core)
    endif()
    if(TARGET picoquic-log AND NOT TARGET picoquic::picoquic-log)
        add_library(picoquic::picoquic-log ALIAS picoquic-log)
    endif()
    if(_MOQ_PQ_NEED_HTTP)
        # Core may be present while HTTP is not yet loaded (e.g. a prior find of
        # only the core export); give the installed config a chance to populate
        # the full export set before deciding.
        _moq_pq_alias_http()
        if(NOT TARGET picoquic::picohttp-core)
            find_package(picoquic CONFIG QUIET)
        endif()
        _moq_pq_require_http("the already-available picoquic")
    endif()
    _moq_pq_ptls_built_by(picoquic::picoquic-core _moq_pq_ptls_source)
    if(_moq_pq_ptls_source)
        _moq_pq_ptls_from_built("${_moq_pq_ptls_source}")
    else()
        _moq_pq_resolve_ptls(FROM_PREFIX)
    endif()
    unset(_moq_pq_ptls_source)
    set(Picoquic_FOUND TRUE)
    return()
endif()

# -- Installed config package -------------------------------------------
# Bypassed when a drain-capable adapter requires the source tree (above): the
# linked library must match the private headers, so an installed picoquic is not
# eligible -- fall through to source-tree mode regardless of picoquic_DIR.
if(_MOQ_PQ_REQUIRE_SOURCE)
    if(picoquic_DIR)
        message(STATUS
            "libmoq: ignoring installed picoquic (picoquic_DIR=${picoquic_DIR}); "
            "a drain-capable picoquic adapter is enabled, so picoquic is built "
            "from MOQ_PICOQUIC_SOURCE_DIR=${MOQ_PICOQUIC_SOURCE_DIR} to keep "
            "picoquic_internal.h and the linked library at the same revision.")
    endif()
else()
    find_package(picoquic CONFIG QUIET)
    if(TARGET picoquic::picoquic-core)
        if(_MOQ_PQ_NEED_HTTP)
            _moq_pq_require_http("the installed picoquic CONFIG package")
        endif()
        _moq_pq_resolve_ptls(FROM_PREFIX)
        set(Picoquic_FOUND TRUE)
        return()
    endif()
endif()

# -- Source-tree mode ---------------------------------------------------
if(MOQ_PICOQUIC_SOURCE_DIR)
    if(NOT EXISTS "${MOQ_PICOQUIC_SOURCE_DIR}/CMakeLists.txt")
        message(FATAL_ERROR
            "MOQ_PICOQUIC_SOURCE_DIR does not contain CMakeLists.txt:\n"
            "  ${MOQ_PICOQUIC_SOURCE_DIR}")
    endif()

    # With PICOQUIC_FETCH_PTLS picoquic builds picotls itself (see "picotls
    # built in this tree" above). Otherwise MOQ_PICOTLS_PREFIX names a
    # prebuilt picotls.
    if(NOT PICOQUIC_FETCH_PTLS AND NOT MOQ_PICOTLS_PREFIX)
        set(_ptls_build "${MOQ_PICOQUIC_SOURCE_DIR}/../picotls/build")
        if(EXISTS "${_ptls_build}")
            get_filename_component(MOQ_PICOTLS_PREFIX "${_ptls_build}" ABSOLUTE)
        else()
            message(FATAL_ERROR
                "picotls build not found adjacent to picoquic.\n"
                "Pass -DMOQ_PICOTLS_PREFIX=/path/to/picotls/build\n"
                "or build picotls: cd /path/to/picotls && cmake -B build && cmake --build build")
        endif()
    endif()

    # MOQ_PICOTLS_PREFIX decides picotls for the source build; see
    # _moq_pq_ptls_from_prefix. PTLS_PREFIX tells picoquic's FindPTLS the same.
    if(NOT PICOQUIC_FETCH_PTLS)
        _moq_pq_ptls_from_prefix()
        if(_moq_ptls_build_form)
            list(APPEND CMAKE_LIBRARY_PATH "${MOQ_PICOTLS_PREFIX}")
        endif()
        unset(_moq_ptls_build_form)
    endif()

    # Disable picoquic extras.
    set(BUILD_DEMO OFF CACHE BOOL "" FORCE)
    set(BUILD_PQBENCH OFF CACHE BOOL "" FORCE)
    set(BUILD_LOGLIB ON CACHE BOOL "" FORCE)
    set(BUILD_PICO_SIM OFF CACHE BOOL "" FORCE)

    # picohttp/h3zero needed for the picoquic WebTransport adapter.
    # picoquic-test needed for loopback sim harness. Upstream test
    # executables are built but disabled from our CTest suite.
    if(MOQ_PICO_WT_BUILD_LOOPBACK)
        set(picoquic_BUILD_TESTS ON CACHE BOOL "" FORCE)
    else()
        set(picoquic_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    endif()
    if(_MOQ_PQ_NEED_HTTP)
        set(BUILD_HTTP ON CACHE BOOL "" FORCE)
    else()
        set(BUILD_HTTP OFF CACHE BOOL "" FORCE)
    endif()

    # Resolved before picoquic exists as a target, so that pre-existing
    # picoquic targets (the branches above) are never touched.
    _moq_pq_openssl_identity_wanted(_moq_pq_openssl_identity)
    if(_moq_pq_openssl_identity)
        _moq_pq_openssl_targets()
    endif()

    _moq_pq_add_picoquic_source()

    # picoquic_BUILD_TESTS=ON (forced above for the picoquic-test simulation
    # library our loopback/sim harness links) also makes picoquic register its
    # own conformance tests (picoquic_ct, picohttp_ct) in CTest. picoquic was
    # added EXCLUDE_FROM_ALL, so the normal libmoq build does not build their
    # executables -- which would leave the registered tests pointing at missing
    # binaries ("Not Run"). Since they ARE registered in our suite and run
    # quickly (~27s total), build their executables as part of the normal build
    # so they actually run. They are not hidden, ignored, or disabled.
    if(TARGET picoquic_ct AND TARGET picohttp_ct)
        add_custom_target(moq-picoquic-upstream-tests ALL
            DEPENDS picoquic_ct picohttp_ct)
    endif()

    if(TARGET picoquic-core)
        # picoquic-core was created by the add_subdirectory above. Only it is
        # respelled: it is the provider edge the adapter's consumers link.
        # picohttp-core and an in-tree picotls-openssl carry raw OpenSSL
        # edges of their own and are left as they are.
        if(_moq_pq_openssl_identity)
            _moq_pq_openssl_respell(picoquic-core)
        endif()
        # The provider's own edges are used as declared: picoquic-log links
        # picoquic-core, picoquic-test links picoquic-log, and picoquic-core
        # needs nothing from picoquic-log. That holds for the qualified source
        # baseline (picoquic a602e60b), whose core no longer calls the log
        # library. Automatic accommodation of the older core/log cycle (e.g.
        # 55b473e2, whose core calls picoquic_set_qlog) is retired, and there
        # is no configure-time capability check. Consumers of such a source
        # tree MAY fail to link: its static archives still build with the
        # symbol unresolved, and another dependency may or may not supply
        # picoquic-log.
        if(NOT TARGET picoquic::picoquic-core)
            add_library(picoquic::picoquic-core ALIAS picoquic-core)
        endif()
        if(TARGET picoquic-log AND NOT TARGET picoquic::picoquic-log)
            add_library(picoquic::picoquic-log ALIAS picoquic-log)
        endif()
        set(Picoquic_FOUND TRUE)
    elseif(TARGET picoquic::picoquic-core)
        set(Picoquic_FOUND TRUE)
    else()
        message(FATAL_ERROR "picoquic add_subdirectory did not produce picoquic-core target")
    endif()

    # Sanitize include dirs: some picoquic source trees add
    # $<BUILD_INTERFACE:path> for directories that may not exist.
    if(TARGET picoquic-core)
        get_target_property(_pq_inc picoquic-core INTERFACE_INCLUDE_DIRECTORIES)
        if(_pq_inc)
            set(_pq_clean "")
            foreach(_pq_d IN LISTS _pq_inc)
                set(_pq_keep TRUE)
                if(_pq_d MATCHES "^\\$<BUILD_INTERFACE:(.+)>$")
                    if(NOT EXISTS "${CMAKE_MATCH_1}")
                        set(_pq_keep FALSE)
                    endif()
                elseif(NOT _pq_d MATCHES "^\\$<" AND NOT EXISTS "${_pq_d}")
                    set(_pq_keep FALSE)
                endif()
                if(_pq_keep)
                    list(APPEND _pq_clean "${_pq_d}")
                endif()
            endforeach()
            set_target_properties(picoquic-core PROPERTIES
                INTERFACE_INCLUDE_DIRECTORIES "${_pq_clean}")
        endif()
    endif()

    # Source-tree mode forces BUILD_HTTP=ON when WT is needed, so picohttp-core
    # is built here; normalize it and assert the contract (the FATAL message
    # would only fire if upstream picoquic stopped producing the target).
    if(_MOQ_PQ_NEED_HTTP)
        _moq_pq_require_http("the picoquic source tree")
    endif()

    # picoquic add_subdirectory'd above runs picoquic's FindPTLS in a child
    # scope, so PTLS_INCLUDE_DIR / picotls lib paths set there are not reliably
    # visible here (and PTLS_OPENSSL_LIBRARY may be a bare target name). Resolve
    # both directly so .pc generation and the verifier helper see them.
    if(PICOQUIC_FETCH_PTLS)
        _moq_pq_ptls_built_by(picoquic-core _moq_pq_ptls_source)
        if(NOT _moq_pq_ptls_source)
            message(FATAL_ERROR
                "PICOQUIC_FETCH_PTLS is set, but picoquic-core does not link a "
                "picotls-core built in this tree.")
        endif()
        if(FETCHCONTENT_SOURCE_DIR_PICOTLS)
            get_filename_component(_moq_ptls_a "${FETCHCONTENT_SOURCE_DIR_PICOTLS}" REALPATH)
            get_filename_component(_moq_ptls_b "${_moq_pq_ptls_source}" REALPATH)
            if(NOT _moq_ptls_a STREQUAL _moq_ptls_b)
                message(FATAL_ERROR
                    "picoquic built picotls from ${_moq_pq_ptls_source}, not from "
                    "FETCHCONTENT_SOURCE_DIR_PICOTLS=${FETCHCONTENT_SOURCE_DIR_PICOTLS}.")
            endif()
            unset(_moq_ptls_a)
            unset(_moq_ptls_b)
        endif()
        _moq_pq_ptls_from_built("${_moq_pq_ptls_source}")
        unset(_moq_pq_ptls_source)
    else()
        _moq_pq_resolve_ptls()
    endif()

    return()
endif()

# -- Not found ----------------------------------------------------------
message(FATAL_ERROR
    "picoquic not found.\n"
    "Either install picoquic to a prefix in CMAKE_PREFIX_PATH,\n"
    "or pass -DMOQ_PICOQUIC_SOURCE_DIR=/path/to/picoquic\n"
    "(picotls must be adjacent or set -DMOQ_PICOTLS_PREFIX).")
