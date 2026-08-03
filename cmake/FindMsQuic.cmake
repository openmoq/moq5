# FindMsQuic
# ----------
# Locates MsQuic and defines the imported target msquic::msquic.
#
# Search order:
#   1. An installed msquic CMake config package (e.g. Homebrew
#      libmsquic).
#   2. MOQ_MSQUIC_ROOT (cache var), defaulting to ../msquic next to
#      this checkout: header at src/inc/msquic.h, library under
#      build/bin/.
#
# Result variables: MsQuic_FOUND

if(TARGET msquic::msquic)
    set(MsQuic_FOUND TRUE)
    return()
endif()

find_package(msquic CONFIG QUIET)
if(msquic_FOUND)
    if(TARGET msquic AND NOT TARGET msquic::msquic)
        add_library(msquic::msquic ALIAS msquic)
    endif()
    set(MsQuic_FOUND TRUE)
    return()
endif()

set(MOQ_MSQUIC_ROOT "${CMAKE_CURRENT_LIST_DIR}/../../msquic" CACHE PATH
    "Path to an MsQuic checkout (header at src/inc/msquic.h)")

find_path(MOQ_MSQUIC_INCLUDE_DIR msquic.h
    PATHS "${MOQ_MSQUIC_ROOT}/src/inc"
    NO_DEFAULT_PATH
)
find_library(MOQ_MSQUIC_LIBRARY
    NAMES msquic
    PATHS
        "${MOQ_MSQUIC_ROOT}/build/bin/Release"
        "${MOQ_MSQUIC_ROOT}/build/bin/Debug"
        "${MOQ_MSQUIC_ROOT}/build/bin"
    NO_DEFAULT_PATH
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(MsQuic
    REQUIRED_VARS MOQ_MSQUIC_INCLUDE_DIR MOQ_MSQUIC_LIBRARY
)

if(MsQuic_FOUND AND NOT TARGET msquic::msquic)
    add_library(msquic::msquic UNKNOWN IMPORTED)
    set_target_properties(msquic::msquic PROPERTIES
        IMPORTED_LOCATION "${MOQ_MSQUIC_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${MOQ_MSQUIC_INCLUDE_DIR}"
    )
endif()
