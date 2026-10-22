# The SDK the Rust rows consume: install the build tree into a fresh,
# test-owned prefix (or, with EXISTING=ON, take the named prefix as it is),
# and require the service pkg-config file in it.
#   cmake -DBUILD=<tree> -DPREFIX=<prefix> [-DCONFIG=<cfg>] [-DEXISTING=ON]
#         -P stage_sdk.cmake
foreach(_v BUILD PREFIX)
    if(NOT DEFINED ${_v} OR "${${_v}}" STREQUAL "")
        message(FATAL_ERROR "pass -D${_v}=<path>")
    endif()
endforeach()
if(NOT EXISTING)
    file(REMOVE_RECURSE "${PREFIX}")
    set(_config)
    if(DEFINED CONFIG AND NOT "${CONFIG}" STREQUAL "")
        set(_config --config "${CONFIG}")
    endif()
    execute_process(
        COMMAND "${CMAKE_COMMAND}" --install "${BUILD}" --prefix "${PREFIX}" ${_config}
        RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
    # The install output is kept beside the prefix, and read: a warning in
    # it (a missing or misplaced file cmake still installs around) fails
    # the staging rather than being discarded with a success status.
    file(WRITE "${PREFIX}-install.log" "${_out}")
    if(NOT _rc STREQUAL "0")
        message(FATAL_ERROR "staging the SDK failed: ${_rc}\n${_out}")
    endif()
    string(REGEX MATCH "(^|\n)[^\n]*([Ww]arning|WARNING|[Ee]rror)[^\n]*" _diag "${_out}")
    if(_diag)
        message(FATAL_ERROR "staging the SDK emitted a diagnostic:${_diag}\n(full output: ${PREFIX}-install.log)")
    endif()
endif()
if(NOT EXISTS "${PREFIX}/lib/pkgconfig/libmoq-service.pc" AND
   NOT EXISTS "${PREFIX}/lib64/pkgconfig/libmoq-service.pc")
    message(FATAL_ERROR
        "the staged SDK has no libmoq-service.pc: the service was built with a backend "
        "that has no pkg-config representation (mvfst, proxygen or the direct MsQuic "
        "managed backend), which the Rust binding does not link")
endif()
message(STATUS "staged SDK: ${PREFIX}")
