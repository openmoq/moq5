# What an installed SDK carries beyond the service -- the player
# (<moq/media_player.h>) and MPEG-TS (<moq/mpeg2ts.h>) -- read from its own
# pkg-config metadata: libmoq-service's includedir, found in the SDK's
# lib/pkgconfig (or lib64/pkgconfig) directory, with the dependency
# directories its closure spans searched after it. Never a fixed path, and
# never another SDK's metadata: a prefix without libmoq-service.pc carries
# nothing. The same rule as the rows' check (scripts/ctest_row.sh).
#
#   include(sdk_features.cmake)
#   moq_rust_sdk_carries(<prefix> <pkg-config> <dependency pc path>
#                        <player var> <mpeg2ts var> <includedir var>)
#
# What pkg-config prints on its error stream is never discarded: it is a
# CMake warning, each of its lines as printed. Run as a script, the same
# call for one SDK prints that warning, if any, and then the answer:
#   cmake -DPREFIX=<prefix> -DPKG_CONFIG=<pkg-config> [-DPC_PATH=<a : list>]
#         -P sdk_features.cmake
#   -> player=ON|OFF mpeg2ts=ON|OFF includedir=<directory, or empty>

function(moq_rust_sdk_carries prefix pkg_config pc_path out_player out_mpeg2ts out_includedir)
    set(player OFF)
    set(mpeg2ts OFF)
    set(includedir "")
    set(pcdir "")
    foreach(d lib/pkgconfig lib64/pkgconfig)
        if(NOT pcdir AND EXISTS "${prefix}/${d}/libmoq-service.pc")
            set(pcdir "${prefix}/${d}")
        endif()
    endforeach()
    if(pcdir)
        set(search "${pcdir}")
        if(pc_path)
            string(APPEND search ":${pc_path}")
        endif()
        execute_process(
            COMMAND "${CMAKE_COMMAND}" -E env "PKG_CONFIG_PATH=${search}"
                "${pkg_config}" --variable=includedir libmoq-service
            RESULT_VARIABLE rc OUTPUT_VARIABLE includedir ERROR_VARIABLE messages
            OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_STRIP_TRAILING_WHITESPACE)
        if(messages)
            # Indented lines are printed as they are, not rewrapped.
            string(REPLACE "\n" "\n  " messages "${messages}")
            message(WARNING "Rust rows: pkg-config, asked where ${prefix} keeps its headers, said:\n  ${messages}")
        endif()
        if(NOT rc STREQUAL "0")
            set(includedir "")
        endif()
    endif()
    if(includedir)
        if(EXISTS "${includedir}/moq/media_player.h")
            set(player ON)
        endif()
        if(EXISTS "${includedir}/moq/mpeg2ts.h")
            set(mpeg2ts ON)
        endif()
    endif()
    set(${out_player} ${player} PARENT_SCOPE)
    set(${out_mpeg2ts} ${mpeg2ts} PARENT_SCOPE)
    set(${out_includedir} "${includedir}" PARENT_SCOPE)
endfunction()

if(CMAKE_SCRIPT_MODE_FILE STREQUAL CMAKE_CURRENT_LIST_FILE)
    foreach(_v PREFIX PKG_CONFIG)
        if(NOT DEFINED ${_v} OR "${${_v}}" STREQUAL "")
            message(FATAL_ERROR "pass -D${_v}=<value>")
        endif()
    endforeach()
    if(NOT DEFINED PC_PATH)
        set(PC_PATH "")
    endif()
    moq_rust_sdk_carries("${PREFIX}" "${PKG_CONFIG}" "${PC_PATH}" _player _mpeg2ts _includedir)
    message("player=${_player} mpeg2ts=${_mpeg2ts} includedir=${_includedir}")
endif()
