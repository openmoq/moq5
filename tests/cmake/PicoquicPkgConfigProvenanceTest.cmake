# The pkg-config provider closure (cmake/MoqPicoquicPkgConfig.cmake) for
# each picoquic topology: a dependency installed inside the prefix in its
# own subtree, installed into LibMoQ's directories, an external install, a
# future (not yet created) install prefix, and a picoquic source tree built
# here. Configure-only (no compiler); every configure must be free of
# diagnostics.
# Args (-D): SRC (repo root), WORK (scratch dir, recreated).
cmake_minimum_required(VERSION 3.20)
foreach(_v SRC WORK)
    if(NOT DEFINED ${_v})
        message(FATAL_ERROR "pass -D${_v}=<path>")
    endif()
endforeach()
file(REMOVE_RECURSE "${WORK}")
set(root "${WORK}/root")
file(MAKE_DIRECTORY "${root}/prefix/deps/include" "${root}/prefix/deps/lib"
    "${root}/prefix/include" "${root}/prefix/lib"
    "${root}/ext/include" "${root}/ext/lib")

function(configure_driver name)
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -Werror=dev
            -S "${SRC}/tests/cmake/picoquic_pkgconfig_provenance"
            -B "${WORK}/${name}" "-DMOQ_SOURCE_DIR=${SRC}" "-DROOT=${root}" ${ARGN}
        RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
    if(NOT rc EQUAL 0)
        message(FATAL_ERROR "${name}: configure failed (${rc})\n${out}\n${err}")
    endif()
    if(NOT err STREQUAL "" OR out MATCHES "(CMake Warning|CMake Deprecation)")
        message(FATAL_ERROR "${name}: diagnostics\n${out}\n${err}")
    endif()
    message(STATUS "${name}: ok")
endfunction()
configure_driver(imported)

# Source mode: no imported picoquic target; the co-installed default layout.
file(WRITE "${WORK}/source-mode/CMakeLists.txt" "
cmake_minimum_required(VERSION 3.20)
project(source_mode NONE)
include(\"${SRC}/cmake/MoqPicoquicPkgConfig.cmake\")
set(CMAKE_INSTALL_PREFIX \"${root}/not-yet\")
foreach(dir include sdk/include)
    set(CMAKE_INSTALL_INCLUDEDIR \${dir})
    moq_picoquic_pkgconfig_cflags(c)
    message(STATUS \"includedir=\${dir} cflags=[\${c}]\")
    if(dir STREQUAL include AND NOT c STREQUAL \"\")
        message(FATAL_ERROR \"default includedir adds [\${c}]\")
    endif()
    if(dir STREQUAL sdk/include AND NOT c STREQUAL \"-I\\\${prefix}/include\")
        message(FATAL_ERROR \"moved includedir gives [\${c}]\")
    endif()
endforeach()
")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -Werror=dev -S "${WORK}/source-mode" -B "${WORK}/source-mode/build"
    RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
if(NOT rc EQUAL 0 OR NOT err STREQUAL "")
    message(FATAL_ERROR "source mode: (${rc})\n${out}\n${err}")
endif()
message(STATUS "source mode: ok")
