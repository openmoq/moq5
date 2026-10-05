cmake_minimum_required(VERSION 3.20)
foreach(v SOURCE ARCHIVE WORK)
    if(NOT DEFINED ${v})
        message(FATAL_ERROR "pass -D${v}=<absolute path>")
    endif()
endforeach()
file(MAKE_DIRECTORY "${WORK}/recipe/scripts" "${WORK}/recipe/cmake/patches" "${WORK}/bin" "${WORK}/deps")
file(COPY "${SOURCE}/scripts/setup_mvfst_deps.sh" DESTINATION "${WORK}/recipe/scripts")
set(patches mvfst-2026.05.25-nullability.patch mvfst-2026.05.25-hostid-guard.patch)
foreach(p IN LISTS patches)
    file(COPY "${SOURCE}/cmake/patches/${p}" DESTINATION "${WORK}/recipe/cmake/patches")
endforeach()
file(WRITE "${WORK}/deps/libssl" "offline dependency fixture\n")
file(WRITE "${WORK}/deps/libcrypto" "offline dependency fixture\n")
file(MAKE_DIRECTORY "${WORK}/deps/openssl")
foreach(config folly-config.cmake fizz-config.cmake fmt-config.cmake openssl/opensslv.h)
    file(WRITE "${WORK}/deps/${config}" "# offline dependency fixture\n")
endforeach()
file(WRITE "${WORK}/wrong.tar.gz" "not the pinned source\n")
file(WRITE "${WORK}/bin/cmake" [=[#!/usr/bin/env bash
set -eu
printf '%s\n' "$*" >> "$CALL_LOG"
case "$1" in
  -S)
    phase=configure
    for arg in "$@"; do
      case "$arg" in -DCMAKE_INSTALL_PREFIX=*) printf '%s\n' "${arg#*=}" > "$STATE_FILE" ;; esac
    done ;;
  --build) phase=build ;;
  --install)
    phase=install
    prefix=$(<"$STATE_FILE")
    mkdir -p "$prefix/lib/cmake/mvfst"
    printf '# offline mock install\n' > "$prefix/lib/cmake/mvfst/mvfst-config.cmake" ;;
  *) exit 8 ;;
esac
if [ "${INDUCE_WARNING:-}" = "$phase" ]; then
  printf 'CMake Warning: deliberately induced zero-exit %s diagnostic\n' "$phase" >&2
fi
exit 0
]=])
file(CHMOD "${WORK}/bin/cmake" PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE)
function(run name archive warning expected)
    file(REMOVE "${WORK}/${name}.calls")
    execute_process(COMMAND "${CMAKE_COMMAND}" -E env
        "PATH=${WORK}/bin:$ENV{PATH}" "CALL_LOG=${WORK}/${name}.calls"
        "STATE_FILE=${WORK}/${name}.state" "INDUCE_WARNING=${warning}"
        "MVFST_ARCHIVE=${archive}" "MVFST_DEPS_DIR=${WORK}/${name}"
        "folly_DIR=${WORK}/deps" "Fizz_DIR=${WORK}/deps" "fmt_DIR=${WORK}/deps"
        "OPENSSL_INCLUDE_DIR=${WORK}/deps" "OPENSSL_SSL_LIBRARY=${WORK}/deps/libssl"
        "OPENSSL_CRYPTO_LIBRARY=${WORK}/deps/libcrypto"
        bash "${WORK}/recipe/scripts/setup_mvfst_deps.sh"
        RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
    file(WRITE "${WORK}/${name}.log" "${out}${err}")
    if(expected STREQUAL "pass")
        if(NOT rc EQUAL 0 OR NOT out MATCHES "mvfst_DIR=" OR "${out}${err}" MATCHES "[Ww]arning[: ]")
            message(FATAL_ERROR "${name}: clean positive failed: ${out}${err}")
        endif()
    else()
        if(rc EQUAL 0 OR NOT "${out}${err}" MATCHES "${expected}" OR out MATCHES "mvfst_DIR=")
            message(FATAL_ERROR "${name}: missing exact refusal: ${out}${err}")
        endif()
        if(expected MATCHES "SHA256 mismatch" AND EXISTS "${WORK}/${name}.calls")
            message(FATAL_ERROR "${name}: reached CMake before refusing identity")
        endif()
    endif()
endfunction()
run(source-mismatch "${WORK}/wrong.tar.gz" "" "source archive SHA256 mismatch")
foreach(p IN LISTS patches)
    file(APPEND "${WORK}/recipe/cmake/patches/${p}" "# altered fixture\n")
    run("${p}-mismatch" "${ARCHIVE}" "" "patch SHA256 mismatch")
    file(COPY "${SOURCE}/cmake/patches/${p}" DESTINATION "${WORK}/recipe/cmake/patches")
endforeach()
run(clean "${ARCHIVE}" "" pass)
foreach(phase configure build install)
    run("diagnostic-${phase}" "${ARCHIVE}" "${phase}" "${phase} emitted diagnostics")
endforeach()
message(STATUS "Offline recipe: source/two-patch mismatch refused before CMake; clean install accepted; three zero-exit diagnostics refused (mock CMake, no provider qualification)")
