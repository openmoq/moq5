# Included by the permanent driver after its safety/warning helpers are defined.
function(embedding_run build metadata label)
    include("${build}/${metadata}")
    foreach(lang c cxx)
        if(loader_oracle STREQUAL "glibc" AND ARGC GREATER 3)
            # The fourth argument is the staged install's owned libdir. This
            # process-only runtime setup replaces, rather than extends, ambient
            # LD_LIBRARY_PATH; product RPATH and build-tree runs are unchanged.
            command("${label}-${lang}-run" PASS "${CMAKE_COMMAND}" -E env
                "LD_LIBRARY_PATH=${ARGV3}" LD_DEBUG=libs "${executable_${lang}}")
        else()
            command("${label}-${lang}-run" PASS "${executable_${lang}}")
        endif()
    endforeach()
endfunction()

function(embedding_no_package build)
    set(package_dir "${build}/producer/libmoq")
    file(GLOB advertised LIST_DIRECTORIES FALSE
        "${package_dir}/*Targets*.cmake" "${package_dir}/libmoqConfig*.cmake"
        "${package_dir}/libmoqRelayComponents.cmake" "${package_dir}/MoqWtquicApi.cmake"
        "${package_dir}/cmake-install/libmoqConfig.cmake")
    if(advertised)
        message(FATAL_ERROR "Packaging OFF advertised LibMoQ package files: ${advertised}")
    endif()
endfunction()

function(embedding_install build label exports)
    include("${build}/embedding.cmake")
    set(prefix "${WORK}/prefix-${label}")
    command("${label}-install" PASS "${CMAKE_COMMAND}" --install "${build}" --prefix "${prefix}")
    foreach(target IN LISTS owned_targets)
        string(MAKE_C_IDENTIFIER "${target}" id)
        installed_artifact("${owned_${id}}" required)
    endforeach()
    set(headers moq/moq.h moq/moq.hpp moq/relay/relay.h)
    if(has_service)
        list(APPEND headers moq/media_sender.h)
    endif()
    foreach(header IN LISTS headers)
        if(NOT EXISTS "${prefix}/${package_includedir}/${header}")
            message(FATAL_ERROR "Packaging ${exports} lost installed header ${header}")
        endif()
    endforeach()
    set(pc_files libmoq.pc libmoq-relay-core.pc libmoq-relay.pc)
    if(has_service)
        list(APPEND pc_files libmoq-service.pc)
    endif()
    foreach(pc IN LISTS pc_files)
        if(NOT EXISTS "${prefix}/${package_libdir}/pkgconfig/${pc}")
            message(FATAL_ERROR "Packaging switch lost existing pkg-config output ${pc}")
        endif()
    endforeach()
    if(exports)
        command("${label}-consumer-configure" PASS "${CMAKE_COMMAND}" ${context}
            -S "${fixture}/embedding/consumer" -B "${WORK}/${label}-consumer" -G Ninja
            -DCMAKE_BUILD_TYPE=Debug "-DFIXTURE_SOURCE=${fixture}/embedding"
            "-Dlibmoq_DIR=${prefix}/${package_libdir}/cmake/libmoq"
            "-Dfixture_provider_DIR=${prefix}/${package_libdir}/cmake/fixture_provider")
        command("${label}-consumer-build" PASS "${CMAKE_COMMAND}" --build "${WORK}/${label}-consumer" --parallel 1)
        embedding_run("${WORK}/${label}-consumer" embedding-consumer.cmake "${label}-consumer"
            "${prefix}/${package_libdir}")
    else()
        file(GLOB_RECURSE modules LIST_DIRECTORIES FALSE "${prefix}/*.cmake")
        if(modules)
            message(FATAL_ERROR "Packaging OFF installed CMake modules: ${modules}")
        endif()
    endif()
endfunction()

foreach(exports OFF ON)
    set(build "${WORK}/packaging-${exports}")
    command("packaging-${exports}-configure" PASS "${CMAKE_COMMAND}" ${context}
        -S "${fixture}/embedding" -B "${build}" -G Ninja -DCMAKE_BUILD_TYPE=Debug
        "-DMOQ_SOURCE=${MOQ_SOURCE}" "-DBUILD_SHARED_LIBS=${MOQ_SHARED}"
        "-DMOQ_BUILD_PACKAGE_EXPORTS=${exports}" "-DFIXTURE_PROVIDER_EXPORTS=${exports}")
    if(NOT exports)
        embedding_no_package("${build}")
    endif()
    command("packaging-${exports}-build" PASS "${CMAKE_COMMAND}" --build "${build}" --parallel 1)
    embedding_run("${build}" embedding.cmake "packaging-${exports}")
    embedding_install("${build}" "packaging-${exports}" "${exports}")
    if(exports)
        command(provider-build-consumer-configure PASS "${CMAKE_COMMAND}" ${context}
            -S "${fixture}/embedding/consumer" -B "${WORK}/provider-build-consumer" -G Ninja
            -DCMAKE_BUILD_TYPE=Debug "-DFIXTURE_SOURCE=${fixture}/embedding"
            "-Dlibmoq_DIR=${build}/producer/libmoq" "-Dfixture_provider_DIR=${build}")
        command(provider-build-consumer-build PASS "${CMAKE_COMMAND}" --build "${WORK}/provider-build-consumer" --parallel 1)
        embedding_run("${WORK}/provider-build-consumer" embedding-consumer.cmake provider-build-consumer)
    endif()
endforeach()
# An ON-to-OFF transition must withdraw the old build package advertisement;
# it must still configure/build with the now-unexported source dependency.
# Disable service simultaneously so its registration helper is not called.
set(build "${WORK}/packaging-ON")
command(packaging-off-again-configure PASS "${CMAKE_COMMAND}" ${context}
    -S "${fixture}/embedding" -B "${build}" -G Ninja -DCMAKE_BUILD_TYPE=Debug
    "-DMOQ_SOURCE=${MOQ_SOURCE}" "-DBUILD_SHARED_LIBS=${MOQ_SHARED}"
    -DMOQ_BUILD_PACKAGE_EXPORTS=OFF -DFIXTURE_PROVIDER_EXPORTS=OFF -DFIXTURE_SERVICE=OFF)
embedding_no_package("${build}")
if(EXISTS "${build}/producer/libmoq/libmoqServiceTargets.cmake")
    message(FATAL_ERROR "Disabled service left its owned module after package OFF")
endif()
command(packaging-off-again-build PASS "${CMAKE_COMMAND}" --build "${build}" --parallel 1)
embedding_run("${build}" embedding.cmake packaging-off-again)
embedding_install("${build}" packaging-off-again OFF)

# The explicit ON contract must not silently hide an unexported source edge.
command(unexported-provider "not in any export set" "${CMAKE_COMMAND}" ${context}
    -S "${fixture}/embedding" -B "${WORK}/unexported-provider" -G Ninja -DCMAKE_BUILD_TYPE=Debug
    "-DMOQ_SOURCE=${MOQ_SOURCE}" "-DBUILD_SHARED_LIBS=${MOQ_SHARED}"
    -DMOQ_BUILD_PACKAGE_EXPORTS=ON -DFIXTURE_PROVIDER_EXPORTS=OFF)
if(NOT transcript MATCHES "fixture_provider_runtime" OR NOT transcript MATCHES "moq-core")
    message(FATAL_ERROR "The ON refusal did not identify the real owned-to-provider export edge")
endif()
message(STATUS "Package exports embedding, shared=${MOQ_SHARED}: OFF in-tree/install, ON provider packages and explicit missing-export refusal passed")
