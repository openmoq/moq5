cmake_minimum_required(VERSION 3.20)
# This embedding project owns its consumers' link policies; the LibMoQ child
# cannot set them for its parent. Let capable linkers deduplicate shared edges.
foreach(policy CMP0156 CMP0179)
    if(POLICY ${policy})
        cmake_policy(SET ${policy} NEW)
    endif()
endforeach()
project(moq_package_embedding LANGUAGES C CXX)
include(GNUInstallDirs)
# A real source library owned by the embedding project. This is an export-graph
# fixture, not a stand-in for a transport SDK or a backend runtime qualification.
option(MOQ_BUILD_PACKAGE_EXPORTS "Generate LibMoQ package exports" ON)
option(FIXTURE_PROVIDER_EXPORTS "Register the fixture provider's own exports" ON)
option(FIXTURE_SERVICE "Build the embedding service component" ON)
add_library(fixture_provider_runtime runtime.c)
add_library(fixture_provider::runtime ALIAS fixture_provider_runtime)
set_target_properties(fixture_provider_runtime PROPERTIES EXPORT_NAME runtime
    POSITION_INDEPENDENT_CODE ON)
target_include_directories(fixture_provider_runtime PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>")
if(FIXTURE_PROVIDER_EXPORTS)
    install(TARGETS fixture_provider_runtime EXPORT FixtureProviderTargets
        ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}" RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}")
    export(EXPORT FixtureProviderTargets NAMESPACE fixture_provider::
        FILE "${CMAKE_BINARY_DIR}/FixtureProviderTargets.cmake")
    install(EXPORT FixtureProviderTargets NAMESPACE fixture_provider::
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/fixture_provider")
    configure_file(fixture_providerConfig.cmake "${CMAKE_BINARY_DIR}/fixture_providerConfig.cmake" COPYONLY)
    install(FILES fixture_providerConfig.cmake
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/fixture_provider")
else()
    install(TARGETS fixture_provider_runtime
        ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}" RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}")
    file(REMOVE "${CMAKE_BINARY_DIR}/FixtureProviderTargets.cmake"
        "${CMAKE_BINARY_DIR}/fixture_providerConfig.cmake")
endif()
install(FILES fixture_provider.h DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
set(MOQ_MODE embedding)
add_subdirectory("${MOQ_SOURCE}/tests/cmake/manual_component_exports/producer" producer)
# The actual core and service targets must remain usable without exporting this
# dependency. With packaging ON, native CMake resolves its separate provider set.
target_link_libraries(moq-core PUBLIC fixture_provider::runtime)
foreach(lang c cxx)
    if(TARGET moq::service)
        if(lang STREQUAL "c")
            set(source consumer.c)
        else()
            set(source consumer.cpp)
        endif()
    elseif(lang STREQUAL "c")
        set(source consumer_core.c)
    else()
        set(source consumer_core.cpp)
    endif()
    add_executable("embedded_${lang}" "${source}")
    if(TARGET moq::service)
        target_link_libraries("embedded_${lang}" PRIVATE moq::service)
    else()
        target_link_libraries("embedded_${lang}" PRIVATE moq::core)
    endif()
    if(lang STREQUAL "cxx")
        target_link_libraries("embedded_${lang}" PRIVATE moq::cpp)
    endif()
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options("embedded_${lang}" PRIVATE -Wall -Wextra -Wpedantic -Werror)
    endif()
endforeach()
set(owned_targets moq::core moq::msf moq::relay-core moq::relay)
set(has_service OFF)
if(TARGET moq::service)
    list(APPEND owned_targets moq::service)
    set(has_service ON)
endif()
set(metadata "set(package_libdir [==[${CMAKE_INSTALL_LIBDIR}]==])\nset(package_includedir [==[${CMAKE_INSTALL_INCLUDEDIR}]==])\nset(owned_targets [==[${owned_targets}]==])\nset(has_service ${has_service})\n")
foreach(target IN LISTS owned_targets)
    string(MAKE_C_IDENTIFIER "${target}" id)
    string(APPEND metadata "set(owned_${id} [==[$<TARGET_FILE:${target}>]==])\n")
endforeach()
foreach(lang c cxx)
    string(APPEND metadata "set(executable_${lang} [==[$<TARGET_FILE:embedded_${lang}>]==])\n")
endforeach()
file(GENERATE OUTPUT "${CMAKE_BINARY_DIR}/embedding.cmake" CONTENT "${metadata}")
