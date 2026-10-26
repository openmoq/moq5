cmake_minimum_required(VERSION 3.20)
project(moq_component_export_mechanics LANGUAGES C CXX)
set(MOQ_BUILD_PACKAGE_EXPORTS ON)
# This fixture tests the private native registration with real tiny libraries; it is not a
# fake provider SDK and cannot certify the corresponding production backends.
get_property(multi GLOBAL PROPERTY GENERATOR_IS_MULTI_CONFIG)
if(multi)
    if(MOQ_RELEASE_ONLY)
        set(CMAKE_CONFIGURATION_TYPES Release CACHE STRING "Fixture configurations" FORCE)
    else()
        set(CMAKE_CONFIGURATION_TYPES "Debug;Release" CACHE STRING "Fixture configurations" FORCE)
    endif()
endif()
include(GNUInstallDirs)
include("${MOQ_SOURCE}/cmake/MoqComponentExports.cmake")
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/dependency.cpp"
    "#include <string>\nextern \"C\" int probe_dependency(void) {\n#ifdef NDEBUG\nreturn std::stoi(std::string(\"2\"));\n#else\nreturn std::stoi(std::string(\"1\"));\n#endif\n}\n")
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    add_compile_options(-Wall -Wextra -Wpedantic -Werror)
endif()
add_library(dependency "${CMAKE_CURRENT_BINARY_DIR}/dependency.cpp")
target_compile_features(dependency PRIVATE cxx_std_11)
set_target_properties(dependency PROPERTIES POSITION_INDEPENDENT_CODE ON)
# A C-only marker keeps the own-C++ component's language requirement from
# being supplied accidentally by the separately native-exported C++ dependency.
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/marker.c" "int probe_marker(void) { return 0; }\n")
add_library(marker STATIC "${CMAKE_CURRENT_BINARY_DIR}/marker.c")
set_target_properties(marker PROPERTIES POSITION_INDEPENDENT_CODE ON)
add_library(install_marker STATIC "${CMAKE_CURRENT_BINARY_DIR}/marker.c")
set_target_properties(install_marker PROPERTIES POSITION_INDEPENDENT_CODE ON)
# A private reference checks the source's explicit NO_SONAME metadata.
# The runtime cases separately discriminate the imported library identity.
add_library(no_soname SHARED "${CMAKE_CURRENT_BINARY_DIR}/marker.c")
set_target_properties(no_soname PROPERTIES NO_SONAME ON)
install(TARGETS dependency marker install_marker no_soname EXPORT ProbeTargets
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}" LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}")
export(EXPORT ProbeTargets NAMESPACE probe:: FILE "${CMAKE_BINARY_DIR}/NativeTargets.cmake")
install(EXPORT ProbeTargets NAMESPACE probe:: FILE NativeTargets.cmake
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/libmoq")
set(components service adapter-picoquic adapter-picoquic-threaded adapter-pico-wt
    adapter-pico-wt-managed adapter-wtquic adapter-wtquic-network-managed
    adapter-wtquic-msquic-managed adapter-msquic adapter-mvfst
    adapter-proxygen-wt adapter-proxygen-wt-managed)
set(modules libmoqServiceTargets libmoqAdapterTargets libmoqAdapterThreadedTargets
    libmoqPicoWtAdapterTargets libmoqPicoWtManagedTargets libmoqWtquicAdapterTargets
    libmoqWtquicNetworkManagedTargets libmoqWtquicMsquicManagedTargets
    libmoqMsquicAdapterTargets libmoqAdapterMvfstTargets
    libmoqAdapterProxygenWtTargets libmoqAdapterProxygenWtManagedTargets)
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/include" "${CMAKE_CURRENT_BINARY_DIR}/link-search")
install(DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/link-search/"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/probe-search")
set(interface_expectations "")
set(header "#ifdef __cplusplus\nextern \"C\" {\n#endif\n")
set(main "#include <probe.h>\n")
set(config "include(\"\${CMAKE_CURRENT_LIST_DIR}/NativeTargets.cmake\")\n")
set(metadata "set(components [==[${components}]==])\nset(modules [==[${modules}]==])\n")
list(LENGTH components count)
math(EXPR last "${count} - 1")
foreach(i RANGE ${last})
    list(GET components ${i} component)
    list(GET modules ${i} module)
    string(MAKE_C_IDENTIFIER "${component}" id)
    if(component STREQUAL "adapter-mvfst")
        set(source "${CMAKE_CURRENT_BINARY_DIR}/${id}.cpp")
        file(WRITE "${source}"
            "#include <string>\nextern \"C\" int probe_marker(void);\nextern \"C\" int probe_${id}(void) {\n#ifdef NDEBUG\nreturn std::stoi(std::string(\"2\")) + probe_marker();\n#else\nreturn std::stoi(std::string(\"1\")) + probe_marker();\n#endif\n}\n")
        set(real_dependency marker)
        set(build_links "$<LINK_ONLY:probe::marker>")
        set(install_links "$<LINK_ONLY:probe::install_marker>")
        set(install_dependency install_marker)
    else()
        set(source "${CMAKE_CURRENT_BINARY_DIR}/${id}.c")
        file(WRITE "${source}"
            "extern int probe_dependency(void);\nint probe_${id}(void) { return probe_dependency(); }\n")
        set(real_dependency dependency)
        set(install_dependency dependency)
        set(build_links "$<LINK_ONLY:probe::dependency>")
        set(install_links "$<LINK_ONLY:probe::dependency>")
    endif()
    add_library("moq-${component}" "${source}")
    if(component STREQUAL "adapter-mvfst")
        target_compile_features("moq-${component}" PRIVATE cxx_std_11)
    endif()
    set_target_properties("moq-${component}" PROPERTIES
        OUTPUT_NAME_DEBUG "${component}-debug" OUTPUT_NAME_RELEASE "${component}-release"
        MACOSX_RPATH TRUE)
    target_include_directories("moq-${component}" INTERFACE
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/include>"
        "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>")
    target_link_libraries("moq-${component}" PUBLIC
        "$<BUILD_INTERFACE:$<LINK_ONLY:${real_dependency}>>"
        "$<INSTALL_INTERFACE:$<LINK_ONLY:${install_dependency}>>")
    target_link_directories("moq-${component}" INTERFACE
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/link-search>"
        "$<INSTALL_INTERFACE:${CMAKE_INSTALL_LIBDIR}/probe-search>")
    if(APPLE AND component STREQUAL "adapter-proxygen-wt-managed")
        # Build uses its absolute source-owned identity; installation changes
        # that identity to @rpath. Native exports must distinguish the two.
        set_target_properties("moq-${component}" PROPERTIES MACOSX_RPATH FALSE
            BUILD_WITH_INSTALL_NAME_DIR FALSE INSTALL_NAME_DIR "@rpath")
    endif()
    moq_export_component("moq-${component}" "${component}" "${module}")
    list(APPEND targets "moq::${component}")
    string(MAKE_C_IDENTIFIER "moq::${component}" target_id)
    # Independent source-side usage/identity expectations. These are not read
    # back from either native export file.
    string(APPEND interface_expectations
        "set(own_cxx_${target_id} [==[${component}]==])\nset(build_dirs_${target_id} [==[${CMAKE_CURRENT_BINARY_DIR}/link-search]==])\nset(install_dirs_relative_${target_id} [==[${CMAKE_INSTALL_LIBDIR}/probe-search]==])\nset(build_links_${target_id} [==[${build_links}]==])\nset(install_links_${target_id} [==[${install_links}]==])\n")
    string(APPEND metadata "set(expected_${target_id} [==[$<TARGET_FILE:moq-${component}>]==])\nset(kind_${target_id} [==[$<TARGET_PROPERTY:moq-${component},TYPE>]==])\n")
    if(BUILD_SHARED_LIBS AND (APPLE OR CMAKE_SYSTEM_NAME STREQUAL "Linux"))
        if(APPLE)
            set(install_identity "@rpath/$<TARGET_SONAME_FILE_NAME:moq-${component}>")
            if(component STREQUAL "adapter-proxygen-wt-managed")
                set(build_identity "$<TARGET_SONAME_FILE:moq-${component}>")
            else()
                set(build_identity "${install_identity}")
            endif()
        else()
            set(build_identity "$<TARGET_SONAME_FILE_NAME:moq-${component}>")
            set(install_identity "${build_identity}")
        endif()
        string(APPEND metadata
            "set(build_identity_${target_id} [==[${build_identity}]==])\nset(install_identity_${target_id} [==[${install_identity}]==])\n")
    endif()
    string(APPEND header "int probe_${id}(void);\n")
    string(APPEND main "#if defined(SMOKE_${id})\n#define probe_value probe_${id}\n#endif\n")
    string(APPEND config "include(\"\${CMAKE_CURRENT_LIST_DIR}/${module}.cmake\")\n")
endforeach()
add_library(moq-adapter-msquic-managed INTERFACE)
add_library(moq::adapter-msquic-managed ALIAS moq-adapter-msquic-managed)
add_library(moq::adapter-msquic ALIAS moq-adapter-msquic)
target_link_libraries(moq-adapter-msquic-managed INTERFACE moq::adapter-msquic)
moq_export_component(moq-adapter-msquic-managed adapter-msquic-managed libmoqMsquicManagedTargets)
string(APPEND config "include(\"\${CMAKE_CURRENT_LIST_DIR}/libmoqMsquicManagedTargets.cmake\")\n")
string(APPEND header "#ifdef __cplusplus\n}\n#endif\n")
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/include/probe.h" "${header}")
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/include/probe.h" DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
string(APPEND main "int main(void) {\n#ifdef PROBE_EXPECTED\nreturn probe_value() != PROBE_EXPECTED;\n#elif defined(NDEBUG)\nreturn probe_value() != 2;\n#else\nreturn probe_value() != 1;\n#endif\n}\n")
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/main.c" "${main}")
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/main.cpp" "#include \"main.c\"\n")
file(WRITE "${CMAKE_BINARY_DIR}/component_testConfig.cmake" "${config}")
file(WRITE "${CMAKE_BINARY_DIR}/component_testConfig-install.cmake" "${config}")
install(FILES "${CMAKE_BINARY_DIR}/component_testConfig-install.cmake"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/libmoq" RENAME component_testConfig.cmake)
file(WRITE "${CMAKE_BINARY_DIR}/interface-expectations.cmake" "${interface_expectations}")
string(APPEND metadata "set(targets [==[${targets}]==])\n")
file(GENERATE OUTPUT "${CMAKE_BINARY_DIR}/expected-$<IF:$<BOOL:$<CONFIG>>,$<CONFIG>,NOCONFIG>.cmake"
    CONTENT "${metadata}")
file(WRITE "${CMAKE_BINARY_DIR}/package.cmake"
    "set(package_name component_test)\nset(package_dir [==[${CMAKE_BINARY_DIR}]==])\nset(package_libdir [==[${CMAKE_INSTALL_LIBDIR}]==])\n")
