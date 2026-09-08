# Transport-independent engine and host-pumped session/shard runtime.
set(MOQ_RELAY_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")
set(MOQR_CORE_SOURCES
    src/core/wire_codes.c src/core/capacity.c src/core/log.c
    src/core/relay.c src/core/trace.c src/core/types.c src/core/stats.c)
add_library(moq-relay-core ${MOQR_CORE_SOURCES})
add_library(moq::relay-core ALIAS moq-relay-core)
target_link_libraries(moq-relay-core PUBLIC moq::core)
target_compile_definitions(moq-relay-core PRIVATE MOQ_RELAY_CORE_BUILDING)

function(moqr_component target export_name export_set)
    target_include_directories(${target} PUBLIC
        $<BUILD_INTERFACE:${MOQ_RELAY_INCLUDE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
    target_compile_features(${target} PUBLIC c_std_11)
    set_target_properties(${target} PROPERTIES
        C_VISIBILITY_PRESET hidden POSITION_INDEPENDENT_CODE ON
        EXPORT_NAME ${export_name})
    if(APPLE)
        set_target_properties(${target} PROPERTIES INSTALL_RPATH "@loader_path")
    elseif(UNIX)
        set_target_properties(${target} PROPERTIES INSTALL_RPATH "$ORIGIN")
    endif()
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    install(TARGETS ${target} EXPORT ${export_set}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
    install(EXPORT ${export_set} NAMESPACE moq::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/libmoq)
endfunction()
moqr_component(moq-relay-core relay-core libmoqRelayCoreTargets)
if(NOT BUILD_SHARED_LIBS)
    target_compile_definitions(moq-relay-core PUBLIC MOQ_RELAY_CORE_STATIC)
endif()

set(MOQR_CORE_HEADERS export.h types.h auth.h capacity.h log.h placement.h
    relay.h trace.h wire_codes.h)
foreach(header IN LISTS MOQR_CORE_HEADERS)
    install(FILES include/moq/relay/${header}
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/moq/relay)
endforeach()

if(MOQ_BUILD_RELAY_RUNTIME)
    find_package(Threads REQUIRED)
    add_library(moq-relay src/bind/moqr_bind.c src/shard/moqr_shards.c
        src/obs/metrics.c src/stats.c)
    add_library(moq::relay ALIAS moq-relay)
    target_link_libraries(moq-relay PUBLIC moq::relay-core PRIVATE Threads::Threads)
    target_compile_definitions(moq-relay PRIVATE MOQ_RELAY_BUILDING)
    moqr_component(moq-relay relay libmoqRelayTargets)
    if(NOT BUILD_SHARED_LIBS)
        target_compile_definitions(moq-relay PUBLIC MOQ_RELAY_STATIC)
    endif()
    foreach(header IN ITEMS moqr_bind.h moqr_shards.h moqr_obs.h)
        install(FILES include/moq/relay/${header}
            DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/moq/relay)
    endforeach()
endif()

# Optional CLI policy; neither its headers nor its implementation is an SDK API.
if(MOQ_BUILD_RELAY_TOOL OR MOQ_BUILD_TESTS)
    add_library(moq-relay-policy STATIC private/auth_toy.c)
    target_include_directories(moq-relay-policy PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/private")
    target_link_libraries(moq-relay-policy PUBLIC moq::relay-core)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(moq-relay-policy PRIVATE -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
endif()

# A minimal .pc avoids inheriting the enabled-component aggregate libmoq.pc.
file(RELATIVE_PATH MOQR_PC_UP "/${CMAKE_INSTALL_LIBDIR}/pkgconfig" "/")
set(MOQR_PC_DEFINES "")
if(NOT BUILD_SHARED_LIBS AND WIN32)
    set(MOQR_PC_DEFINES "-DMOQ_STATIC")
endif()
set(MOQR_PC_NAME libmoq-core)
set(MOQR_PC_DESCRIPTION "Sans-I/O MoQ protocol core")
set(MOQR_PC_REQUIRES "")
set(MOQR_PC_LIBRARY moq-core)
set(MOQR_PC_PRIVATE "")
configure_file(cmake/libmoq-relay.pc.in libmoq-core.pc @ONLY)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/libmoq-core.pc"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)

foreach(component IN ITEMS relay-core relay)
    if(component STREQUAL "relay" AND NOT MOQ_BUILD_RELAY_RUNTIME)
        continue()
    endif()
    set(MOQR_PC_NAME libmoq-${component})
    set(MOQR_PC_LIBRARY moq-${component})
    set(MOQR_PC_DESCRIPTION "MOQ5 ${component} library")
    set(MOQR_PC_PRIVATE "")
    set(MOQR_PC_DEFINES "")
    if(component STREQUAL "relay-core")
        set(MOQR_PC_REQUIRES "libmoq-core")
        if(NOT BUILD_SHARED_LIBS)
            set(MOQR_PC_DEFINES "-DMOQ_RELAY_CORE_STATIC")
        endif()
    else()
        set(MOQR_PC_REQUIRES "libmoq-relay-core")
        set(MOQR_PC_PRIVATE "${CMAKE_THREAD_LIBS_INIT}")
        if(NOT BUILD_SHARED_LIBS)
            set(MOQR_PC_DEFINES "-DMOQ_RELAY_STATIC")
        endif()
    endif()
    configure_file(cmake/libmoq-relay.pc.in ${MOQR_PC_NAME}.pc @ONLY)
    install(FILES "${CMAKE_CURRENT_BINARY_DIR}/${MOQR_PC_NAME}.pc"
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)
endforeach()

if(MOQ_BUILD_RELAY_RUNTIME AND
   (MOQ_BUILD_TESTS OR MOQ_BUILD_RELAY_TOOL OR MOQ_BUILD_BENCHMARKS))
    add_library(moq-relay-inspect OBJECT src/shard/moqr_shards.c)
    target_include_directories(moq-relay-inspect PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/private")
    target_compile_definitions(moq-relay-inspect PUBLIC
        MOQR_RELAY_INSPECT MOQ_RELAY_BUILDING)
    target_link_libraries(moq-relay-inspect PUBLIC moq::relay Threads::Threads)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(moq-relay-inspect PRIVATE -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
endif()
if(MOQ_BUILD_TESTS)
    add_subdirectory(tests)
endif()
if(MOQ_BUILD_EXAMPLES OR MOQ_BUILD_TESTS)
    add_executable(relay_in_memory examples/in_memory.c)
    target_link_libraries(relay_in_memory PRIVATE moq::relay-core)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(relay_in_memory PRIVATE -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    if(MOQ_BUILD_TESTS)
        add_test(NAME relay_in_memory COMMAND relay_in_memory)
        set_tests_properties(relay_in_memory PROPERTIES TIMEOUT 30 LABELS "relay;sansio")
    endif()
endif()
install(FILES README.md DESTINATION ${CMAKE_INSTALL_DOCDIR}/relay)
install(FILES docs/api.md docs/architecture.md
    DESTINATION ${CMAKE_INSTALL_DOCDIR}/relay/docs)
install(FILES examples/in_memory.c
    DESTINATION ${CMAKE_INSTALL_DOCDIR}/relay/examples)
if(MOQ_BUILD_RELAY_RUNTIME)
    install(FILES
        ${PROJECT_SOURCE_DIR}/examples/relay/simple-relay/main.c
        ${PROJECT_SOURCE_DIR}/examples/relay/simple-relay/CMakeLists.txt
        ${PROJECT_SOURCE_DIR}/examples/relay/simple-relay/README.md
        DESTINATION ${CMAKE_INSTALL_DOCDIR}/relay/examples/simple-relay)
endif()
if(MOQ_BUILD_RELAY_RUNTIME AND
   (MOQ_BUILD_RELAY_TOOL OR MOQ_BUILD_BENCHMARKS))
    add_subdirectory(bench)
endif()
