# MoQ over raw QUIC on MsQuic.
#
# When the managed facade is enabled it shares this library rather than
# living in its own dylib: the doorbell facade (msquic_managed.c) calls an
# adapter-internal helper (moq_msq_conn_service_wake) that is inseparable
# from the hidden conn machinery in msquic_adapter.c, so under
# BUILD_SHARED_LIBS=ON the two translation units must resolve inside one
# shared object. moq::adapter-msquic-managed is therefore an INTERFACE over
# this base target (below). MOQ_BUILD_MSQUIC_MANAGED=OFF keeps the attach
# library lean and free of the threading dependency.
add_library(moq-adapter-msquic
    msquic_adapter.c
)
add_library(moq::adapter-msquic ALIAS moq-adapter-msquic)
if(MOQ_BUILD_MSQUIC_MANAGED)
    find_package(Threads REQUIRED)
    target_sources(moq-adapter-msquic PRIVATE msquic_managed.c)
endif()

target_include_directories(moq-adapter-msquic PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)
# msquic::msquic is PUBLIC: the adapter header includes <msquic.h>.
target_link_libraries(moq-adapter-msquic
    PUBLIC moq::core msquic::msquic
)
if(MOQ_BUILD_MSQUIC_MANAGED)
    # The managed sources own the doorbell thread. PRIVATE: a shared base
    # dylib resolves pthread internally, and a static base propagates the
    # dependency to consumers that pull the managed objects.
    target_link_libraries(moq-adapter-msquic PRIVATE Threads::Threads)
endif()
target_compile_definitions(moq-adapter-msquic PRIVATE MOQ_BUILDING)
set_target_properties(moq-adapter-msquic PROPERTIES
    C_VISIBILITY_PRESET hidden
    POSITION_INDEPENDENT_CODE ON
)
if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(moq-adapter-msquic PRIVATE
        -Wall -Wextra -Wpedantic
        $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()

# Managed facade component. Its implementation lives in the base library
# above (shared-link ownership: the facade and the hidden helper it calls
# must share one dylib). This INTERFACE target is the stable public
# contract — moq::adapter-msquic-managed / find_package(... COMPONENTS
# adapter-msquic-managed) — carrying the base target and the threading
# dependency the managed objects need. No separate managed archive/dylib is
# produced; link the CMake target, not a library file.
if(MOQ_BUILD_MSQUIC_MANAGED)
    add_library(moq-adapter-msquic-managed INTERFACE)
    add_library(moq::adapter-msquic-managed ALIAS
        moq-adapter-msquic-managed)
    target_link_libraries(moq-adapter-msquic-managed INTERFACE
        moq::adapter-msquic Threads::Threads)
endif()

# -- Build-tree imported targets so `find_package(libmoq COMPONENTS
# adapter-msquic[-managed])` works against ${PROJECT_BINARY_DIR},
# mirroring the wtquic adapter. Install-tree counterparts below.
file(GENERATE
    OUTPUT "${PROJECT_BINARY_DIR}/libmoqMsquicAdapterTargets.cmake"
    CONTENT "\
if(NOT TARGET moq::adapter-msquic)\n\
  add_library(moq::adapter-msquic $<IF:$<STREQUAL:$<TARGET_PROPERTY:moq-adapter-msquic,TYPE>,STATIC_LIBRARY>,STATIC,SHARED> IMPORTED)\n\
  set_target_properties(moq::adapter-msquic PROPERTIES\n\
    IMPORTED_LOCATION \"$<TARGET_FILE:moq-adapter-msquic>\"\n\
    INTERFACE_INCLUDE_DIRECTORIES \"${CMAKE_CURRENT_SOURCE_DIR}/include\"\n\
    INTERFACE_LINK_LIBRARIES \"moq::core;msquic::msquic\")\n\
endif()\n")
if(MOQ_BUILD_MSQUIC_MANAGED)
    # INTERFACE imported target: the managed implementation ships inside the
    # attach library, so there is no managed artifact to point at.
    file(GENERATE
        OUTPUT "${PROJECT_BINARY_DIR}/libmoqMsquicManagedTargets.cmake"
        CONTENT "\
if(NOT TARGET moq::adapter-msquic-managed)\n\
  add_library(moq::adapter-msquic-managed INTERFACE IMPORTED)\n\
  set_target_properties(moq::adapter-msquic-managed PROPERTIES\n\
    INTERFACE_LINK_LIBRARIES \"moq::adapter-msquic;Threads::Threads\")\n\
endif()\n")
endif()

# -- Install / export --------------------------------------------------
include(GNUInstallDirs)

install(TARGETS moq-adapter-msquic
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES include/moq/msquic.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/moq)
if(MOQ_BUILD_MSQUIC_MANAGED)
    # The managed facade is an INTERFACE over the attach library — no
    # artifact of its own. Only its header and the component target file
    # (below) are installed.
    install(FILES include/moq/msquic_managed.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/moq)
endif()

# The installed component re-finds MsQuic through the same vendored
# module the build used.
install(FILES ${CMAKE_SOURCE_DIR}/cmake/FindMsQuic.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/libmoq)

# Relocatable install-tree imported target files (MsQuic is re-found by
# the component blocks in libmoqConfig).
set(_msquic_lib_name "$<TARGET_FILE_NAME:moq-adapter-msquic>")
set(_msquic_lib_type "$<IF:$<STREQUAL:$<TARGET_PROPERTY:moq-adapter-msquic,TYPE>,STATIC_LIBRARY>,STATIC,SHARED>")
set(_msquic_libdir "${CMAKE_INSTALL_LIBDIR}")
set(_msquic_incdir "${CMAKE_INSTALL_INCLUDEDIR}")
configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/libmoqMsquicAdapterTargets.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/libmoqMsquicAdapterTargets-install.cmake.in
    @ONLY)
file(GENERATE
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libmoqMsquicAdapterTargets-install.cmake
    INPUT ${CMAKE_CURRENT_BINARY_DIR}/libmoqMsquicAdapterTargets-install.cmake.in)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libmoqMsquicAdapterTargets-install.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/libmoq
    RENAME libmoqMsquicAdapterTargets.cmake)
if(MOQ_BUILD_MSQUIC_MANAGED)
    # Relocatable managed component file: an INTERFACE imported target over
    # the installed attach library (no artifact of its own, so nothing to
    # locate). The template is placeholder-free; configure_file just stages
    # it into the install cmake dir.
    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/cmake/libmoqMsquicManagedTargets.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/libmoqMsquicManagedTargets-install.cmake
        COPYONLY)
    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libmoqMsquicManagedTargets-install.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/libmoq
        RENAME libmoqMsquicManagedTargets.cmake)
endif()

# No pkg-config file: MsQuic ships no .pc to chain a Requires: onto, so
# a .pc here would have to bake link paths. CMake package consumption
# is the supported path (see README).

if(MOQ_BUILD_TESTS)
    # Public-header compile/link smokes: <moq/msquic.h> stands alone,
    # in C and C++, against the build-tree target.
    add_executable(test_msquic_public_compile
        tests/test_msquic_public_compile.c)
    # the managed target carries the attach target publicly
    if(MOQ_BUILD_MSQUIC_MANAGED)
        target_link_libraries(test_msquic_public_compile PRIVATE
            moq::adapter-msquic-managed)
        target_compile_definitions(test_msquic_public_compile PRIVATE
            MOQ_HAVE_MSQUIC_MANAGED)
    else()
        target_link_libraries(test_msquic_public_compile PRIVATE
            moq::adapter-msquic)
    endif()
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(test_msquic_public_compile PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME msquic_public_compile COMMAND test_msquic_public_compile)

    include(CheckLanguage)
    check_language(CXX)
    if(CMAKE_CXX_COMPILER)
        enable_language(CXX)
        add_executable(test_msquic_public_compile_cxx
            tests/test_msquic_public_compile.cpp)
        if(MOQ_BUILD_MSQUIC_MANAGED)
            target_link_libraries(test_msquic_public_compile_cxx PRIVATE
                moq::adapter-msquic-managed)
            target_compile_definitions(test_msquic_public_compile_cxx
                PRIVATE MOQ_HAVE_MSQUIC_MANAGED)
        else()
            target_link_libraries(test_msquic_public_compile_cxx PRIVATE
                moq::adapter-msquic)
        endif()
        set_target_properties(test_msquic_public_compile_cxx PROPERTIES
            CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)
        add_test(NAME msquic_public_compile_cxx
            COMMAND test_msquic_public_compile_cxx)
    endif()

    # White-box units over the fake QUIC_API_TABLE: compiles the
    # adapter source directly with the testing seams enabled — no real
    # MsQuic behavior involved.
    add_executable(test_msquic_unit
        tests/test_msquic_unit.c
        tests/support/fake_msq_table.c
        msquic_adapter.c)
    target_include_directories(test_msquic_unit PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
        ${CMAKE_SOURCE_DIR}/tests) # shared fake_endpoint for the rx pair
    target_compile_definitions(test_msquic_unit PRIVATE
        MOQ_MSQUIC_TESTING MOQ_BUILDING)
    target_link_libraries(test_msquic_unit PRIVATE
        moq::core msquic::msquic)
    target_include_directories(test_msquic_unit PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include)
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(test_msquic_unit PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME msquic_unit COMMAND test_msquic_unit)

    # The transport terminal always reaches the session, exactly once --
    # including when the bridge already latched a terminal of its own, whose
    # latch tells the session nothing. Same white-box rig as the units: the
    # production connection callback and service entries over the fake API
    # table, one thread, no socket and no timing.
    add_executable(test_msquic_close_feed
        tests/test_msquic_close_feed.c
        tests/support/fake_msq_table.c
        msquic_adapter.c)
    target_include_directories(test_msquic_close_feed PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
        ${CMAKE_CURRENT_SOURCE_DIR}/include)
    target_compile_definitions(test_msquic_close_feed PRIVATE
        MOQ_MSQUIC_TESTING MOQ_BUILDING)
    target_link_libraries(test_msquic_close_feed PRIVATE
        moq::core msquic::msquic)
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(test_msquic_close_feed PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME msquic_close_feed COMMAND test_msquic_close_feed)
    set_tests_properties(msquic_close_feed PROPERTIES
        LABELS "msquic"
        TIMEOUT 60)

    # Consumers: the same standalone project resolved against the
    # build tree (no install) and against a scratch install prefix.
    # Both request the managed component, which pulls the attach
    # component and re-finds MsQuic. Discovery hints from this build
    # are forwarded so the consumer resolves the same MsQuic.
    if(MOQ_BUILD_MSQUIC_MANAGED)
        add_test(NAME msquic_adapter_consumer
            COMMAND ${CMAKE_COMMAND}
                -DBUILD=${PROJECT_BINARY_DIR}
                -DSRC=${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer
                -DWORK=${CMAKE_CURRENT_BINARY_DIR}/buildtree-consumer-work
                -DC_COMPILER=${CMAKE_C_COMPILER}
                "-DC_FLAGS=${CMAKE_C_FLAGS}"
                "-DLINK_FLAGS=${CMAKE_EXE_LINKER_FLAGS}"
                -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer_buildtree.cmake)
        set_tests_properties(msquic_adapter_consumer PROPERTIES
            TIMEOUT 180)

        add_test(NAME msquic_install_consumer
            COMMAND ${CMAKE_COMMAND}
                -DBUILD=${PROJECT_BINARY_DIR}
                -DSRC=${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer
                -DWORK=${CMAKE_CURRENT_BINARY_DIR}/install-consumer-work
                "-DMSQUIC_DIR_HINT=${msquic_DIR}"
                "-DMSQUIC_ROOT_HINT=${MOQ_MSQUIC_ROOT}"
                -DC_COMPILER=${CMAKE_C_COMPILER}
                "-DC_FLAGS=${CMAKE_C_FLAGS}"
                "-DLINK_FLAGS=${CMAKE_EXE_LINKER_FLAGS}"
                -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer_install.cmake)
        set_tests_properties(msquic_install_consumer PROPERTIES
            TIMEOUT 300)

        # Shared-library link regression: a BUILD_SHARED_LIBS=ON tree must
        # build and link the managed component (the facade and its hidden
        # helper share one dylib). A static build cannot catch the
        # cross-dylib failure, so this configures a fresh shared tree.
        add_test(NAME msquic_managed_shared_link
            COMMAND ${CMAKE_COMMAND}
                -DSRC=${CMAKE_SOURCE_DIR}
                -DWORK=${CMAKE_CURRENT_BINARY_DIR}/managed-shared-link-work
                "-DMSQUIC_DIR_HINT=${msquic_DIR}"
                "-DMSQUIC_ROOT_HINT=${MOQ_MSQUIC_ROOT}"
                -DC_COMPILER=${CMAKE_C_COMPILER}
                -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/managed_shared_link.cmake)
        set_tests_properties(msquic_managed_shared_link PROPERTIES
            LABELS "msquic"
            TIMEOUT 600)
    endif()

    # Managed loopback: real MsQuic on localhost, self-signed certs,
    # managed client + managed server, one MoQ pub/sub flow.
    if(MOQ_BUILD_MSQUIC_MANAGED)
        # A COMMITTED, test-only loopback identity: a fixed self-signed
        # certificate and key that live in the source tree and are passed to
        # each consumer directly. Generating a fresh identity per run through
        # an ambient `openssl` added no security coverage, made every
        # loopback test depend on a tool being on PATH at configure time, and
        # made registration itself conditional on that discovery. Provenance
        # is carried by the filenames, tests/README_test_certs.md and the
        # certificate's own subject -- never by prose inside the PEM.
        set(MOQ_MSQUIC_TEST_CERT
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_only_loopback_cert.pem)
        set(MOQ_MSQUIC_TEST_KEY
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_only_loopback_key.pem)

        # Application-event hold/release over real MsQuic loopback: a
        # receiver that stops polling object events neither breaks the
        # connection nor loses data, receives nothing while held, and on
        # release takes the whole workload byte-exact, in order and exactly
        # once. It claims no sender backpressure and no adapter arrest --
        # the arrest/hold/resume mechanics are owned deterministically by
        # the unit rail, and receive-credit renewal by the over-window
        # qualification cell.
        add_executable(test_msquic_recv_loopback
            tests/test_msquic_recv_loopback.c)
        target_link_libraries(test_msquic_recv_loopback PRIVATE
            moq::adapter-msquic-managed Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_msquic_recv_loopback PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        # Two cells, two lanes, one binary: a CTest label applies to the
        # registered command, not to a C function, so each registration
        # names the case it wants through the selector. `stall` is the
        # application-event hold/release cell; `over_window` is
        # qualification -- it proves real QUIC receive-credit renewal past
        # the 16/32 MiB windows, which no sans-I/O rail can reproduce, but
        # it is not a pause/resume proof and does not own queue-capacity
        # policy either.
        add_test(NAME msquic_recv_loopback
            COMMAND test_msquic_recv_loopback
                ${MOQ_MSQUIC_TEST_CERT} ${MOQ_MSQUIC_TEST_KEY} stall)
        set_tests_properties(msquic_recv_loopback PROPERTIES
            LABELS "network;msquic"
            TIMEOUT 120)
        add_test(NAME msquic_over_window_credit
            COMMAND test_msquic_recv_loopback
                ${MOQ_MSQUIC_TEST_CERT} ${MOQ_MSQUIC_TEST_KEY} over_window)
        set_tests_properties(msquic_over_window_credit PROPERTIES
            LABELS "network;msquic;qualification"
            TIMEOUT 120)

        # The managed max_open_subgroups must reach the LIVE session: with a
        # constrained (=1) pool the server's second concurrent open is refused,
        # while a prefix-sized (old-caller) config keeps the session default and
        # the second open succeeds.
        add_executable(test_msquic_subgroup_pool
            tests/test_msquic_subgroup_pool.c)
        target_link_libraries(test_msquic_subgroup_pool PRIVATE
            moq::adapter-msquic-managed Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_msquic_subgroup_pool PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()

        add_executable(test_msquic_loopback tests/test_msquic_loopback.c)
        target_link_libraries(test_msquic_loopback PRIVATE
            moq::adapter-msquic-managed Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_msquic_loopback PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()

        add_test(NAME msquic_loopback
            COMMAND test_msquic_loopback
                ${MOQ_MSQUIC_TEST_CERT} ${MOQ_MSQUIC_TEST_KEY})
        set_tests_properties(msquic_loopback PROPERTIES
            LABELS "network;msquic"
            TIMEOUT 120)
        add_test(NAME msquic_subgroup_pool
            COMMAND test_msquic_subgroup_pool
                ${MOQ_MSQUIC_TEST_CERT} ${MOQ_MSQUIC_TEST_KEY})
        set_tests_properties(msquic_subgroup_pool PROPERTIES
            LABELS "network;msquic"
            TIMEOUT 120)

        add_executable(test_msquic_multi_alpn
            tests/test_msquic_multi_alpn.c)
        target_link_libraries(test_msquic_multi_alpn PRIVATE
            moq::adapter-msquic-managed Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_msquic_multi_alpn PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME msquic_multi_alpn
            COMMAND test_msquic_multi_alpn
                ${MOQ_MSQUIC_TEST_CERT} ${MOQ_MSQUIC_TEST_KEY})
        set_tests_properties(msquic_multi_alpn PROPERTIES
            LABELS "network;msquic"
            TIMEOUT 120)

        # Decisive lane-partition pin: accepts synthesized through the exact
        # production listener callback over a borrowed fake API table, so
        # placement, lane-scoped iteration, wake selectivity, deadline cause
        # separation and callback confinement are proven with no listener,
        # socket, credential or certificate. Compiles the production adapter
        # and managed sources directly so every private seam stays test-only.
        add_executable(test_msquic_lanes
            tests/test_msquic_lanes.c
            tests/support/fake_msq_table.c
            msquic_adapter.c
            msquic_managed.c)
        target_include_directories(test_msquic_lanes PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests
            ${CMAKE_CURRENT_SOURCE_DIR}/include
            ${CMAKE_SOURCE_DIR}/tests) # shared fake_endpoint for the peer
        target_compile_definitions(test_msquic_lanes PRIVATE
            MOQ_MSQUIC_TESTING MOQ_BUILDING)
        target_link_libraries(test_msquic_lanes PRIVATE
            moq::core msquic::msquic Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_msquic_lanes PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME msquic_lanes COMMAND test_msquic_lanes)
        set_tests_properties(msquic_lanes PROPERTIES
            LABELS "msquic;sansio"
            TIMEOUT 30)

        # Managed-lane seams: the API table is borrowed rather than opened,
        # no doorbell thread is created, one production doorbell decision is
        # driven explicitly on the caller's thread, and the lane snapshot
        # observes without mutating and fails closed past its row capacity.
        # Compiles the production sources directly so every seam stays
        # test-only.
        add_executable(test_msq_seams
            tests/test_msq_seams.c
            tests/support/fake_msq_table.c
            msquic_adapter.c
            msquic_managed.c)
        target_include_directories(test_msq_seams PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests
            ${CMAKE_CURRENT_SOURCE_DIR}/include
            ${CMAKE_SOURCE_DIR}/tests)
        target_compile_definitions(test_msq_seams PRIVATE
            MOQ_MSQUIC_TESTING MOQ_BUILDING)
        target_link_libraries(test_msq_seams PRIVATE
            moq::core msquic::msquic Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_msq_seams PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME msq_seams COMMAND test_msq_seams)
        set_tests_properties(msq_seams PROPERTIES
            LABELS "msquic;sansio"
            TIMEOUT 30)

        # Fake-MsQuic shutdown history: repeated stream shutdowns keep their
        # order and their distinct (flags, code) pairs, and the first call
        # that cannot be recorded makes the loss observable.
        add_executable(test_fake_msq_history
            tests/test_fake_msq_history.c
            tests/support/fake_msq_table.c)
        target_include_directories(test_fake_msq_history PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests
            ${CMAKE_CURRENT_SOURCE_DIR}/include)
        target_compile_definitions(test_fake_msq_history PRIVATE
            MOQ_MSQUIC_TESTING MOQ_BUILDING)
        target_link_libraries(test_fake_msq_history PRIVATE
            moq::core msquic::msquic Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_fake_msq_history PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME fake_msq_history COMMAND test_fake_msq_history)
        set_tests_properties(fake_msq_history PROPERTIES
            LABELS "msquic;sansio"
            TIMEOUT 30)

        # Size-qualified config-prefix derivation on the production path:
        # a prefix-sized cfg defaults every appended field, a cfg sized
        # through max_connections honors that field while still defaulting
        # lane_count, and a full cfg honors all of them. Admission runs
        # through the exact production listener callback over a borrowed
        # fake API table, so this needs no listener, socket, credential or
        # certificate. Compiles the production adapter and managed sources
        # directly so every private seam stays test-only.
        add_executable(test_msquic_cfg_prefix
            tests/test_msquic_cfg_prefix.c
            tests/support/fake_msq_table.c
            msquic_adapter.c
            msquic_managed.c)
        target_include_directories(test_msquic_cfg_prefix PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests
            ${CMAKE_CURRENT_SOURCE_DIR}/include)
        target_compile_definitions(test_msquic_cfg_prefix PRIVATE
            MOQ_MSQUIC_TESTING MOQ_BUILDING)
        target_link_libraries(test_msquic_cfg_prefix PRIVATE
            moq::core msquic::msquic Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_msquic_cfg_prefix PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME msquic_cfg_prefix COMMAND test_msquic_cfg_prefix)
        set_tests_properties(msquic_cfg_prefix PROPERTIES
            LABELS "msquic;sansio"
            TIMEOUT 30)

        # Facade control surface: drain refuses while releasing the reserve,
        # stop is idempotent, the facade pending aggregates are the sum across
        # lanes (children on distinct lanes over distinct fake connections),
        # and a live SERVER facade reports a bounded wait as MOQ_DONE rather
        # than claiming it is terminal. Lanes-only facade: no listener,
        # socket, credential or certificate. Compiles the production adapter
        # and managed sources directly so every private seam stays test-only.
        add_executable(test_msquic_facade_control
            tests/test_msquic_facade_control.c
            tests/support/fake_msq_table.c
            msquic_adapter.c
            msquic_managed.c)
        target_include_directories(test_msquic_facade_control PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests
            ${CMAKE_CURRENT_SOURCE_DIR}/include)
        target_compile_definitions(test_msquic_facade_control PRIVATE
            MOQ_MSQUIC_TESTING MOQ_BUILDING)
        target_link_libraries(test_msquic_facade_control PRIVATE
            moq::core msquic::msquic Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_msquic_facade_control PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME msquic_facade_control COMMAND test_msquic_facade_control)
        set_tests_properties(msquic_facade_control PROPERTIES
            LABELS "msquic;sansio"
            TIMEOUT 30)

        # Callback confinement: the CLIENT-only session accessor is valid
        # ONLY inside lane 0's on_lane_pump (NULL from an app thread, from
        # on_activity, and always on a SERVER), and stop() from inside any
        # managed callback is refused rather than deadlocked. Lanes-only
        # facade with an injected child: no listener, socket, credential or
        # certificate.
        add_executable(test_msquic_confinement
            tests/test_msquic_confinement.c
            tests/support/fake_msq_table.c
            msquic_adapter.c
            msquic_managed.c)
        target_include_directories(test_msquic_confinement PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests
            ${CMAKE_CURRENT_SOURCE_DIR}/include)
        target_compile_definitions(test_msquic_confinement PRIVATE
            MOQ_MSQUIC_TESTING MOQ_BUILDING)
        target_link_libraries(test_msquic_confinement PRIVATE
            moq::core msquic::msquic Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_msquic_confinement PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME msquic_confinement COMMAND test_msquic_confinement)
        set_tests_properties(msquic_confinement PROPERTIES
            LABELS "msquic;sansio"
            TIMEOUT 30)

        # No-spin scheduling, counted rather than timed: one wake owes
        # exactly one pump generation and the lane then parks at its
        # pre-wait barrier; a raw bell generation arms no work and buys no
        # pump at all. A build that re-armed without end is stopped INSIDE
        # the callback by the documented nonzero pump-exit return, so it is
        # observed rather than hung.
        add_executable(test_msquic_no_spin
            tests/test_msquic_no_spin.c
            tests/support/fake_msq_table.c
            msquic_adapter.c
            msquic_managed.c)
        target_include_directories(test_msquic_no_spin PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests
            ${CMAKE_CURRENT_SOURCE_DIR}/include
            ${CMAKE_SOURCE_DIR}/tests) # shared fake_endpoint for the peer
        target_compile_definitions(test_msquic_no_spin PRIVATE
            MOQ_MSQUIC_TESTING MOQ_BUILDING)
        target_link_libraries(test_msquic_no_spin PRIVATE
            moq::core msquic::msquic Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_msquic_no_spin PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME msquic_no_spin COMMAND test_msquic_no_spin)
        set_tests_properties(msquic_no_spin PROPERTIES
            LABELS "msquic;sansio"
            TIMEOUT 30)

        # cfg.streaming_objects must reach the CHILD SESSION through the
        # production child path (mgd_make_child). A rig that builds its own
        # subscriber session cannot pin that, so the child here is created
        # by the facade and driven through a full MoQ flow against a raw
        # peer over the fake table. Both arms run on independent facades.
        add_executable(test_msquic_child_forwarding
            tests/test_msquic_child_forwarding.c
            tests/support/fake_msq_table.c
            msquic_adapter.c
            msquic_managed.c)
        target_include_directories(test_msquic_child_forwarding PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests
            ${CMAKE_CURRENT_SOURCE_DIR}/include
            ${CMAKE_SOURCE_DIR}/tests) # shared fake_endpoint for the peer
        target_compile_definitions(test_msquic_child_forwarding PRIVATE
            MOQ_MSQUIC_TESTING MOQ_BUILDING)
        target_link_libraries(test_msquic_child_forwarding PRIVATE
            moq::core msquic::msquic Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_msquic_child_forwarding PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME msquic_child_forwarding
            COMMAND test_msquic_child_forwarding)
        set_tests_properties(msquic_child_forwarding PROPERTIES
            LABELS "msquic;sansio"
            TIMEOUT 30)

        # Deterministic lane statistics: exact wake/deadline/idle cause
        # accounting, ABI-prefix discipline, callback confinement, and
        # accepted-send lifetime totals across ordinary reap and stop. The
        # facade borrows the fake API table and opens no listener, socket,
        # credential, or certificate. Compiles the production adapter and
        # managed sources directly so every private seam stays test-only.
        add_executable(test_msquic_lane_stats
            tests/test_msquic_lane_stats.c
            tests/support/fake_msq_table.c
            msquic_adapter.c
            msquic_managed.c)
        target_include_directories(test_msquic_lane_stats PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests
            ${CMAKE_CURRENT_SOURCE_DIR}/include)
        target_compile_definitions(test_msquic_lane_stats PRIVATE
            MOQ_MSQUIC_TESTING MOQ_BUILDING)
        target_link_libraries(test_msquic_lane_stats PRIVATE
            moq::core msquic::msquic Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_msquic_lane_stats PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME msquic_lane_stats COMMAND test_msquic_lane_stats)
        set_tests_properties(msquic_lane_stats PROPERTIES
            LABELS "msquic;sansio"
            TIMEOUT 30)

        # Managed settings policy: the listener's pinned path MTU. Compiles
        # the adapter + managed sources directly (test seams on) so it asserts
        # the SAME settings construction the create path hands to
        # ConfigurationOpen(), not a reconstruction. Pure assertions -- no
        # socket, no certificates, no transport.
        add_executable(test_msquic_settings
            tests/test_msquic_settings.c
            msquic_adapter.c
            msquic_managed.c)
        target_include_directories(test_msquic_settings PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/include)
        target_compile_definitions(test_msquic_settings PRIVATE
            MOQ_MSQUIC_TESTING MOQ_BUILDING)
        target_link_libraries(test_msquic_settings PRIVATE
            moq::core msquic::msquic Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_msquic_settings PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME msquic_settings COMMAND test_msquic_settings)
        set_tests_properties(msquic_settings PROPERTIES
            LABELS "msquic" TIMEOUT 30)

        # Network-free cross-lane doorbell proof: check->wait handoff,
        # bidirectional wakes, coalescing, cause classification, callback
        # confinement, and wake-vs-stop. Uses only the lanes-only facade and
        # explicit test rendezvous; no listener, certificate, socket, sleep,
        # retry, or elapsed-time verdict. Compiles the adapter + managed
        # sources directly with testing seams and links no production adapter.
        add_executable(test_msquic_wake
            tests/test_msquic_wake.c
            msquic_adapter.c
            msquic_managed.c)
        target_include_directories(test_msquic_wake PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/include)
        target_compile_definitions(test_msquic_wake PRIVATE
            MOQ_MSQUIC_TESTING MOQ_BUILDING)
        target_link_libraries(test_msquic_wake PRIVATE
            moq::core msquic::msquic Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_msquic_wake PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME msquic_wake COMMAND test_msquic_wake)
        set_tests_properties(msquic_wake PROPERTIES
            LABELS "msquic;sansio"
            TIMEOUT 30)

        # Reaper scheduling pin: a child is not reclaimed while a pump is owed
        # (a pump OPPORTUNITY precedes further batch reclamation — not proof the
        # application consumed the terminal, which is separate later work). Uses
        # the reap-gap seam to make the unlock-window race deterministic, so it
        # compiles the adapter + managed sources with the testing seams on and
        # links no production adapter library.
        add_executable(test_msquic_reap
            tests/test_msquic_reap.c
            msquic_adapter.c
            msquic_managed.c)
        target_include_directories(test_msquic_reap PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/include)
        target_compile_definitions(test_msquic_reap PRIVATE
            MOQ_MSQUIC_TESTING MOQ_BUILDING)
        target_link_libraries(test_msquic_reap PRIVATE
            moq::core msquic::msquic Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_msquic_reap PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME msquic_reap
            COMMAND test_msquic_reap
                ${MOQ_MSQUIC_TEST_CERT} ${MOQ_MSQUIC_TEST_KEY})
        set_tests_properties(msquic_reap PROPERTIES
            LABELS "network;msquic"
            TIMEOUT 120)

        # Reap FAIRNESS pin, plus the pump-exit and stop controls for the new
        # reap site: an acknowledged, transport-reapable child must be
        # reclaimed while its lane still has work owed on every pass, not only
        # once the lane falls idle. NETWORK-FREE — the facade comes from a
        # testing constructor that opens no listener, socket or credential, so
        # this needs no cert fixture and carries no network label. Compiles the
        # adapter + managed sources with the testing seams on and links no
        # production adapter library.
        add_executable(test_msquic_reap_fairness
            tests/test_msquic_reap_fairness.c
            msquic_adapter.c
            msquic_managed.c)
        target_include_directories(test_msquic_reap_fairness PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/include)
        target_compile_definitions(test_msquic_reap_fairness PRIVATE
            MOQ_MSQUIC_TESTING MOQ_BUILDING)
        target_link_libraries(test_msquic_reap_fairness PRIVATE
            moq::core msquic::msquic Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_msquic_reap_fairness PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        # No cert fixture and no network: the facade this test builds opens no
        # listener, so it is registered unconditionally.
        add_test(NAME msquic_reap_fairness COMMAND test_msquic_reap_fairness)
        set_tests_properties(msquic_reap_fairness PROPERTIES
            LABELS "msquic"
            TIMEOUT 120)

        # Terminal-child lifetime pin: a bounded application pump that leaves
        # SESSION_CLOSED unconsumed must still be given its child, its session
        # and its own per-connection state on a later pump; an unacknowledged
        # child holds its admission reserve; and ack_terminal classifies every
        # guard exactly. Kept out of test_msquic_reap so that binary's narrower
        # scope — a pump OPPORTUNITY precedes further batch reclamation — stays
        # separately reportable. The facade borrows the fake API table and opens
        # no listener, socket, credential or certificate; the reap-gap and
        # prewait seams observe the doorbell's own decision points, so it
        # compiles the adapter + managed sources with the testing seams on and
        # links no production adapter library.
        add_executable(test_msquic_terminal_ack
            tests/test_msquic_terminal_ack.c
            tests/support/fake_msq_table.c
            msquic_adapter.c
            msquic_managed.c)
        target_include_directories(test_msquic_terminal_ack PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests
            ${CMAKE_CURRENT_SOURCE_DIR}/include)
        target_compile_definitions(test_msquic_terminal_ack PRIVATE
            MOQ_MSQUIC_TESTING MOQ_BUILDING)
        target_link_libraries(test_msquic_terminal_ack PRIVATE
            moq::core msquic::msquic Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_msquic_terminal_ack PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME msquic_terminal_ack COMMAND test_msquic_terminal_ack)
        set_tests_properties(msquic_terminal_ack PROPERTIES
            LABELS "msquic;sansio"
            TIMEOUT 30)

        # Production-contract conformance over real MsQuic loopback:
        # every MoQ session call runs inside the managed on_lane_pump,
        # driven by per-side event-triggered scripts (see
        # tests/msquic_contract_pair.h — deliberately not the shared
        # sim-shaped conformance rail).
        add_executable(test_msquic_conformance
            tests/test_msquic_conformance.c
            tests/msquic_contract_pair.c)
        target_link_libraries(test_msquic_conformance PRIVATE
            moq::adapter-msquic-managed Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_msquic_conformance PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME msquic_conformance
            COMMAND test_msquic_conformance
                ${MOQ_MSQUIC_TEST_CERT} ${MOQ_MSQUIC_TEST_KEY})
        set_tests_properties(msquic_conformance PROPERTIES
            LABELS "network;msquic"
            TIMEOUT 180)

        # Stress/soak: repeated full-lifecycle churn over the same
        # script harness (no thresholds; MOQ_MSQUIC_STRESS_LOOPS
        # extends the run for manual soaks).
        add_executable(test_msquic_stress
            tests/test_msquic_stress.c
            tests/msquic_contract_pair.c)
        target_link_libraries(test_msquic_stress PRIVATE
            moq::adapter-msquic-managed Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_msquic_stress PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME msquic_stress
            COMMAND test_msquic_stress
                ${MOQ_MSQUIC_TEST_CERT} ${MOQ_MSQUIC_TEST_KEY})
        # Soak, not correctness: repeated full-lifecycle churn with no
        # threshold of its own. It stays in the msquic and network label
        # sets, so a plain full run and `ctest -L msquic` still include it;
        # only the fast gate excludes it, and does so explicitly.
        set_tests_properties(msquic_stress PROPERTIES
            LABELS "network;msquic;soak"
            TIMEOUT 600)

        # A registration set that differs between the FIRST configure of a
        # fresh tree and an immediate second one is invisible from inside an
        # already-configured build tree: by the time anyone looks, the cache
        # is populated and both passes agree. So this regression configures a
        # throwaway CHILD build in a directory it deletes first, and compares
        # its first- and second-configure CTest name sets. Any guard that
        # consults state its own configure has not yet written -- a cache
        # entry, a discovered path, a recorded branch -- drops a test from the
        # first pass only, which is exactly what the comparison catches.
        #
        # MOQ_MSQUIC_REGISTRATION_CHILD is internal and test-only: the child
        # sets it so it does not register this fixture again. It defaults OFF,
        # so an ordinary configure still registers the top-level test, and the
        # fixture asserts the suppression actually took effect.
        # Validate the recorded branch HERE, in configure scope -- before and
        # independently of whether OpenSSL exists or this particular test is
        # registered. Doing it only inside the script would be unreachable for
        # an empty or unrecognized mode, because such a mode fails the
        # registration conjunction below and the script never runs: the test
        # would silently disappear instead of failing.
        if(NOT MOQ_MSQUIC_DISCOVERY_MODE STREQUAL "pinned"
           AND NOT MOQ_MSQUIC_DISCOVERY_MODE STREQUAL "package"
           AND NOT MOQ_MSQUIC_DISCOVERY_MODE STREQUAL "root"
           AND NOT MOQ_MSQUIC_DISCOVERY_MODE STREQUAL "preexisting")
            message(FATAL_ERROR
                "MsQuic was found but FindMsQuic recorded discovery mode "
                "'${MOQ_MSQUIC_DISCOVERY_MODE}', which is not one of "
                "pinned/package/root/preexisting. Every branch that resolves MsQuic "
                "must record which one it was, or the configure-order "
                "regression silently stops being registered.")
        endif()

        # The recorded branch, surfaced so a nested control can observe it
        # without it having to live in (and pollute) the cache. The artifacts
        # are surfaced the same way and for the same reason: the pin branch
        # publishes them as configure-scope variables, so the cache is not a
        # place either a human or a nested control can read them from.
        message(STATUS
            "MsQuic discovery mode: ${MOQ_MSQUIC_DISCOVERY_MODE}")
        # Only the branches that RESOLVE a header/library pair have one to
        # report, and the test is on the MODE rather than on the variables:
        # a package or an injected target can be configured in a tree whose
        # cache still holds an earlier root search's results, and publishing
        # those would attribute one dependency's paths to another. Those two
        # branches carry their artifacts on the imported target instead, under
        # config-specific property names that are not this project's to guess.
        set(_moq_msquic_owns_pair FALSE)
        if(MOQ_MSQUIC_DISCOVERY_MODE STREQUAL "pinned"
           OR MOQ_MSQUIC_DISCOVERY_MODE STREQUAL "root")
            set(_moq_msquic_owns_pair TRUE)
        endif()

        # A pinned tree's results are configure-scope variables, so this is the
        # one place the equality can be checked at all -- and it is what lets
        # the identity below be read from the pin INPUTS, which are cached and
        # survive any path a filesystem accepts.
        if(MOQ_MSQUIC_DISCOVERY_MODE STREQUAL "pinned")
            if(NOT MOQ_MSQUIC_INCLUDE_DIR STREQUAL "${MOQ_MSQUIC_PIN_INCLUDE_DIR}"
               OR NOT MOQ_MSQUIC_LIBRARY STREQUAL "${MOQ_MSQUIC_PIN_LIBRARY}")
                message(FATAL_ERROR
                    "MsQuic resolved through the pin branch but published "
                    "something other than the pinned pair.\n"
                    "  pin include dir: ${MOQ_MSQUIC_PIN_INCLUDE_DIR}\n"
                    "  published:       ${MOQ_MSQUIC_INCLUDE_DIR}\n"
                    "  pin library:     ${MOQ_MSQUIC_PIN_LIBRARY}\n"
                    "  published:       ${MOQ_MSQUIC_LIBRARY}")
            endif()
        endif()

        if(_moq_msquic_owns_pair)
            message(STATUS
                "MsQuic include dir: ${MOQ_MSQUIC_INCLUDE_DIR}")
            message(STATUS
                "MsQuic library: ${MOQ_MSQUIC_LIBRARY}")
        endif()

        # A machine-readable copy of the same facts. Status text is for people:
        # a nested control that parsed paths out of it would be parsing a
        # human-formatted, wrapped, space-separated stream, and would silently
        # truncate any path containing a space. Bracket arguments hand the
        # values over exactly as they are.
        set(_moq_msquic_ident_inc "")
        set(_moq_msquic_ident_lib "")
        if(_moq_msquic_owns_pair)
            set(_moq_msquic_ident_inc "${MOQ_MSQUIC_INCLUDE_DIR}")
            set(_moq_msquic_ident_lib "${MOQ_MSQUIC_LIBRARY}")
        endif()
        file(WRITE "${CMAKE_BINARY_DIR}/msquic-discovery.cmake"
"set(MOQ_MSQUIC_DISCOVERY_MODE [==[${MOQ_MSQUIC_DISCOVERY_MODE}]==])\n"
"set(MOQ_MSQUIC_OWNS_PAIR [==[${_moq_msquic_owns_pair}]==])\n"
"set(MOQ_MSQUIC_INCLUDE_DIR [==[${_moq_msquic_ident_inc}]==])\n"
"set(MOQ_MSQUIC_LIBRARY [==[${_moq_msquic_ident_lib}]==])\n")

        # The registration set is no longer gated on an ambient tool: the
        # loopback identity is committed, so the certificate-consuming tests
        # register on the first configure of a fresh tree unconditionally.
        # That is exactly what the child now proves -- the two passes must
        # agree, AND both receive registrations must already be present in
        # the first one, so a guard that read a cache entry before its own
        # discovery wrote it still fails here.
        #
        # Only for the discovery branches an independent child can reproduce:
        # a pinned artifact pair, a package, or a checkout root. FindMsQuic
        # also supports a caller-supplied
        # msquic::msquic target ("preexisting"); that dependency exists only
        # in the parent's own scope, so a nested configure cannot be given it
        # -- and inventing a fake package/root identity for it would test a
        # different dependency. In that mode this one test is omitted while
        # the rest of the MsQuic suite registers normally. Any other value
        # already failed the configure-scope check above -- it is not silently
        # dropped here.
        if(NOT MOQ_MSQUIC_REGISTRATION_CHILD
           AND (MOQ_MSQUIC_DISCOVERY_MODE STREQUAL "pinned"
                OR MOQ_MSQUIC_DISCOVERY_MODE STREQUAL "package"
                OR MOQ_MSQUIC_DISCOVERY_MODE STREQUAL "root"))
            # Which of FindMsQuic's discovery branches ACTUALLY resolved
            # MsQuic this configure. Read from the module's own record, not
            # inferred from msquic_DIR: a nonempty msquic_DIR can be stale while
            # package discovery is disabled and the checkout fallback is what
            # really ran, and classifying that as package mode would send the
            # child off to whatever ambient package it can find.
            set(_reg_mode "${MOQ_MSQUIC_DISCOVERY_MODE}")
            add_test(NAME msquic_first_configure_registration
                COMMAND ${CMAKE_COMMAND}
                    -DSOURCE_DIR=${CMAKE_SOURCE_DIR}
                    -DBUILD_DIR=${CMAKE_CURRENT_BINARY_DIR}/first-configure-child
                    -DCMAKE_CMD=${CMAKE_COMMAND}
                    -DCTEST_CMD=${CMAKE_CTEST_COMMAND}
                    -DMSQUIC_MODE=${_reg_mode}
                    "-DMSQUIC_DIR=${msquic_DIR}"
                    "-DMSQUIC_ROOT=${MOQ_MSQUIC_ROOT}"
                    "-DMSQUIC_INCLUDE_DIR=${MOQ_MSQUIC_INCLUDE_DIR}"
                    "-DMSQUIC_LIBRARY=${MOQ_MSQUIC_LIBRARY}"
                    -DEXPECT_RECEIVE_REGISTRATIONS=ON
                    -DSELF_TEST_NAME=msquic_first_configure_registration
                    -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/first_configure_registration.cmake)
            set_tests_properties(msquic_first_configure_registration PROPERTIES
                LABELS "msquic"
                TIMEOUT 600)
        endif()

        # The pinned-artifact contract, proved by refusal. Registered wherever
        # a concrete header/library pair is known, which is the only thing the
        # cases need -- the pin under test is passed to each child explicitly,
        # so this does not depend on how THIS tree resolved MsQuic.
        if(NOT MOQ_MSQUIC_REGISTRATION_CHILD AND _moq_msquic_owns_pair)
            add_test(NAME msquic_pinned_fail_closed
                COMMAND ${CMAKE_COMMAND}
                    -DSOURCE_DIR=${CMAKE_SOURCE_DIR}
                    -DBUILD_DIR=${CMAKE_CURRENT_BINARY_DIR}/pinned-fail-closed-child
                    -DCMAKE_CMD=${CMAKE_COMMAND}
                    -DCTEST_CMD=${CMAKE_CTEST_COMMAND}
                    "-DMSQUIC_INCLUDE_DIR=${MOQ_MSQUIC_INCLUDE_DIR}"
                    "-DMSQUIC_LIBRARY=${MOQ_MSQUIC_LIBRARY}"
                    # The synthetic checkout root the test builds has to be
                    # findable by find_library on THIS platform. Its naming is
                    # taken from CMake's own platform knowledge rather than
                    # guessed from the pinned file, whose name may be versioned
                    # (libmsquic.so.2.6.0) or otherwise unrepresentative.
                    "-DLIB_PREFIX=${CMAKE_SHARED_LIBRARY_PREFIX}"
                    "-DLIB_SUFFIX=${CMAKE_SHARED_LIBRARY_SUFFIX}"
                    "-DLIB_SUFFIXES=${CMAKE_FIND_LIBRARY_SUFFIXES}"
                    -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/pinned_msquic_fail_closed.cmake)
            set_tests_properties(msquic_pinned_fail_closed PROPERTIES
                LABELS "msquic"
                TIMEOUT 600)
        endif()

        # FindMsQuic's preexisting-target branch: prove a caller-supplied
        # msquic::msquic configures cleanly, records that branch, OMITS the
        # nested-configure regression above rather than registering it to fail,
        # and keeps the rest of the MsQuic suite. Registered only from the root
        # branch, which is where a concrete include dir and library to import
        # are known; the child it configures is a plain project configure, so it
        # needs no recursion guard of its own.
        if(NOT MOQ_MSQUIC_REGISTRATION_CHILD
           AND (MOQ_MSQUIC_DISCOVERY_MODE STREQUAL "root"
                OR MOQ_MSQUIC_DISCOVERY_MODE STREQUAL "pinned")
           AND MOQ_MSQUIC_INCLUDE_DIR AND MOQ_MSQUIC_LIBRARY)
            add_test(NAME msquic_preexisting_target
                COMMAND ${CMAKE_COMMAND}
                    -DSOURCE_DIR=${CMAKE_SOURCE_DIR}
                    -DBUILD_DIR=${CMAKE_CURRENT_BINARY_DIR}/preexisting-child
                    -DCMAKE_CMD=${CMAKE_COMMAND}
                    -DCTEST_CMD=${CMAKE_CTEST_COMMAND}
                    "-DMSQUIC_INCLUDE_DIR=${MOQ_MSQUIC_INCLUDE_DIR}"
                    "-DMSQUIC_LIBRARY=${MOQ_MSQUIC_LIBRARY}"
                    -DSELF_TEST_NAME=msquic_first_configure_registration
                    -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/preexisting_msquic_target.cmake)
            set_tests_properties(msquic_preexisting_target PROPERTIES
                LABELS "msquic"
                TIMEOUT 600)
        endif()
    endif()
endif()
