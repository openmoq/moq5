enable_language(CXX)

# Header-only C++ binding target
add_library(moq_cpp INTERFACE)
target_include_directories(moq_cpp INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(moq_cpp INTERFACE moq::core)
target_compile_features(moq_cpp INTERFACE cxx_std_20)
add_library(moq::cpp ALIAS moq_cpp)

if(TARGET moq::loc)
    target_link_libraries(moq_cpp INTERFACE moq::loc)
    target_compile_definitions(moq_cpp INTERFACE MOQ_HAS_LOC)
endif()

if(TARGET moq::msf)
    target_link_libraries(moq_cpp INTERFACE moq::msf)
    target_compile_definitions(moq_cpp INTERFACE MOQ_HAS_MSF)
endif()

if(TARGET moq::cmaf)
    target_link_libraries(moq_cpp INTERFACE moq::cmaf)
    target_compile_definitions(moq_cpp INTERFACE MOQ_HAS_CMAF)
endif()

set_target_properties(moq_cpp PROPERTIES EXPORT_NAME cpp)

install(TARGETS moq_cpp
    EXPORT libmoqTargets)

install(DIRECTORY include/moq
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.hpp"
    PATTERN "loc.hpp" EXCLUDE
    PATTERN "msf.hpp" EXCLUDE
    PATTERN "cmaf.hpp" EXCLUDE)

if(TARGET moq::loc)
    install(FILES include/moq/loc.hpp
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/moq)
endif()

if(TARGET moq::msf)
    install(FILES include/moq/msf.hpp
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/moq)
endif()

if(TARGET moq::cmaf)
    install(FILES include/moq/cmaf.hpp
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/moq)
endif()

# Tests (only when tests are enabled)
if(MOQ_BUILD_TESTS)
    # Owned warning baseline for the in-tree C++ binding test executables
    # below (directory scope -> applies to targets defined after this in
    # this dir). It does NOT touch the header-only `moq_cpp` INTERFACE
    # target (defined above, compiles nothing) nor the out-of-tree
    # `cpp_consumer` cold-consumer build (a separate cmake invocation that
    # must stay free of -Werror). -Werror gated by MOQ_WARNINGS_AS_ERRORS.
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        add_compile_options(-Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()

    add_executable(test_cpp_result tests/test_result.cpp)
    target_link_libraries(test_cpp_result PRIVATE moq::cpp)
    add_test(NAME cpp_result COMMAND test_cpp_result)

    add_executable(test_cpp_buffer tests/test_buffer.cpp)
    target_link_libraries(test_cpp_buffer PRIVATE moq::cpp)
    add_test(NAME cpp_buffer COMMAND test_cpp_buffer)

    add_executable(test_cpp_types tests/test_types.cpp)
    target_link_libraries(test_cpp_types PRIVATE moq::cpp)
    add_test(NAME cpp_types COMMAND test_cpp_types)

    add_executable(test_cpp_session tests/test_session.cpp)
    target_link_libraries(test_cpp_session PRIVATE moq::cpp)
    add_test(NAME cpp_session COMMAND test_cpp_session)

    add_executable(test_cpp_operations tests/test_operations.cpp)
    target_link_libraries(test_cpp_operations PRIVATE moq::cpp)
    add_test(NAME cpp_operations COMMAND test_cpp_operations)

    add_executable(test_cpp_advanced_operations tests/test_advanced_operations.cpp)
    target_link_libraries(test_cpp_advanced_operations PRIVATE moq::cpp)
    add_test(NAME cpp_advanced_operations COMMAND test_cpp_advanced_operations)

    add_executable(test_cpp_publisher tests/test_publisher.cpp)
    target_link_libraries(test_cpp_publisher PRIVATE moq::cpp)
    add_test(NAME cpp_publisher COMMAND test_cpp_publisher)

    add_executable(test_cpp_subscriber tests/test_subscriber.cpp)
    target_link_libraries(test_cpp_subscriber PRIVATE moq::cpp)
    add_test(NAME cpp_subscriber COMMAND test_cpp_subscriber)

    if(TARGET moq::loc)
        add_executable(test_cpp_loc tests/test_loc.cpp)
        target_link_libraries(test_cpp_loc PRIVATE moq::cpp)
        add_test(NAME cpp_loc COMMAND test_cpp_loc)
    endif()

    if(TARGET moq::msf)
        add_executable(test_cpp_msf tests/test_msf.cpp)
        target_link_libraries(test_cpp_msf PRIVATE moq::cpp)
        add_test(NAME cpp_msf COMMAND test_cpp_msf)
    endif()

    if(TARGET moq::cmaf)
        add_executable(test_cpp_cmaf tests/test_cmaf.cpp)
        target_link_libraries(test_cpp_cmaf PRIVATE moq::cpp)
        add_test(NAME cpp_cmaf COMMAND test_cpp_cmaf)
    endif()

    # Out-of-tree consumer smoke test
    set(_consumer_src "${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer")
    set(_consumer_bld "${CMAKE_CURRENT_BINARY_DIR}/consumer-test-build")
    set(_consumer_extra "")
    if(MOQ_PICOQUIC_SOURCE_DIR)
        string(APPEND _consumer_extra
            " -DMOQ_PICOQUIC_SOURCE_DIR='${MOQ_PICOQUIC_SOURCE_DIR}'")
    endif()
    if(MOQ_PICOTLS_PREFIX)
        string(APPEND _consumer_extra
            " -DMOQ_PICOTLS_PREFIX='${MOQ_PICOTLS_PREFIX}'")
    endif()
    add_test(NAME cpp_consumer
        COMMAND ${CMAKE_COMMAND} -E env bash -c
            "set -e; \
             '${CMAKE_COMMAND}' -E rm -rf '${_consumer_bld}'; \
             '${CMAKE_COMMAND}' -S '${_consumer_src}' -B '${_consumer_bld}' \
                 -DCMAKE_PREFIX_PATH='${PROJECT_BINARY_DIR}' \
                 -DCMAKE_CXX_COMPILER='${CMAKE_CXX_COMPILER}' \
                 -DCMAKE_C_COMPILER='${CMAKE_C_COMPILER}'\
                 ${_consumer_extra} && \
             '${CMAKE_COMMAND}' --build '${_consumer_bld}' && \
             '${_consumer_bld}/moq_consumer_test'")
endif()
