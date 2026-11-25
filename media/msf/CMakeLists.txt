set(_MSF_CORE_SOURCES
    src/msf.c
    src/msf_check.c
    src/msf_names.c
    src/msf_supplement.c)
set(_MSF_SOURCES ${_MSF_CORE_SOURCES})
if(TARGET moq::media-object AND TARGET moq::cmaf)
    list(APPEND _MSF_SOURCES src/msf_media.c)
endif()

add_library(moq-msf ${_MSF_SOURCES})

add_library(moq::msf ALIAS moq-msf)

target_include_directories(moq-msf
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/vendor
)

target_link_libraries(moq-msf PUBLIC moq::core)

if(TARGET moq::media-object AND TARGET moq::cmaf)
    target_link_libraries(moq-msf PUBLIC moq::media-object moq::cmaf)
endif()

target_compile_features(moq-msf PUBLIC c_std_11)

set_target_properties(moq-msf PROPERTIES
    C_VISIBILITY_PRESET hidden
    OUTPUT_NAME moq-msf
    EXPORT_NAME msf
)

install(TARGETS moq-msf
    EXPORT libmoqTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

install(DIRECTORY include/moq
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.h"
    PATTERN "msf_media.h" EXCLUDE)

if(TARGET moq::media-object AND TARGET moq::cmaf)
    install(FILES include/moq/msf_media.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/moq)
endif()

install(FILES THIRD_PARTY.md
    DESTINATION ${CMAKE_INSTALL_DOCDIR}
    RENAME moq-msf-THIRD_PARTY.md)

target_compile_definitions(moq-msf PRIVATE MOQ_BUILDING)

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq-msf PRIVATE -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
elseif(MSVC)
    target_compile_options(moq-msf PRIVATE $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:/WX>)
endif()

if(MOQ_BUILD_TESTS)
    add_executable(test_msf tests/test_msf.c)
    target_link_libraries(test_msf PRIVATE moq::msf)
    target_compile_definitions(test_msf PRIVATE
        MOQ_MSF_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures")
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_msf PRIVATE -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME msf COMMAND test_msf)

    # The exact export set of moq-msf, in static and shared builds.
    set(_MSF_HAS_MEDIA OFF)
    if(TARGET moq::media-object AND TARGET moq::cmaf)
        set(_MSF_HAS_MEDIA ON)
    endif()
    add_test(NAME msf_symbols COMMAND ${CMAKE_COMMAND}
        -DNM=${CMAKE_NM} -DREADELF=${CMAKE_READELF}
        -DELF=$<STREQUAL:${CMAKE_EXECUTABLE_FORMAT},ELF>
        -DLIBRARY=$<TARGET_FILE:moq-msf>
        -DSHARED=$<BOOL:${BUILD_SHARED_LIBS}>
        -DMEDIA=${_MSF_HAS_MEDIA}
        -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_symbols.cmake)
    set_tests_properties(msf_symbols PROPERTIES TIMEOUT 90 LABELS "media;msf")

    # draft-gregoire-moq-msfts catalog fields, against the draft's examples.
    add_executable(test_msf_mpeg2ts tests/test_msf_mpeg2ts.c)
    target_link_libraries(test_msf_mpeg2ts PRIVATE moq::msf)
    target_compile_definitions(test_msf_mpeg2ts PRIVATE
        MOQ_MSF_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures")
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_msf_mpeg2ts PRIVATE -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME msf_mpeg2ts COMMAND test_msf_mpeg2ts)

    # draft-ietf-moq-msf-02 catalogs beside MSF-01, against the draft's
    # examples.
    add_executable(test_msf02 tests/test_msf02.c)
    target_link_libraries(test_msf02 PRIVATE moq::msf)
    target_compile_definitions(test_msf02 PRIVATE
        MOQ_MSF_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures")
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_msf02 PRIVATE -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME msf02 COMMAND test_msf02)

    # The sized-companion cases use LOC through moq::msf's PUBLIC
    # moq::media-object dependency; do not repeat that static archive here.
    if(TARGET moq::media-object AND TARGET moq::cmaf AND TARGET moq::loc)
        add_executable(test_msf_media tests/test_msf_media.c
            ${PROJECT_SOURCE_DIR}/tests/support/media_builders.c)
        target_include_directories(test_msf_media PRIVATE
            ${PROJECT_SOURCE_DIR}/tests/support)
        target_link_libraries(test_msf_media PRIVATE moq::msf)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_msf_media PRIVATE -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME msf_media COMMAND test_msf_media)
    endif()

    # Structural O(n) proof for apply_delta identity lookups: recompile the
    # library sources with MOQ_MSF_TESTING so the test-only compare counter is present (it is
    # absent from the shipped moq-msf library, which never defines the macro).
    add_executable(test_msf_scaling tests/test_msf_scaling.c ${_MSF_CORE_SOURCES})
    target_compile_definitions(test_msf_scaling PRIVATE MOQ_MSF_TESTING MOQ_BUILDING)
    target_include_directories(test_msf_scaling PRIVATE include)
    target_link_libraries(test_msf_scaling PRIVATE moq::core)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_msf_scaling PRIVATE -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME msf_scaling COMMAND test_msf_scaling)

    # The checked array sizer is hidden in the shipped library, so its boundary
    # is exercised against the sources compiled directly, the same way as above;
    # test_msf keeps linking the real library for everything public.
    add_executable(test_msf_checked_array tests/test_msf_checked_array.c ${_MSF_CORE_SOURCES})
    target_compile_definitions(test_msf_checked_array PRIVATE MOQ_BUILDING)
    target_include_directories(test_msf_checked_array PRIVATE include)
    target_link_libraries(test_msf_checked_array PRIVATE moq::core)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_msf_checked_array PRIVATE -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME msf_checked_array COMMAND test_msf_checked_array)
endif()
