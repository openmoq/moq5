set(_MSF_SOURCES src/msf.c)
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
    target_compile_options(moq-msf PRIVATE -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>
        -Wno-unused-parameter)
endif()

if(MOQ_BUILD_TESTS)
    add_executable(test_msf tests/test_msf.c)
    target_link_libraries(test_msf PRIVATE moq::msf)
    target_compile_definitions(test_msf PRIVATE
        MOQ_MSF_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures")
    add_test(NAME msf COMMAND test_msf)

    if(TARGET moq::media-object AND TARGET moq::cmaf)
        add_executable(test_msf_media tests/test_msf_media.c)
        target_link_libraries(test_msf_media PRIVATE moq::msf)
        add_test(NAME msf_media COMMAND test_msf_media)
    endif()
endif()
