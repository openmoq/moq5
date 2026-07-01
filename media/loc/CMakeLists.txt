add_library(moq-loc
    src/loc.c
)

add_library(moq::loc ALIAS moq-loc)

target_include_directories(moq-loc
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(moq-loc PUBLIC moq::core)

target_compile_features(moq-loc PUBLIC c_std_11)

set_target_properties(moq-loc PROPERTIES
    C_VISIBILITY_PRESET hidden
    OUTPUT_NAME moq-loc
    EXPORT_NAME loc
)

install(TARGETS moq-loc
    EXPORT libmoqTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

install(DIRECTORY include/moq
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.h")

target_compile_definitions(moq-loc PRIVATE MOQ_BUILDING)

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq-loc PRIVATE -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()
