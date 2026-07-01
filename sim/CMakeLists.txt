add_library(moq-sim
    src/simpair.c
    src/simpair_fault.c
    src/simpair_delay.c
    src/simpair_pump.c
)

add_library(moq::sim ALIAS moq-sim)

target_include_directories(moq-sim
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(moq-sim PUBLIC moq::core)
target_compile_features(moq-sim PUBLIC c_std_11)

set_target_properties(moq-sim PROPERTIES
    C_VISIBILITY_PRESET hidden
    OUTPUT_NAME moq-sim
    EXPORT_NAME sim
)

install(TARGETS moq-sim
    EXPORT libmoqTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

install(FILES include/moq/sim.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/moq)

target_compile_definitions(moq-sim PRIVATE MOQ_SIM_BUILDING)

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq-sim PRIVATE -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()
