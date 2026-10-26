include("${MOQ_SOURCE}/cmake/MoqPicotlsBuildPolicy.cmake")
add_library(dependency_probe STATIC "${CMAKE_BINARY_DIR}/probe.c")
file(GENERATE OUTPUT "${CMAKE_BINARY_DIR}/dependency.options"
    CONTENT "$<TARGET_GENEX_EVAL:dependency_probe,$<JOIN:$<TARGET_PROPERTY:dependency_probe,COMPILE_OPTIONS>,;>>")
