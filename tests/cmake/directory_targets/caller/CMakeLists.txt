# What a caller gives its directory before LibMoQ's own is added beneath it.
set_property(DIRECTORY APPEND PROPERTY COMPILE_OPTIONS -DCALLER)
set_property(DIRECTORY APPEND PROPERTY INCLUDE_DIRECTORIES /caller/include)
add_subdirectory(declaring)
