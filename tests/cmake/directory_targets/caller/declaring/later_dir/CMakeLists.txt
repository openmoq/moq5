# Added after the declaration, with options of its own directory and target.
set_property(DIRECTORY APPEND PROPERTY COMPILE_OPTIONS -DLATER_DIR)
add_library(later_sub STATIC "${probe}")
target_compile_options(later_sub PRIVATE -DLATER_OWN)
