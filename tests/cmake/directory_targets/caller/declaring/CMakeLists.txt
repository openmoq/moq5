# Declares with the caller's option and include directory inherited, after a
# target of its own and a subdirectory already exist.
add_subdirectory(earlier_dir)
add_library(earlier STATIC "${probe}")
target_compile_options(earlier PRIVATE -DEARLIER_OWN)
target_include_directories(earlier PRIVATE /earlier/include)
moq_directory_targets(
    COMPILE_OPTIONS -DDECLARED_A -DDECLARED_B
    COMPILE_DEFINITIONS DECLARED_DEFINITION
    INCLUDE_DIRECTORIES /declared/include)
get_property(declared DIRECTORY PROPERTY MOQ_DIRECTORY_COMPILE_OPTIONS)
file(WRITE "${CMAKE_BINARY_DIR}/declared.txt" "${declared}")
target_include_directories(earlier PRIVATE /earlier/after)

add_library(bare STATIC "${probe}")
add_executable(own "${probe}")
target_compile_options(own PRIVATE -DOWN)
target_include_directories(own PRIVATE /own/include)
add_library(interface_only INTERFACE)
add_subdirectory(later_dir)
add_library(last OBJECT "${probe}")
