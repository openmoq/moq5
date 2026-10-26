# cmake -DIN=<path> -P real_path.cmake
# Prints the canonical path of IN -- every symlink resolved, the final file
# component included -- and nothing when it does not resolve to an existing
# file (a dangling link, a missing path). Used by player_consumer_lib.sh.
if(NOT DEFINED IN OR IN STREQUAL "")
    return()
endif()
file(REAL_PATH "${IN}" _resolved)
if(EXISTS "${_resolved}")
    message(NOTICE "${_resolved}")
endif()
