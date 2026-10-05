# Injected into picoquic's own project() call when FindPicoquic builds picoquic
# from source (CMAKE_PROJECT_picoquic_INCLUDE). picoquic's CMakeLists declares a
# cmake_minimum_required below the versions that introduced link-line
# de-duplication, so inside its directory scope CMake keeps repeating static
# archives (picoquic-core/log and the picotls archives appear several times on
# its test executables' link lines) and capability-aware linkers warn about
# duplicate libraries. Set the de-duplication policies in that scope when the
# running CMake knows them: NEW lets CMake choose per linker capability
# (traditional linkers keep the repetitions they need), so no dependency edge
# is removed and nothing is stripped by hand.
if(POLICY CMP0156)
    cmake_policy(SET CMP0156 NEW)
endif()
if(POLICY CMP0179)
    cmake_policy(SET CMP0179 NEW)
endif()

# Whatever the caller had in CMAKE_PROJECT_picoquic_INCLUDE still runs here,
# after the policies, with the variable's own semantics: a semicolon list of
# CMake files or module names (found through CMAKE_MODULE_PATH), in order,
# each in this scope -- NO_POLICY_SCOPE, so a caller's policy and variable
# settings land in picoquic's scope exactly as a direct injection would.
foreach(_moq_pq_caller_entry IN LISTS MOQ_PICOQUIC_CALLER_PROJECT_INCLUDE)
    if(NOT _moq_pq_caller_entry STREQUAL "")
        include("${_moq_pq_caller_entry}" NO_POLICY_SCOPE)
    endif()
endforeach()
unset(_moq_pq_caller_entry)
