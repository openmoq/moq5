# Injected into picotls's own project() call when picoquic builds picotls
# itself (PICOQUIC_FETCH_PTLS; CMAKE_PROJECT_picotls_INCLUDE, set by
# FindPicoquic while picoquic is added).
#
# Whatever the caller had in CMAKE_PROJECT_picotls_INCLUDE runs first, with
# the variable's own semantics: a semicolon list of CMake files or module
# names (found through CMAKE_MODULE_PATH), in order, each in this scope --
# NO_POLICY_SCOPE, so a caller's policy and variable settings land in
# picotls's scope exactly as a direct injection would.
foreach(_moq_ptls_caller_entry IN LISTS MOQ_PICOTLS_CALLER_PROJECT_INCLUDE)
    if(NOT _moq_ptls_caller_entry STREQUAL "")
        include("${_moq_ptls_caller_entry}" NO_POLICY_SCOPE)
    endif()
endforeach()
unset(_moq_ptls_caller_entry)

# LibMoQ's chosen invariant policy for the picotls it links, the same one
# scripts/setup_picoquic_deps.sh applies to a prebuilt picotls: picotls's
# assertions stay enabled in every configuration and its warnings are errors.
# It applies to this directory (picotls) only.
include("${CMAKE_CURRENT_LIST_DIR}/MoqPicotlsBuildPolicy.cmake")

# picoquic's FindPTLS needs PTLS_INCLUDE_DIR in its fetched mbedTLS
# configuration, which it reaches after FetchContent has made picotls
# available; this picotls's headers are selected now, from its own source.
# The selection is checked again against the built targets once picoquic has
# been added.
_moq_pq_ptls_headers_from_source("${CMAKE_CURRENT_SOURCE_DIR}")
