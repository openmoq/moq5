cmake_minimum_required(VERSION 3.20)
project(moq_incompatible_provider_consumer LANGUAGES C)

# A picoquic target is ALREADY DEFINED before find_package(libmoq), and it is
# deliberately incompatible with the artifact being consumed: its interface
# names the OTHER provider's picotls backend. A LibMoQ package that recorded
# the provider it was built with would refuse this with an actionable
# diagnostic; today nothing checks, so the configure is expected to succeed.
add_library(moq_incompatible_stub INTERFACE)
# The marker is a comma-separated ATOM SET: a provider is proven by a complete
# signature, so a single generic library is deliberately not enough.
string(REPLACE "," ";" _moq_marker_atoms "${MOQ_INCOMPATIBLE_MARKER}")
target_link_libraries(moq_incompatible_stub INTERFACE ${_moq_marker_atoms})

add_library(picoquic::picoquic-core ALIAS moq_incompatible_stub)
add_library(picoquic::picoquic-log ALIAS moq_incompatible_stub)

find_package(libmoq REQUIRED COMPONENTS adapter-picoquic)

# Reaching here at all is the RED: the incompatible target was accepted.
message(STATUS "MOQ_INCOMPATIBLE_ACCEPTED")
