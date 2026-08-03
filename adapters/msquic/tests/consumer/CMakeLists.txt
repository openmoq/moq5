cmake_minimum_required(VERSION 3.16)
project(moq_msquic_consumer LANGUAGES C)

# Request BOTH components and link BOTH targets. The managed facade ships
# inside the attach library (moq::adapter-msquic-managed is an INTERFACE
# over it), so requesting both must resolve to one library with no
# duplicate or ambiguous symbol ownership. main.c touches an attach and a
# managed symbol to force real resolution of each surface.
find_package(libmoq REQUIRED COMPONENTS adapter-msquic adapter-msquic-managed)

add_executable(moq_msquic_consumer_test main.c)
target_link_libraries(moq_msquic_consumer_test PRIVATE
    moq::adapter-msquic moq::adapter-msquic-managed)
