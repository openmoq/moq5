cmake_minimum_required(VERSION 3.20)
project(moq_player_consumer_test LANGUAGES C CXX)

# A C++ player consumer of an installed (or build-tree) SDK: public targets
# only -- moq::cpp for the binding, moq::service for the player it wraps.
find_package(libmoq REQUIRED COMPONENTS service)

add_executable(moq_player_consumer_test main.cpp)
target_compile_features(moq_player_consumer_test PRIVATE cxx_std_20)
target_link_libraries(moq_player_consumer_test PRIVATE moq::cpp moq::service)

# The files of the libmoq and picoquic libraries this consumer links, per
# configuration, so an install-tree check can verify where they came from.
set(_linked "")
foreach(_t moq::service moq::core moq::playback moq::media-object moq::msf
        moq::adapter-picoquic moq::adapter-picoquic-threaded
        picoquic::picoquic-core picoquic::picohttp-core)
    if(TARGET ${_t})
        get_target_property(_type ${_t} TYPE)
        if(NOT _type STREQUAL "INTERFACE_LIBRARY")
            string(APPEND _linked "$<TARGET_FILE:${_t}>\n")
        endif()
    endif()
endforeach()
file(GENERATE OUTPUT "${CMAKE_BINARY_DIR}/linked-$<CONFIG>.txt" CONTENT "${_linked}")
