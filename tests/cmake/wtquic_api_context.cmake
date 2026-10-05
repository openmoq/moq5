cmake_minimum_required(VERSION 3.20)
if(NOT CMAKE_OSX_ARCHITECTURES STREQUAL "architecture one;architecture two")
    message(FATAL_ERROR "discovery context lost argument boundaries: '${CMAKE_OSX_ARCHITECTURES}'")
endif()
