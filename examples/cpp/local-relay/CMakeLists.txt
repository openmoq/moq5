cmake_minimum_required(VERSION 3.20)
project(local_relay LANGUAGES C CXX)

find_package(libmoq REQUIRED)

add_executable(local_relay main.cpp)
target_link_libraries(local_relay PRIVATE moq::cpp)
