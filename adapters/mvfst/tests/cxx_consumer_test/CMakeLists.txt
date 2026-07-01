cmake_minimum_required(VERSION 3.16)
project(moq_cxx_consumer CXX)

find_package(libmoq REQUIRED COMPONENTS adapter-mvfst)
add_executable(consumer main.cpp)
target_link_libraries(consumer PRIVATE moq::adapter-mvfst)
