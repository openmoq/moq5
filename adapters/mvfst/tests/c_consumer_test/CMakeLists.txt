cmake_minimum_required(VERSION 3.16)
project(moq_c_consumer C)

find_package(libmoq REQUIRED COMPONENTS adapter-mvfst)
add_executable(consumer main.c)
target_link_libraries(consumer PRIVATE moq::adapter-mvfst)
