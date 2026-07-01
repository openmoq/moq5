cmake_minimum_required(VERSION 3.20)
project(moq_consumer_test LANGUAGES C CXX)

find_package(libmoq REQUIRED)

add_executable(moq_consumer_test main.cpp)
target_link_libraries(moq_consumer_test PRIVATE moq::cpp)
