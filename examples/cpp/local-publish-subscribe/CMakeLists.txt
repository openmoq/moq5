cmake_minimum_required(VERSION 3.20)
project(local_publish_subscribe LANGUAGES C CXX)

find_package(libmoq REQUIRED)

add_executable(local_publish_subscribe main.cpp)
target_link_libraries(local_publish_subscribe PRIVATE moq::cpp)
