cmake_minimum_required(VERSION 3.20)
project(moq_adapter_consumer_test LANGUAGES C)

find_package(libmoq REQUIRED COMPONENTS adapter-picoquic)

add_executable(moq_adapter_consumer_test main.c)
target_link_libraries(moq_adapter_consumer_test PRIVATE moq::adapter-picoquic)
