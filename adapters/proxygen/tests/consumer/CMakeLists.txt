cmake_minimum_required(VERSION 3.20)
project(moq_proxygen_wt_consumer CXX)

# Consume the proxygen WebTransport adapter as a downstream cold consumer:
# only the public component + public header. find_dependency(proxygen
# CONFIG) inside the libmoq config resolves proxygen/folly.
find_package(libmoq REQUIRED COMPONENTS adapter-proxygen-wt)

add_executable(consumer main.cpp)
target_link_libraries(consumer PRIVATE moq::adapter-proxygen-wt)
