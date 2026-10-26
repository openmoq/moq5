# Stand-in for a picoquic source tree whose targets link OpenSSL the way
# picoquic's do: as the raw artifact paths of ${OPENSSL_LIBRARIES}, on
# picoquic-core and on picohttp-core.
cmake_minimum_required(VERSION 3.20)
project(picoquic C)
find_package(OpenSSL REQUIRED)
add_library(picoquic-core INTERFACE)
target_link_libraries(picoquic-core INTERFACE ${OPENSSL_LIBRARIES})
add_library(picohttp-core INTERFACE)
target_link_libraries(picohttp-core INTERFACE picoquic-core ${OPENSSL_LIBRARIES})
