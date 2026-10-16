cmake_minimum_required(VERSION 3.20)
project(moq_adapter_threaded_install_consumer_test LANGUAGES C CXX)

# Raw picoquic managed + the verifier helper. The threaded component
# transitively provides moq::adapter-picoquic (the verifier helper), so a
# consumer needs only this one component.
find_package(libmoq REQUIRED COMPONENTS adapter-picoquic-threaded)

add_executable(moq_adapter_threaded_install_consumer_test main.c)
target_link_libraries(moq_adapter_threaded_install_consumer_test
    PRIVATE moq::adapter-picoquic-threaded)
target_compile_features(moq_adapter_threaded_install_consumer_test PRIVATE c_std_11)

add_executable(moq_adapter_threaded_install_consumer_cxx_test main_cxx.cpp)
target_link_libraries(moq_adapter_threaded_install_consumer_cxx_test
    PRIVATE moq::adapter-picoquic-threaded)
target_compile_features(moq_adapter_threaded_install_consumer_cxx_test PRIVATE cxx_std_11)
