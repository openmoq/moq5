cmake_minimum_required(VERSION 3.20)

# A source-tree picoquic makes picoquic-core and picoquic-log mutually
# dependent static archives in this project. Let capable linkers de-duplicate
# CMake's cycle expansion, as the libmoq build itself does.
if(POLICY CMP0156)
    cmake_policy(SET CMP0156 NEW)
endif()

project(moq_pico_wt_managed_verify_consumer_test LANGUAGES C)

# WT managed + the verifier helper: the helper lives in adapter-picoquic,
# which the managed component now depends on transitively. Requesting BOTH
# explicitly is redundant but must still compose — that is what this test
# pins (a consumer that names adapter-picoquic directly keeps working).
find_package(libmoq REQUIRED
    COMPONENTS adapter-pico-wt-managed adapter-picoquic)

add_executable(moq_pico_wt_managed_verify_consumer_test main.c)
target_link_libraries(moq_pico_wt_managed_verify_consumer_test
    PRIVATE moq::adapter-pico-wt-managed moq::adapter-picoquic)
