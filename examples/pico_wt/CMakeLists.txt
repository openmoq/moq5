# pico WT examples — real picoquic packet-loop demos of MoQ over
# WebTransport via the attach-mode adapter. Build-tree only; not
# installed. No public API; helpers are static in each source.

# These examples call picoquic core APIs (picoquic_create, packet loop)
# directly, and they get picoquic-core -- headers and archive -- through the
# adapter's PUBLIC picohttp-core edge, since upstream picohttp-core itself
# links picoquic-core PUBLIC (picoquic/CMakeLists.txt: target_link_libraries
# (picohttp-core ... PUBLIC picoquic-core picoquic-log)). Naming picoquic-core
# again here adds a SECOND link-line occurrence at a different position, which
# the linker reports as a duplicate library; the closure is already complete
# without it. Build-tree only, so the adapter's BUILD_INTERFACE edge is the
# one that applies.
add_executable(moq_pico_wt_server server.c)
target_link_libraries(moq_pico_wt_server PRIVATE moq::adapter-pico-wt)

add_executable(moq_pico_wt_client client.c)
target_link_libraries(moq_pico_wt_client PRIVATE moq::adapter-pico-wt)

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq_pico_wt_server PRIVATE
        -Wall -Wextra -Wpedantic -Wno-unused-parameter)
    target_compile_options(moq_pico_wt_client PRIVATE
        -Wall -Wextra -Wpedantic -Wno-unused-parameter)
endif()

# -- Localhost smoke -------------------------------------------------
# Runs the real server+client over loopback UDP and asserts end-to-end
# object delivery. Uses picoquic's test cert/key. Labeled "network" so
# UDP-restricted environments can exclude it: ctest -LE network.
if(MOQ_BUILD_TESTS AND MOQ_PICOQUIC_SOURCE_DIR AND
   EXISTS "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem")
    add_test(NAME pico_wt_example_smoke
        COMMAND bash
            "${CMAKE_CURRENT_SOURCE_DIR}/run_smoke.sh"
            "$<TARGET_FILE:moq_pico_wt_server>"
            "$<TARGET_FILE:moq_pico_wt_client>"
            "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
            "${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem"
            14450)
    set_tests_properties(pico_wt_example_smoke PROPERTIES
        TIMEOUT 30
        LABELS "network;pico_wt;example")
endif()
