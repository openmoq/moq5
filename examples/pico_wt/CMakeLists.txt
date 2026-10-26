# pico WT examples — real picoquic packet-loop demos of MoQ over
# WebTransport via the attach-mode adapter. Build-tree only; not
# installed. No public API; helpers are static in each source.

# Resolve the picoquic core target for the direct picoquic_create /
# picoquic_packet_loop calls (the adapter brings picohttp-core, but the
# examples call core packet-loop APIs themselves).
if(TARGET picoquic::picoquic-core)
    set(_PICO_CORE picoquic::picoquic-core)
elseif(TARGET picoquic-core)
    set(_PICO_CORE picoquic-core)
else()
    message(FATAL_ERROR "picoquic-core target not found for pico_wt examples")
endif()

add_executable(moq_pico_wt_server server.c)
target_link_libraries(moq_pico_wt_server
    PRIVATE moq::adapter-pico-wt ${_PICO_CORE})

add_executable(moq_pico_wt_client client.c)
target_link_libraries(moq_pico_wt_client
    PRIVATE moq::adapter-pico-wt ${_PICO_CORE})

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq_pico_wt_server PRIVATE
        -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    target_compile_options(moq_pico_wt_client PRIVATE
        -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
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
    # The client's failure exits, after each construction step, through its
    # one cleanup path; the sanitizer lanes check that path frees what each
    # step made. No server is needed: the run stops before any packet.
    add_test(NAME pico_wt_example_fail_after_judgement
        COMMAND bash "${CMAKE_CURRENT_SOURCE_DIR}/run_fail_after.sh" --self-test)
    set_tests_properties(pico_wt_example_fail_after_judgement PROPERTIES
        TIMEOUT 60 LABELS "pico_wt;example")
    # An unknown step is refused as usage (status 2), never run.
    add_test(NAME pico_wt_example_client_fail_unknown
        COMMAND bash -c "\"$0\" --fail-after bogus; test $? -eq 2"
                "$<TARGET_FILE:moq_pico_wt_client>")
    set_tests_properties(pico_wt_example_client_fail_unknown PROPERTIES
        TIMEOUT 30 LABELS "pico_wt;example")
    foreach(_step prepare connect start)
        add_test(NAME pico_wt_example_client_fail_${_step}
            COMMAND bash "${CMAKE_CURRENT_SOURCE_DIR}/run_fail_after.sh"
                    "$<TARGET_FILE:moq_pico_wt_client>" ${_step})
        set_tests_properties(pico_wt_example_client_fail_${_step} PROPERTIES
            TIMEOUT 30 LABELS "pico_wt;example")
    endforeach()
endif()
