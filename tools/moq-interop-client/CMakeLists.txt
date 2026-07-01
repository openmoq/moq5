add_executable(moq-interop-client main.c)
# The client drives the sans-I/O core via the service-tier endpoint, which owns
# transport selection (raw QUIC + WebTransport) and version negotiation. Linking
# moq::service transitively provides moq::core (session) plus the picoquic and
# pico_wt adapters for both transports.
target_link_libraries(moq-interop-client PRIVATE moq::service)

# Owned tool: warning baseline (PRIVATE; -Werror gated by the option).
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq-interop-client PRIVATE
        -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()
