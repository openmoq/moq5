add_executable(moq_publisher_example publisher.c)
target_link_libraries(moq_publisher_example PRIVATE moq::adapter-picoquic)

add_executable(moq_subscriber_example subscriber.c)
target_link_libraries(moq_subscriber_example PRIVATE moq::adapter-picoquic)

# Connection-isolation classifier test for the singleton publisher (security
# finding #9). Pure C, no picoquic/network dependency.
if(MOQ_BUILD_TESTS)
    add_executable(test_publisher_conn_guard tests/publisher_conn_guard_test.c)
    add_test(NAME picoquic_publisher_conn_guard COMMAND test_publisher_conn_guard)
endif()
