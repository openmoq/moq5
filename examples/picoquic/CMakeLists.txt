add_executable(moq_publisher_example publisher.c)
target_link_libraries(moq_publisher_example PRIVATE moq::adapter-picoquic)

add_executable(moq_subscriber_example subscriber.c)
target_link_libraries(moq_subscriber_example PRIVATE moq::adapter-picoquic)
