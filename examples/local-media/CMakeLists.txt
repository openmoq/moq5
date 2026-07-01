add_executable(moq_demo_publisher publisher/main.c)
target_link_libraries(moq_demo_publisher PRIVATE moq::adapter-picoquic moq::loc moq::msf moq::cmaf)

add_executable(moq_demo_relay relay/main.c)
target_link_libraries(moq_demo_relay PRIVATE moq::adapter-picoquic)

add_executable(moq_demo_client client/main.c)
target_link_libraries(moq_demo_client PRIVATE moq::adapter-picoquic moq::msf moq::playback)
