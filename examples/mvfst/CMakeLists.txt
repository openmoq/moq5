enable_language(CXX)

add_executable(moq_mvfst_subscriber managed_subscriber.c)
target_link_libraries(moq_mvfst_subscriber PRIVATE moq::adapter-mvfst)

add_executable(moq_mvfst_server managed_server.c)
target_link_libraries(moq_mvfst_server PRIVATE moq::adapter-mvfst)
