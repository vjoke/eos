add_executable(pubsub_plugin_test
    test.cpp
    fifo_test.cpp
    consumer_test.cpp
    )

target_link_libraries(pubsub_plugin_test
    pubsub_plugin
    ${Boost_LIBRARIES}
    )

#add_test(pubsub_plugin_test pubsub_plugin_test)
