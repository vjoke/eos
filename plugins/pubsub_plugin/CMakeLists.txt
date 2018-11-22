#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall -g")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -std=gnu++0x")

find_package(RdKafka REQUIRED ${FIND_PACKAGE_QUIET})
find_package(Zookeeper REQUIRED ${FIND_PACKAGE_QUIET})
find_package(JANSSON REQUIRED ${FIND_PACKAGE_QUIET})

if (RDKAFKA_FOUND)
    include_directories(${RDKAFKA_INCLUDE_DIR})
    set(EXTRA_LIBS ${EXTRA_LIBS} ${RDKAFKA_LIBRARY} ${RDKAFKA_LIBRARY_CPLUSPLUS})
    message(STATUS "RdKafka include dir: ${RDKAFKA_INCLUDE_DIR}")
    message(STATUS "RdKafka library: ${RDKAFKA_LIBRARY}")
    message(STATUS "RdKafka c++ library: ${RDKAFKA_LIBRARY_CPLUSPLUS}")
endif()

if (Zookeeper_FOUND) 
    include_directories(${Zookeeper_INCLUDE_DIR})
    set(EXTRA_LIBS ${EXTRA_LIBS} ${Zookeeper_LIBRARY})
    ADD_DEFINITIONS(-DTHREADED)
    message(STATUS "Zookeeper include dir: ${Zookeeper_INCLUDE_DIR}")
    message(STATUS "Zookeeper library: ${Zookeeper_LIBRARY}")
endif()

if (JANSSON_FOUND) 
    include_directories(${JANSSON_INCLUDE_DIRS})
    set(EXTRA_LIBS ${EXTRA_LIBS} ${JANSSON_LIBRARIES})
    message(STATUS "JANSSON include dir: ${Zookeeper_INCLUDE_DIR}")
    message(STATUS "JANSSON library: ${JANSSON_LIBRARIES}")
endif()

include_directories(${CMAKE_CURRENT_SOURCE_DIR} include be)

add_library(pubsub_plugin
    be/backend.cpp
    pubsub_plugin.cpp
    applied_message.cpp
    )
    
target_link_libraries(pubsub_plugin
    http_plugin
    chain_plugin 
    ${EXTRA_LIBS}
)

add_subdirectory(test)

