add_executable(simple_consumer simple_key_consumer.cpp
        ${SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(simple_consumer pthread ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES} rembrandtlib ${TBB_IMPORTED_TARGETS})
target_include_directories(simple_consumer PUBLIC ../../include)
target_include_directories(simple_consumer PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS})

add_executable(simple_producer simple_key_producer.cpp
        ${SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(simple_producer pthread ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES} rembrandtlib ${TBB_IMPORTED_TARGETS})
target_include_directories(simple_producer PUBLIC ../../include)
target_include_directories(simple_producer PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS})

add_executable(benchmark_storage_node benchmark_storage_node.cpp
        ${SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(benchmark_storage_node pthread ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${JEMALLOC_LIB} ${Boost_LIBRARIES})
target_include_directories(benchmark_storage_node PUBLIC ../include)
target_include_directories(benchmark_storage_node PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS})

add_executable(benchmark_producer benchmark_producer.cpp
        ${SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(benchmark_producer pthread ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES} rembrandtlib)
target_include_directories(benchmark_producer PUBLIC ../include)
target_include_directories(benchmark_producer PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS})

add_executable(benchmark_broker_node ../benchmark/rembrandt/benchmark_broker_node.cpp
        ${SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(benchmark_broker_node pthread ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES})
target_include_directories(benchmark_broker_node PUBLIC ../include)
target_include_directories(benchmark_broker_node PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS})
