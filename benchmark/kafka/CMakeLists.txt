add_executable(kafka_producer simple_key_producer.cpp
        ${SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(kafka_producer pthread ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES} rembrandtlib ${TBB_IMPORTED_TARGETS})
target_include_directories(kafka_producer PUBLIC ../../include)
target_include_directories(kafka_producer PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS})

set(RDKAFKA_BUILD_STATIC ON CACHE BOOL "")
target_link_libraries(kafka_producer rdkafka)
