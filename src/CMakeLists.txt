add_compile_options(-pthread -Wall -Wextra -pedantic -Werror -Wno-unused-parameter -Wno-dollar-in-identifier-extension -Wno-unknown-pragmas)
#add_compile_options(-pthread -Wall -Wno-dollar-in-identifier-extension -Wno-unknown-pragmas)

set(TBB_DIR ${TBB_DIR}/cmake)
find_package(TBB REQUIRED tbb)
find_library(UCP_LIB NAMES ucp libucp PATHS ${UCX_INSTALL_PATH}/lib NO_DEFAULT_PATH)
find_library(UCS_LIB NAMES ucs libucs PATHS ${UCX_INSTALL_PATH}/lib NO_DEFAULT_PATH)
find_library(UCT_LIB NAMES uct libuct PATHS ${UCX_INSTALL_PATH}/lib NO_DEFAULT_PATH)
find_path(UCX_INC NAMES ucp/api/ucp.h PATHS ${UCX_INSTALL_PATH}/include NO_DEFAULT_PATH)
message(${UCX_INSTALL_PATH})
message(${UCP_LIB})
message(${UCS_LIB})
message(${UCT_LIB})
message(${UCX_INC})
find_library(libpmemobj++ REQUIRED PATHS /usr/local/include)
set(FLATBUFFERS_SRC_DIR ${PROJECT_SOURCE_DIR}/extern/flatbuffers)
# Add FlatBuffers directly to our build. This defines the `flatbuffers` target.
add_subdirectory(${FLATBUFFERS_SRC_DIR}
        ${CMAKE_CURRENT_BINARY_DIR}/flatbuffers-build
        EXCLUDE_FROM_ALL)
# Now simply link against flatbuffers as needed to your already declared target.
# The flatbuffers target carry header search path automatically if CMake > 2.8.11.
#target_link_libraries(own_project_target PRIVATE flatbuffers)

add_subdirectory(network)

set(REMBRANDT_INC ${PROJECT_SOURCE_DIR}/include/rembrandt)

set(BENCHMARK_SOURCE_FILES
        benchmark/data_generator.cpp
        benchmark/data_processor.cpp
        benchmark/parallel_data_generator.cpp
        benchmark/parallel_data_processor.cpp
        benchmark/rate_limiter.cpp
        benchmark/throughput_throttler.cpp
        logging/latency_logger.cpp
        logging/windowed_latency_logger.cpp
        logging/throughput_logger.cpp)
set(SOURCE_FILES
        broker/broker_node.cpp
        broker/partition.cpp
        broker/logical_segment.cpp
        broker/physical_segment.cpp
        consumer/direct_consumer.cpp
        consumer/receiver.cpp
        network/attached_message.cpp
        network/client.cpp
        network/detached_message.cpp
        network/flat_buffers_message.cpp
        network/connection_manager.cpp
        network/message_handler.cpp
        network/request_processor.cpp
        network/ucx/context.cpp
        network/ucx/endpoint.cpp
        network/ucx/endpoint_factory.cpp
        network/ucx/memory_region.cpp
        network/ucx/worker.cpp
        network/server.cpp
        network/utils.cpp
        producer/batch.cpp
        producer/direct_producer.cpp
        producer/message_accumulator.cpp
        producer/sender.cpp
        protocol/message_generator.cpp
        storage/segment.cpp
        storage/storage_manager.cpp
        storage/storage_node.cpp
        storage/volatile_storage_region.cpp)

#add_library(rembrandtlib STATIC
#        ${SOURCE_FILES}
#        ${HEADER_LIST} ${UCX_INC} ${Boost_INCLUDE_DIRS})

#target_link_libraries(rembrandtlib PRIVATE flatbuffers pmemobj)

find_library(JEMALLOC_LIB jemalloc)
message(${JEMALLOC_LIB})

add_executable(simple_consumer benchmark/simple_key_consumer.cpp
        ${SOURCE_FILES} ${BENCHMARK_SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(simple_consumer pthread ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES} ${TBB_IMPORTED_TARGETS})
target_include_directories(simple_consumer PUBLIC ../include)
target_include_directories(simple_consumer PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS})

add_executable(simple_producer benchmark/simple_key_producer.cpp
        ${SOURCE_FILES} ${BENCHMARK_SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(simple_producer pthread ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES} ${TBB_IMPORTED_TARGETS} hdr_histogram)
target_include_directories(simple_producer PUBLIC ../include)
target_include_directories(simple_producer PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS})

add_executable(benchmark_storage_node benchmark/benchmark_storage_node.cpp storage/persistent_storage_region.cpp
        ${SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(benchmark_storage_node pthread ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${JEMALLOC_LIB} ${Boost_LIBRARIES} ${TBB_IMPORTED_TARGETS} hdr_histogram pmem pmemobj)
target_include_directories(benchmark_storage_node PUBLIC ../include)
target_include_directories(benchmark_storage_node PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS})

add_executable(benchmark_broker_node benchmark/benchmark_broker_node.cpp
        ${SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(benchmark_broker_node pthread ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES})
target_include_directories(benchmark_broker_node PUBLIC ../include)
target_include_directories(benchmark_broker_node PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS})
include_directories(../include)
include_directories(../extern/ucx/install-debug/include)

#add_link_options(-L`jemalloc-config --libdir` -Wl,-rpath,`jemalloc-config --libdir` -ljemalloc `jemalloc-config --libs`)

add_executable(kafka_consumer benchmark/kafka/simple_key_consumer.cpp
        ${SOURCE_FILES} ${BENCHMARK_SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(kafka_consumer pthread ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES} ${TBB_IMPORTED_TARGETS})
target_include_directories(kafka_consumer PUBLIC ../include)
target_include_directories(kafka_consumer PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS})#add_link_options(-L`jemalloc-config --libdir` -Wl,-rpath,`jemalloc-config --libdir` -ljemalloc `jemalloc-config --libs`)

add_executable(kafka_producer benchmark/kafka/simple_key_producer.cpp
        ${SOURCE_FILES} ${BENCHMARK_SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(kafka_producer pthread ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES} ${TBB_IMPORTED_TARGETS})
target_include_directories(kafka_producer PUBLIC ../include)
target_include_directories(kafka_producer PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS})

set(RDKAFKA_BUILD_STATIC ON CACHE BOOL "")
target_link_libraries(kafka_consumer rdkafka++ hdr_histogram)
target_link_libraries(kafka_producer rdkafka++ hdr_histogram)
target_link_libraries(simple_consumer hdr_histogram)
target_link_libraries(simple_producer crypto)
target_link_libraries(simple_producer hdr_histogram)
target_link_libraries(simple_consumer crypto)
