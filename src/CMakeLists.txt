#add_compile_options(-pthread -Wall -Wextra -pedantic -Werror -Wno-unused-parameter -Wno-dollar-in-identifier-extension -Wno-unknown-pragmas)
add_compile_options(-pthread -Wall -Wno-dollar-in-identifier-extension -Wno-unknown-pragmas)

find_library(UCP_LIB NAMES ucp libucp PATHS ${UCX_INSTALL_PATH}/lib NO_DEFAULT_PATH)
find_library(UCS_LIB NAMES ucs libucs PATHS ${UCX_INSTALL_PATH}/lib NO_DEFAULT_PATH)
find_library(UCT_LIB NAMES uct libuct PATHS ${UCX_INSTALL_PATH}/lib NO_DEFAULT_PATH)
find_path(UCX_INC NAMES ucp/api/ucp.h PATHS ${UCX_INSTALL_PATH}/include NO_DEFAULT_PATH)
find_package(Boost REQUIRED)
message(${UCX_INSTALL_PATH})
message(${UCP_LIB})
message(${UCS_LIB})
message(${UCT_LIB})
message(${UCX_INC})


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

set(SOURCE_FILES
        broker/broker_node.cpp
        broker/segment_info.cpp
        consumer/consumer.cpp
        consumer/receiver.cpp
        network/basic_message.cpp
        network/flat_buffers_message.cpp
        network/connection_manager.cpp
        network/request_processor.cpp
        network/socket/static_client.cpp
        network/socket/static_server.cpp
        network/ucx/context.cpp
        network/ucx/endpoint.cpp
        network/ucx/endpoint_factory.cpp
        network/ucx/memory_region.cpp
        network/ucx/worker.cpp
        network/server.cpp
        network/utils.cpp
        producer/batch.cpp
        producer/message_accumulator.cpp
        producer/producer.cpp
        producer/sender.cpp
        protocol/message_generator.cpp
        storage/r_key_server.cpp
        storage/segment_header.cpp
        storage/segment.cpp
        storage/storage_node.cpp)

add_library(rembrandtlib STATIC
        ${SOURCE_FILES}
        ${HEADER_LIST} ${UCX_INC} ${Boost_INCLUDE_DIRS})

target_link_libraries(rembrandtlib PRIVATE flatbuffers)

add_executable(consumer consumer/consumer.cpp ${SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(consumer pthread ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES})
target_include_directories(consumer PUBLIC ../include)
target_include_directories(consumer PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS})

add_executable(benchmark_broker_node benchmark/benchmark_broker_node.cpp
        ${SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(benchmark_broker_node pthread ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES})
target_include_directories(benchmark_broker_node PUBLIC ../include)
target_include_directories(benchmark_broker_node PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS})

add_executable(benchmark_storage_node benchmark/benchmark_storage_node.cpp
        ${SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(benchmark_storage_node pthread ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES})
target_include_directories(benchmark_storage_node PUBLIC ../include)
target_include_directories(benchmark_storage_node PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS})

add_executable(benchmark_producer benchmark/benchmark_producer.cpp
        ${SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(benchmark_producer pthread ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES} rembrandtlib)
target_include_directories(benchmark_producer PUBLIC ../include)
target_include_directories(benchmark_producer PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS})
include_directories(../include)
include_directories(../extern/ucx/install-debug/include)
include_directories(${Boost_INCLUDE_DIRS})