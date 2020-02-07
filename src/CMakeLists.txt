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
        network/socket/static_client.cpp
        network/socket/static_server.cpp
        network/ucx/context.cpp
        network/ucx/client.cpp
        network/ucx/endpoint.cpp
        network/ucx/memory_region.cpp
        network/ucx/worker.cpp
        network/server.cpp
        network/utils.cpp
        producer/batch.cpp
        producer/message_accumulator.cpp
        producer/producer.cpp
        producer/sender.cpp
        storage/segment_header.cpp
        storage/segment.cpp
        storage/storage_node.cpp)

add_library(rembrandtlib STATIC
        ${SOURCE_FILES}
        ${HEADER_LIST})

target_link_libraries(rembrandtlib PRIVATE flatbuffers)

#add_executable(client network/ucx/client.cpp
#        network/socket/static_client.cpp
#        network/ucx/context.cpp
#        network/ucx/endpoint.cpp
#        network/ucx/memory_region.cpp
#        network/ucx/worker.cpp
#        network/utils.cpp
#        ${HEADER_LIST})
#
#target_link_libraries(client ${UCP_LIB} ${UCS_LIB} ${UCT_LIB})
#target_include_directories(client PUBLIC ../include)
#target_include_directories(client PRIVATE ${UCX_INC})
#
#add_executable(server network/server.cpp
#        network/socket/static_server.cpp
#        network/ucx/context.cpp
#        network/ucx/endpoint.cpp
#        network/ucx/memory_region.cpp
#        network/ucx/worker.cpp
#        network/utils.cpp
#        ${HEADER_LIST})
#target_link_libraries(server pthread ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} Boost::functional)
#target_include_directories(server PUBLIC ../include)
#target_include_directories(server PRIVATE ${UCX_INC})

#add_executable(static_server network/socket/static_server.cpp ${HEADER_LIST})
#add_executable(static_client network/socket/static_client.cpp ${HEADER_LIST})

add_executable(consumer consumer/consumer.cpp ${SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(consumer pthread ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES})
target_include_directories(consumer PUBLIC ../include)
target_include_directories(consumer PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS})

add_executable(producer producer/producer.cpp ${SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(producer pthread ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES})
target_include_directories(producer PUBLIC ../include)
target_include_directories(producer PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS})

add_executable(storage_node storage/storage_node.cpp
        ${SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(storage_node pthread ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES})
target_include_directories(storage_node PUBLIC ../include)
target_include_directories(storage_node PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS})


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