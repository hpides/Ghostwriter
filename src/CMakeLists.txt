add_compile_options(-pthread -Wall -Wextra -pedantic -Werror -Wno-unused-parameter -Wno-dollar-in-identifier-extension -Wno-unknown-pragmas)

find_library(UCP_LIB NAMES ucp libucp PATHS ${UCX_INSTALL_PATH}/lib NO_DEFAULT_PATH)
find_library(UCS_LIB NAMES ucs libucs PATHS ${UCX_INSTALL_PATH}/lib NO_DEFAULT_PATH)
find_library(UCT_LIB NAMES uct libuct PATHS ${UCX_INSTALL_PATH}/lib NO_DEFAULT_PATH)
find_path(UCX_INC NAMES ucp/api/ucp.h PATHS ${UCX_INSTALL_PATH}/include NO_DEFAULT_PATH)

message(${UCX_INSTALL_PATH})
message(${UCP_LIB})
message(${UCS_LIB})
message(${UCT_LIB})
message(${UCX_INC})

add_subdirectory(network)

set(REMBRANDT_INC ${PROJECT_SOURCE_DIR}/include/rembrandt)

add_library(rembrandtlib STATIC
        network/socket/static_client.cpp
        network/socket/static_server.cpp
        producer/batch.cpp
        producer/message_accumulator.cpp
        producer/sender.cpp
        storage/segment_header.cpp
        storage/segment.cpp
        ${HEADER_LIST}
        )

add_executable(client network/client.cpp
        network/socket/static_client.cpp
        network/context.cpp
        network/memory_region.cpp
        network/worker.cpp
        network/utils.cpp
        ${HEADER_LIST})
target_link_libraries(client ${UCP_LIB} ${UCS_LIB} ${UCT_LIB})
target_include_directories(client PUBLIC ../include)
target_include_directories(client PRIVATE ${UCX_INC})

add_executable(server network/server.cpp
        network/socket/static_server.cpp
        network/context.cpp
        network/memory_region.cpp
        network/worker.cpp
        network/utils.cpp
        ${HEADER_LIST})
target_link_libraries(server pthread ${UCP_LIB} ${UCS_LIB} ${UCT_LIB})
target_include_directories(server PUBLIC ../include)
target_include_directories(server PRIVATE ${UCX_INC})

add_executable(static_server network/socket/static_server.cpp ${HEADER_LIST})
add_executable(static_client network/socket/static_client.cpp ${HEADER_LIST})

set(panda_sources
        broker_node.cpp
        consumer.cpp
        producer/producer.cpp
        storage_node.cpp)
add_library(panda ${panda_sources})

set(BOOST_ROOT /opt/boost)
find_package(Boost 1.7 REQUIRED
        )