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

add_library(rembrandtlib STATIC
        network/socket/static_client.cpp
        network/socket/static_server.cpp
        network/ucx/client.cpp
        network/ucx/endpoint.cpp
        producer/batch.cpp
        producer/message_accumulator.cpp
        producer/sender.cpp
        storage/segment_header.cpp
        storage/segment.cpp
        ${HEADER_LIST}
        )
target_link_libraries(rembrandtlib PRIVATE flatbuffers)

add_executable(client network/ucx/client.cpp
        network/socket/static_client.cpp
        network/ucx/context.cpp
        network/ucx/endpoint.cpp
        network/ucx/memory_region.cpp
        network/ucx/worker.cpp
        network/utils.cpp
        ${HEADER_LIST})
target_link_libraries(client ${UCP_LIB} ${UCS_LIB} ${UCT_LIB})
target_include_directories(client PUBLIC ../include)
target_include_directories(client PRIVATE ${UCX_INC})

add_executable(server network/server.cpp
        network/socket/static_server.cpp
        network/ucx/context.cpp
        network/ucx/endpoint.cpp
        network/ucx/memory_region.cpp
        network/ucx/worker.cpp
        network/utils.cpp
        ${HEADER_LIST})
target_link_libraries(server pthread ${UCP_LIB} ${UCS_LIB} ${UCT_LIB})
target_include_directories(server PUBLIC ../include)
target_include_directories(server PRIVATE ${UCX_INC})

add_executable(static_server network/socket/static_server.cpp ${HEADER_LIST})
add_executable(static_client network/socket/static_client.cpp ${HEADER_LIST})

set(BOOST_ROOT /opt/boost)
find_package(Boost 1.7 REQUIRED)
