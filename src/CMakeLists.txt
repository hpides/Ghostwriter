#add_compile_options(-pthread -Wall -Wextra -pedantic -Werror -Wno-unused-parameter -Wno-dollar-in-identifier-extension -Wno-unknown-pragmas)
#add_compile_options(-pthread -Wall -Wno-dollar-in-identifier-extension -Wno-unknown-pragmas)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

#set(TBB_DIR ${TBB_DIR}/cmake)
#find_package(TBB REQUIRED tbb)
find_library(UCP_LIB NAMES ucp libucp PATHS $ENV{UCX_BUILD_DIR}/lib NO_DEFAULT_PATH)
find_library(UCS_LIB NAMES ucs libucs PATHS $ENV{UCX_BUILD_DIR}/lib NO_DEFAULT_PATH)
find_library(UCT_LIB NAMES uct libuct PATHS $ENV{UCX_BUILD_DIR}/lib NO_DEFAULT_PATH)
find_path(UCX_INC NAMES ucp/api/ucp.h PATHS $ENV{UCX_BUILD_DIR}/include NO_DEFAULT_PATH)
message($ENV{UCX_BUILD_DIR})
message(${UCP_LIB})
message(${UCS_LIB})
message(${UCT_LIB})
message(${UCX_INC})
#
#find_library(libpmemobj REQUIRED PATHS /opt/rh/devtoolset-9/root/include)
#
#if (LIBPMEMOBJ_FOUND)
#    include_directories(${LIBPMEMOBJ_INCLUDE_DIRS})
#    message(STATUS "libpmemobj include: ${LIBPMEMOBJ_INCLUDE_DIRS}")
#endif()

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
#        benchmark/kafka/data_processor.cpp
#        benchmark/kafka/parallel_data_processor.cpp
        logging/latency_logger.cpp
        logging/windowed_latency_logger.cpp
        logging/throughput_logger.cpp)

set(YSB_BENCHMARK_SOURCE_FILES
        benchmark/rate_limiter.cpp
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
target_link_libraries(simple_consumer  ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES} ${TBB_LIBRARIES})
target_include_directories(simple_consumer PUBLIC ../include)
target_include_directories(simple_consumer PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS} ${TBB_INCLUDE_DIRS})


add_executable(simple_producer benchmark/simple_key_producer.cpp
        ${SOURCE_FILES} ${BENCHMARK_SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(simple_producer  ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES} ${TBB_LIBRARIES} hdr_histogram)
target_include_directories(simple_producer PUBLIC ../include)
target_include_directories(simple_producer PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS} ${TBB_INCLUDE_DIRS})


add_executable(benchmark_storage_node benchmark/benchmark_storage_node.cpp storage/persistent_storage_region.cpp
        ${SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(benchmark_storage_node  ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${JEMALLOC_LIB} ${Boost_LIBRARIES} ${TBB_LIBRARIES} hdr_histogram pmem pmemobj)
target_include_directories(benchmark_storage_node PUBLIC ../include)
target_include_directories(benchmark_storage_node PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS} ${TBB_INCLUDE_DIRS})


add_executable(benchmark_broker_node benchmark/benchmark_broker_node.cpp
        ${SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(benchmark_broker_node  ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES})
target_include_directories(benchmark_broker_node PUBLIC ../include)
target_include_directories(benchmark_broker_node PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS} ${TBB_INCLUDE_DIRS})

include_directories(/opt/rh/devtoolset-9/root/include)
include_directories(../include)
include_directories(../extern/ucx/install-debug/include)

#add_link_options(-L`jemalloc-config --libdir` -Wl,-rpath,`jemalloc-config --libdir` -ljemalloc `jemalloc-config --libs`)

#add_executable(kafka_consumer benchmark/kafka/simple_key_consumer.cpp
#        ${SOURCE_FILES} ${BENCHMARK_SOURCE_FILES} ${HEADER_LIST})
#target_link_libraries(kafka_consumer  ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES} ${TBB_LIBRARIES})
#target_include_directories(kafka_consumer PUBLIC ../include)
#target_include_directories(kafka_consumer PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS} ${TBB_INCLUDE_DIRS})
##add_link_options(-L`jemalloc-config --libdir` -Wl,-rpath,`jemalloc-config --libdir` -ljemalloc `jemalloc-config --libs`)
#
#add_executable(kafka_producer benchmark/kafka/simple_key_producer.cpp
#        ${SOURCE_FILES} ${BENCHMARK_SOURCE_FILES} ${HEADER_LIST})
#target_link_libraries(kafka_producer  ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES} ${TBB_LIBRARIES})
#target_include_directories(kafka_producer PUBLIC ../include)
#target_include_directories(kafka_producer PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS} ${TBB_INCLUDE_DIRS})
#
#set(RDKAFKA_BUILD_STATIC ON CACHE BOOL "")
#target_link_libraries(kafka_consumer rdkafka++ hdr_histogram)
#target_link_libraries(kafka_producer rdkafka++ hdr_histogram)
target_link_libraries(simple_consumer hdr_histogram)
target_link_libraries(simple_producer crypto)
target_link_libraries(simple_producer hdr_histogram)
target_link_libraries(simple_consumer crypto)


add_executable(ysb_ghostwriter_producer benchmark/ysb/ghostwriter_producer.cpp
        ${SOURCE_FILES} ${YSB_BENCHMARK_SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(ysb_ghostwriter_producer  ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES} ${TBB_LIBRARIES} crypto hdr_histogram)
target_include_directories(ysb_ghostwriter_producer PUBLIC ../include)
target_include_directories(ysb_ghostwriter_producer PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS} ${TBB_INCLUDE_DIRS})


add_executable(data_generator benchmark/ysb/data_generator.cpp)
find_package(OpenMP)
if(OpenMP_CXX_FOUND)
    target_link_libraries(data_generator PUBLIC OpenMP::OpenMP_CXX)
endif()



#
#find_package(GTest REQUIRED)
#include_directories(${GTEST_INCLUDE_DIRS})

# Configure CCache if available
find_program(CCACHE_PROGRAM ccache)
if (CCACHE_PROGRAM)
    message("Using CCache...")
    #set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    #set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
    set(CMAKE_CXX_COMPILER_LAUNCHER ${CCACHE_PROGRAM})
    set(CMAKE_C_COMPILER_LAUNCHER ${CCACHE_PROGRAM})
    set(CMAKE_CXX_COMPILER "/usr/lib/ccache/clang++")
endif ()

SET(CPP_FILES
        ../extern/lightsaber/src/dispatcher/TaskDispatcher.cpp
        ../extern/lightsaber/src/monitors/PerformanceMonitor.cpp
        ../extern/lightsaber/src/monitors/Measurement.cpp
        ../extern/lightsaber/src/monitors/LatencyMonitor.cpp
        ../extern/lightsaber/src/processor/TaskProcessor.cpp
        ../extern/lightsaber/src/result/ResultHandler.cpp
        ../extern/lightsaber/src/tasks/NumaTaskQueueWrapper.cpp
        ../extern/lightsaber/src/tasks/WindowBatch.cpp
        ../extern/lightsaber/src/tasks/Task.cpp
        ../extern/lightsaber/src/utils/AttributeType.cpp
        ../extern/lightsaber/src/utils/Query.cpp
        ../extern/lightsaber/src/utils/QueryApplication.cpp
        ../extern/lightsaber/src/utils/Utils.cpp
        ../extern/lightsaber/src/utils/SystemConf.cpp
        )

SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pthread -lnuma")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -g -Wall -Wextra -DHAVE_NUM -DPREFETCH")

#find_package(benchmark REQUIRED)
#include_directories(${benchmark_INCLUDE_DIRS})

FIND_LIBRARY(tbb NAMES libtbb.so)
# Yahoo Benchmark
add_executable(gw_ysb
        benchmark/ysb/YahooBenchmark/ghostwriter_consumer.cpp
#        benchmark/ysb/YahooBenchmark/GhostwriterYSB.cpp
        ${CPP_FILES}
        ${SOURCE_FILES} ${YSB_BENCHMARK_SOURCE_FILES} ${HEADER_LIST}
        )
include_directories(../extern/lightsaber/src)
target_link_options(gw_ysb PRIVATE -Wl,--unresolved-symbols=ignore-all)
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    #target_link_libraries(yahoo_benchmark ${Boost_LIBRARIES})
endif ()
target_link_libraries(gw_ysb
        operatorJITLib
        boost_fiber
        ${TBB_LIBRARIES}
        ${Boost_LIBRARIES}
        /boost_1_68_0/stage/lib/libboost_fiber_numa.so
        pthread dl)
target_include_directories(gw_ysb PRIVATE ${TBB_INCLUDE_DIRS})
target_compile_options(gw_ysb PRIVATE -Wall -Wextra -O3 -march=native)

target_link_libraries(gw_ysb  ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES} ${TBB_LIBRARIES} crypto hdr_histogram)
target_include_directories(gw_ysb PUBLIC ../include)
target_include_directories(gw_ysb PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS} ${TBB_INCLUDE_DIRS})
