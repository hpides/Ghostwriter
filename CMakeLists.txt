cmake_minimum_required(VERSION 3.13)
project(rembrandt VERSION 0.1)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pthread -lnuma")

find_package(TBB REQUIRED tbb)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

message("Build type: ${CMAKE_BUILD_TYPE}")
if (CMAKE_BUILD_TYPE MATCHES Debug)
    message("BUILDING DEBUG")
else()
    message("BUILDING RELEASE")
    add_compile_options(-O3 -march=native -mtune=native -mavx)
endif()
include(GoogleTest)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

set(Boost_USE_STATIC_LIBS ON)
find_package(Boost COMPONENTS system REQUIRED program_options)
include_directories(${Boost_INCLUDE_DIRS})
enable_testing()
add_subdirectory(extern/librdkafka)
add_subdirectory(extern/hdrhistogram)
add_subdirectory(extern/lightsaber)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

find_library(UCP_LIB NAMES ucp libucp PATHS $ENV{UCX_BUILD_DIR}/lib NO_DEFAULT_PATH)
find_library(UCS_LIB NAMES ucs libucs PATHS $ENV{UCX_BUILD_DIR}/lib NO_DEFAULT_PATH)
find_library(UCT_LIB NAMES uct libuct PATHS $ENV{UCX_BUILD_DIR}/lib NO_DEFAULT_PATH)
find_path(UCX_INC NAMES ucp/api/ucp.h PATHS $ENV{UCX_BUILD_DIR}/include NO_DEFAULT_PATH)
message($ENV{UCX_BUILD_DIR})
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

set(REMBRANDT_INC ${PROJECT_SOURCE_DIR}/include/rembrandt)

set(BENCHMARK_SOURCE_FILES
        src/benchmark/data_generator.cpp
        src/benchmark/data_processor.cpp
        src/benchmark/parallel_data_generator.cpp
        src/benchmark/parallel_data_processor.cpp
        src/benchmark/rate_limiter.cpp
        src/benchmark/throughput_throttler.cpp
        #        benchmark/kafka/data_processor.cpp
        #        benchmark/kafka/parallel_data_processor.cpp
        src/logging/latency_logger.cpp
        src/logging/windowed_latency_logger.cpp
        src/logging/throughput_logger.cpp)

set(YSB_BENCHMARK_SOURCE_FILES
        src/benchmark/rate_limiter.cpp
        src/logging/throughput_logger.cpp
        src/logging/latency_logger.cpp)

set(SOURCE_FILES
        src/broker/broker_node.cpp
        src/broker/partition.cpp
        src/broker/logical_segment.cpp
        src/broker/physical_segment.cpp
        src/consumer/direct_consumer.cpp
        src/consumer/receiver.cpp
        src/network/attached_message.cpp
        src/network/client.cpp
        src/network/detached_message.cpp
        src/network/flat_buffers_message.cpp
        src/network/connection_manager.cpp
        src/network/message_handler.cpp
        src/network/request_processor.cpp
        src/network/ucx/context.cpp
        src/network/ucx/endpoint.cpp
        src/network/ucx/endpoint_factory.cpp
        src/network/ucx/memory_region.cpp
        src/network/ucx/worker.cpp
        src/network/server.cpp
        src/network/utils.cpp
        src/producer/batch.cpp
        src/producer/direct_producer.cpp
        src/producer/message_accumulator.cpp
        src/producer/sender.cpp
        src/protocol/message_generator.cpp
        src/storage/segment.cpp
        src/storage/storage_manager.cpp
        src/storage/storage_node.cpp
        src/storage/volatile_storage_region.cpp)
#
#add_library(rembrandtlib STATIC
#        ${SOURCE_FILES}
##        ${HEADER_LIST} ${UCX_INC} ${Boost_INCLUDE_DIRS})
#
#target_link_libraries(rembrandtlib PRIVATE flatbuffers pmemobj)

find_library(JEMALLOC_LIB jemalloc)
message(${JEMALLOC_LIB})

add_executable(simple_consumer src/benchmark/simple_key_consumer.cpp ${SOURCE_FILES} ${BENCHMARK_SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(simple_consumer  ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES} ${TBB_LIBRARIES})
target_include_directories(simple_consumer PUBLIC include)
target_include_directories(simple_consumer PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS} ${TBB_INCLUDE_DIRS})

#
add_executable(simple_producer src/benchmark/simple_key_producer.cpp
        ${SOURCE_FILES} ${BENCHMARK_SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(simple_producer  ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES} ${TBB_LIBRARIES} hdr_histogram)
target_include_directories(simple_producer PUBLIC include)
target_include_directories(simple_producer PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS} ${TBB_INCLUDE_DIRS})


add_executable(benchmark_storage_node src/benchmark/benchmark_storage_node.cpp src/storage/persistent_storage_region.cpp
        ${SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(benchmark_storage_node  ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${JEMALLOC_LIB} ${Boost_LIBRARIES} ${TBB_LIBRARIES} hdr_histogram pmem pmemobj)
target_include_directories(benchmark_storage_node PUBLIC include)
target_include_directories(benchmark_storage_node PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS} ${TBB_INCLUDE_DIRS})


add_executable(benchmark_broker_node src/benchmark/benchmark_broker_node.cpp
        ${SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(benchmark_broker_node  ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES})
target_include_directories(benchmark_broker_node PUBLIC include)
target_include_directories(benchmark_broker_node PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS} ${TBB_INCLUDE_DIRS})

#include_directories(/opt/rh/devtoolset-9/root/include)
include_directories(include)
include_directories(extern/ucx/install-debug/include)

target_link_libraries(simple_consumer hdr_histogram)
target_link_libraries(simple_producer crypto)
target_link_libraries(simple_producer hdr_histogram)
target_link_libraries(simple_consumer crypto)


add_executable(ysb_ghostwriter_producer src/benchmark/ysb/ghostwriter_producer.cpp
        ${SOURCE_FILES} ${YSB_BENCHMARK_SOURCE_FILES} ${HEADER_LIST})
target_link_libraries(ysb_ghostwriter_producer  ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES} ${TBB_LIBRARIES} crypto hdr_histogram)
target_include_directories(ysb_ghostwriter_producer PUBLIC include)
target_include_directories(ysb_ghostwriter_producer PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS} ${TBB_INCLUDE_DIRS})


add_executable(data_generator src/benchmark/ysb/data_generator.cpp)
find_package(OpenMP)
if(OpenMP_CXX_FOUND)
    target_link_libraries(data_generator PUBLIC OpenMP::OpenMP_CXX)
endif()


add_executable(lightsaber_data_generator src/benchmark/ysb/YahooBenchmark/lightsaber_data_generator.cpp)
find_package(OpenMP)
if(OpenMP_CXX_FOUND)
    target_link_libraries(data_generator PUBLIC OpenMP::OpenMP_CXX)
endif()
FIND_PACKAGE( OpenMP)
if(OPENMP_FOUND)
    message("OPENMP FOUND")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
endif()
include_directories(SYSTEM ${OpenMP_INCLUDE_PATH})

SET(CPP_FILES
        extern/lightsaber/src/dispatcher/TaskDispatcher.cpp
        extern/lightsaber/src/monitors/PerformanceMonitor.cpp
        extern/lightsaber/src/monitors/Measurement.cpp
        extern/lightsaber/src/monitors/LatencyMonitor.cpp
        extern/lightsaber/src/processor/TaskProcessor.cpp
        extern/lightsaber/src/result/ResultHandler.cpp
        extern/lightsaber/src/tasks/NumaTaskQueueWrapper.cpp
        extern/lightsaber/src/tasks/WindowBatch.cpp
        extern/lightsaber/src/tasks/Task.cpp
        extern/lightsaber/src/utils/AttributeType.cpp
        extern/lightsaber/src/utils/Query.cpp
        extern/lightsaber/src/utils/QueryApplication.cpp
        extern/lightsaber/src/utils/Utils.cpp
        extern/lightsaber/src/utils/SystemConf.cpp
        )

SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pthread -lnuma")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -g -Wall -Wextra -DHAVE_NUM -DPREFETCH")

FIND_LIBRARY(tbb NAMES libtbb.so)

# Yahoo Benchmark
add_executable(ysb_ghostwriter_consumer
        src/benchmark/ysb/YahooBenchmark/ghostwriter_consumer.cpp
        ${CPP_FILES}
        ${SOURCE_FILES} ${YSB_BENCHMARK_SOURCE_FILES} ${HEADER_LIST}
        )
include_directories(extern/lightsaber/src)
target_link_options(ysb_ghostwriter_consumer PRIVATE -Wl,--unresolved-symbols=ignore-all)
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    #target_link_libraries(yahoo_benchmark ${Boost_LIBRARIES})
endif ()
target_link_libraries(ysb_ghostwriter_consumer
        operatorJITLib
        boost_fiber_numa
        ${TBB_LIBRARIES}
        ${Boost_LIBRARIES}
        boost_fiber
        pthread dl)
target_include_directories(ysb_ghostwriter_consumer PRIVATE ${TBB_INCLUDE_DIRS})
target_compile_options(ysb_ghostwriter_consumer PRIVATE -Wall -Wextra -O3 -march=native)

target_link_libraries(ysb_ghostwriter_consumer  ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} flatbuffers ${Boost_LIBRARIES} ${TBB_LIBRARIES} crypto hdr_histogram)
target_include_directories(ysb_ghostwriter_consumer PUBLIC include)
target_include_directories(ysb_ghostwriter_consumer PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS} ${TBB_INCLUDE_DIRS})


# Yahoo Benchmark
add_executable(local_ysb_runner
        src/benchmark/ysb/local_ysb_runner.cpp
        ${CPP_FILES}
        ${YSB_BENCHMARK_SOURCE_FILES}
        )
include_directories(extern/lightsaber/src)
target_link_options(local_ysb_runner PRIVATE -Wl,--unresolved-symbols=ignore-all)
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
endif ()
target_link_libraries(local_ysb_runner
        operatorJITLib
        boost_fiber
        ${TBB_LIBRARIES}
        ${Boost_LIBRARIES}
        boost_fiber_numa
        pthread dl)
target_include_directories(local_ysb_runner PRIVATE ${TBB_INCLUDE_DIRS})
target_compile_options(local_ysb_runner PRIVATE -Wall -Wextra -O3 -march=native)

target_link_libraries(local_ysb_runner ${Boost_LIBRARIES} ${TBB_LIBRARIES} crypto hdr_histogram)
target_include_directories(local_ysb_runner PRIVATE ${UCX_INC} ${Boost_INCLUDE_DIRS} ${TBB_INCLUDE_DIRS})



add_subdirectory("${PROJECT_SOURCE_DIR}/extern/googletest" "extern/googletest")

find_library(UCP_LIB NAMES ucp libucp PATHS $ENV{UCX_BUILD_DIR}/lib NO_DEFAULT_PATH)
find_library(UCS_LIB NAMES ucs libucs PATHS $ENV{UCX_BUILD_DIR}/lib NO_DEFAULT_PATH)
find_library(UCT_LIB NAMES uct libuct PATHS $ENV{UCX_BUILD_DIR}/lib NO_DEFAULT_PATH)
find_path(UCX_INC NAMES ucp/api/ucp.h PATHS $ENV{UCX_BUILD_DIR}/include NO_DEFAULT_PATH)
include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR} ${UCX_INC})

find_library(UCP_LIB NAMES ucp libucp PATHS $ENV{UCX_BUILD_DIR}/lib NO_DEFAULT_PATH)
find_package(Boost REQUIRED)

include_directories(${Boost_INCLUDE_DIRS})
macro(package_add_test TESTNAME)
    # create an exectuable in which the tests will be stored
    add_executable(${TESTNAME} ${ARGN})
    # link the Google test infrastructure, mocking library, and a default main fuction to
    # the test executable.  Remove g_test_main if writing your own main function.
    target_link_libraries(${TESTNAME} gtest gmock gtest_main rembrandtlib flatbuffers ${UCP_LIB} ${UCS_LIB} ${UCT_LIB} ${Boost_LIBRARIES} ${TBB_LIBRARIES})

    target_include_directories(${TESTNAME} PUBLIC include)
    # gtest_discover_tests replaces gtest_add_tests,
    # see https://cmake.org/cmake/help/v3.10/module/GoogleTest.html for more options to pass to it
    gtest_discover_tests(${TESTNAME}
            # set a working directory so your project root so that you can find test data via paths relative to the project root
            WORKING_DIRECTORY ${PROJECT_DIR}
            PROPERTIES VS_DEBUGGER_WORKING_DIRECTORY "${PROJECT_DIR}"
            )
    set_target_properties(${TESTNAME} PROPERTIES FOLDER tests)
endmacro()
#
#package_add_test(BatchTest tests/batch_test.cpp)
#package_add_test(ConnectionManagerTest tests/network/connection_manager_test.cpp)
#package_add_test(DataGeneratorTest tests/benchmark/data_generator_test.cpp)
#package_add_test(MessageAccumulatorTest tests/message_accumulator_test.cpp)
#package_add_test(Bla tests/flatbuffer_scratchtest.cpp)
#package_add_test(MessageGeneratorTest tests/protocol/message_generator_test.cpp)
#package_add_test(RateLimiterTest tests/benchmark/rate_limiter_test.cpp)
#package_add_test(SegmentTest tests/storage/segment_test.cpp)
#package_add_test(StorageNodeTest tests/storage/storage_node_test.cpp)
