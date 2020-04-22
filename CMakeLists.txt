cmake_minimum_required(VERSION 3.13)
project(rembrandt VERSION 0.1)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
include(GoogleTest)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

set(TBB_DIR ${TBB_DIR}/cmake)
find_package(TBB REQUIRED tbb)

find_package(Boost REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})
add_subdirectory(src)
enable_testing()
add_subdirectory(tests)
add_subdirectory(benchmark)
add_subdirectory(extern/librdkafka)
