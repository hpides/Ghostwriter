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

    target_include_directories(${TESTNAME} PUBLIC ../include)
    # gtest_discover_tests replaces gtest_add_tests,
    # see https://cmake.org/cmake/help/v3.10/module/GoogleTest.html for more options to pass to it
    gtest_discover_tests(${TESTNAME}
            # set a working directory so your project root so that you can find test data via paths relative to the project root
            WORKING_DIRECTORY ${PROJECT_DIR}
            PROPERTIES VS_DEBUGGER_WORKING_DIRECTORY "${PROJECT_DIR}"
            )
    set_target_properties(${TESTNAME} PROPERTIES FOLDER tests)
endmacro()

package_add_test(BatchTest batch_test.cpp)
package_add_test(ConnectionManagerTest network/connection_manager_test.cpp)
package_add_test(DataGeneratorTest benchmark/data_generator_test.cpp)
package_add_test(MessageAccumulatorTest message_accumulator_test.cpp)
package_add_test(Bla flatbuffer_scratchtest.cpp)
package_add_test(MessageGeneratorTest protocol/message_generator_test.cpp)
package_add_test(RateLimiterTest benchmark/rate_limiter_test.cpp)
package_add_test(SegmentTest storage/segment_test.cpp)
package_add_test(StorageNodeTest storage/storage_node_test.cpp)

#package_add_test(AllTests
#        benchmark/data_generator_test.cpp
#        benchmark/rate_limiter_test.cpp
#        network/connection_manager_test.cpp
#        protocol/message_generator_test.cpp
#        storage/segment_test.cpp
#        storage/storage_node_test.cpp
#        batch_test.cpp)
