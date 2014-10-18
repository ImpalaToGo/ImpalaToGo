message (STATUS "dfs_cache test ${BUILD_OUTPUT_ROOT_DIRECTORY}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/dfs_cache")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/dfs_cache")

#add_executable(test-filesystem-mgr test-filesystem-mgr.cc)

# target_link_libraries(test-filesystem-mgr ${IMPALA_TEST_LINK_LIBS})
include_directories( ../)

ADD_BE_TEST(test-filesystem-mgr)
