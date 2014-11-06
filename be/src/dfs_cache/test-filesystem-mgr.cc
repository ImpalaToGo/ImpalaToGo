/**
 * @file test-filesystem-mgr.cc
 * @brief contains miscellaneous tests for file system manager
 *
 * Content:
 * - OpenFileCheckOpened - open the file specified, check it is opened successfully
 *
 * @author elenav
 * @date Oct 29, 2014
 */

#include <string>
#include <gtest/gtest.h>
#include <boost/thread.hpp>

#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>

#include "dfs_cache/filesystem-mgr.hpp"
#include "dfs_cache/cache-mgr.hpp"
#include "dfs_cache/gtest-fixtures.hpp"
#include "dfs_cache/test-utilities.hpp"

namespace ph = std::placeholders;

namespace impala {

FileSystemDescriptor CacheLayerTest::m_namenode1;
FileSystemDescriptor CacheLayerTest::m_namenodeHdfs;

SessionContext CacheLayerTest::m_ctx1 = nullptr;
SessionContext CacheLayerTest::m_ctx2 = nullptr;

TEST_F(CacheLayerTest, OpenFileCheckOpened) {
    const char* path = "/home/elenav/src/ImpalaToGo/be/src/dfs_cache/test_data/hello.txt";
    int flags        =  O_RDONLY;
    int buffer_size  = 0;

    short int replication = 0;
    size_t block_size     = 0;
    bool available;

    dfsFile file = filemgmt::FileSystemManager::instance()->dfsOpenFile(m_namenode1, path, flags, buffer_size, replication, block_size, available);

    EXPECT_TRUE(available);
    EXPECT_FALSE(file == NULL);
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();

}




