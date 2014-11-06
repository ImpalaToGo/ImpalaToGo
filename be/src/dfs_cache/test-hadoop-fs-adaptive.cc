/*
 * test-hadoop-fs-adaptive.cc
 *
 *  Created on: Nov 5, 2014
 *      Author: elenav
 */

#include <fcntl.h>
#include "dfs_cache/filesystem-mgr.hpp"
#include "dfs_cache/gtest-fixtures.hpp"

namespace impala{

FileSystemDescriptor CacheLayerTest::m_namenode1;
FileSystemDescriptor CacheLayerTest::m_namenodeHdfs;

SessionContext CacheLayerTest::m_ctx1 = nullptr;
SessionContext CacheLayerTest::m_ctx2 = nullptr;

TEST_F(CacheLayerTest, DISABLED_ReadFileFromDigitalOceanHDFS){
	FileSystemDescriptorBound fsAdaptor(m_namenodeHdfs);
	raiiDfsConnection conn = fsAdaptor.getFreeConnection();
	ASSERT_TRUE(conn.connection() != NULL);

	// open file:
	const char* path = "/test/cat.txt";
	dfsFile hfile = fsAdaptor.fileOpen(conn, path, O_RDONLY, 0, 0, 0);
	ASSERT_TRUE(hfile != NULL);

	 // create or open local file:
	bool available;
	dfsFile file = filemgmt::FileSystemManager::instance()->dfsOpenFile(m_namenodeHdfs, path, O_CREAT, 0, 0, 0, available);
	ASSERT_TRUE(file != NULL);
    ASSERT_TRUE(available);

    #define BUFFER_SIZE 10
    char* buffer = (char*)malloc(sizeof(char) * BUFFER_SIZE);
    ASSERT_TRUE(buffer != NULL);

    // read from the file
    tSize last_read = BUFFER_SIZE;
    for (; last_read == BUFFER_SIZE;) {
    	last_read = fsAdaptor.fileRead(conn, hfile, (void*)buffer, last_read);
    	filemgmt::FileSystemManager::instance()->dfsWrite(m_namenodeHdfs, file, buffer, last_read);
    }

    free(buffer);
    EXPECT_EQ(fsAdaptor.fileClose(conn, file), 0);
    filemgmt::FileSystemManager::instance()->dfsCloseFile(m_namenodeHdfs, file);
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();

}

