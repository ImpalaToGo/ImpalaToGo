/*
 * @file test-hadoop-fs-adaptive.cc
 *
 * @date   Nov 5, 2014
 * @author elenav
 */

#include <fcntl.h>
#include "dfs_cache/filesystem-mgr.hpp"
#include "dfs_cache/gtest-fixtures.hpp"

namespace impala{

FileSystemDescriptor CacheLayerTest::m_namenode1;
FileSystemDescriptor CacheLayerTest::m_namenodeHdfs;

SessionContext CacheLayerTest::m_ctx1 = nullptr;
SessionContext CacheLayerTest::m_ctx2 = nullptr;

TEST_F(CacheLayerTest, ReadFileFromDigitalOceanHDFS) {
	FileSystemDescriptorBound fsAdaptor(m_namenodeHdfs);
	raiiDfsConnection conn = fsAdaptor.getFreeConnection();
	ASSERT_TRUE(conn.connection() != NULL);

	bool available;

	const char* path = "/test.txt";
	// open remote hdfs fs file:
	dfsFile hfile = fsAdaptor.fileOpen(conn, path, O_RDONLY, 0, 0, 0);
	ASSERT_TRUE(hfile != NULL);

	// open local file:
	dfsFile file = filemgmt::FileSystemManager::instance()->dfsOpenFile(
			m_namenodeHdfs, path, O_CREAT, 0, 0, 0, available);
	ASSERT_TRUE(file != NULL);

#define BUFFER_SIZE 10
	char* buffer = (char*) malloc(sizeof(char) * BUFFER_SIZE);
	ASSERT_TRUE(buffer != NULL);

	// read from the file
	tSize last_read = BUFFER_SIZE;
	for (; last_read == BUFFER_SIZE;) {
		last_read = fsAdaptor.fileRead(conn, hfile, (void*) buffer, last_read);
		filemgmt::FileSystemManager::instance()->dfsWrite(m_namenodeHdfs, file,
				buffer, last_read);
	}

	free(buffer);
	EXPECT_EQ(fsAdaptor.fileClose(conn, hfile), 0);
	filemgmt::FileSystemManager::instance()->dfsCloseFile(m_namenodeHdfs, file);
}

TEST_F(CacheLayerTest, CreateLocalFileDifferentInputPathAlternatives) {

	FileSystemDescriptorBound fsAdaptor(m_namenode1);
	raiiDfsConnection conn = fsAdaptor.getFreeConnection();
	ASSERT_TRUE(conn.connection() != NULL);

	bool available;

	const char* path = "/home/elenav/src/ImpalaToGo/datastorage/local_fs/categoriesSampleLocalFs.csv";
	// open "remote" fs file:
	dfsFile hfile = fsAdaptor.fileOpen(conn, path, O_RDONLY, 0, 0, 0);
	ASSERT_TRUE(hfile != NULL);

	// open local file:
	dfsFile file = filemgmt::FileSystemManager::instance()->dfsOpenFile(
			m_namenode1, path, O_CREAT, 0, 0, 0, available);
	ASSERT_TRUE(file != NULL);

#define BUFFER_SIZE 10
	char* buffer = (char*) malloc(sizeof(char) * BUFFER_SIZE);
	ASSERT_TRUE(buffer != NULL);

	// read from the file
	tSize last_read = BUFFER_SIZE;
	for (; last_read == BUFFER_SIZE;) {
		last_read = fsAdaptor.fileRead(conn, hfile, (void*) buffer, last_read);
		filemgmt::FileSystemManager::instance()->dfsWrite(m_namenode1, file,
				buffer, last_read);
	}

	free(buffer);
	EXPECT_EQ(fsAdaptor.fileClose(conn, hfile), 0);
	filemgmt::FileSystemManager::instance()->dfsCloseFile(m_namenode1, file);
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();

}

