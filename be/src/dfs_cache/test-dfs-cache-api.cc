/*
 * @file test-dfs-cache-api.cc
 * @brief contains tests for Cache layer API
 *
 * @date   Nov 17, 2014
 * @author elenav
 */

#include <string>
#include <gtest/gtest.h>
#include <fcntl.h>

#include "dfs_cache/gtest-fixtures.hpp"

namespace impala{

FileSystemDescriptor CacheLayerTest::m_namenode1;
FileSystemDescriptor CacheLayerTest::m_namenodeHdfs;
FileSystemDescriptor CacheLayerTest::m_namenodeDefault;
FileSystemDescriptor CacheLayerTest::m_namenodelocalFilesystem;

SessionContext CacheLayerTest::m_ctx1 = nullptr;
SessionContext CacheLayerTest::m_ctx2 = nullptr;
SessionContext CacheLayerTest::m_ctx3 = nullptr;
SessionContext CacheLayerTest::m_ctx4 = nullptr;
SessionContext CacheLayerTest::m_ctx5 = nullptr;
SessionContext CacheLayerTest::m_ctx6 = nullptr;

TEST_F(CacheLayerTest, DISABLED_ExplicitHostFlowFileOpenAutoloadTest){
	const char* path = "/test.txt";

	bool available;
	dfsFile file = dfsOpenFile(m_namenodeHdfs, path, O_RDONLY, 0, 0, 0, available);
	ASSERT_TRUE(file != nullptr);
	ASSERT_TRUE(available);
}

TEST_F(CacheLayerTest, DefaultHostFlowFileOpenAutoloadTest){
	const char* path = "/test.txt";

	bool available;
	dfsFile file = dfsOpenFile(m_namenodeDefault, path, O_RDONLY, 0, 0, 0, available);
	ASSERT_TRUE(file != nullptr);
	ASSERT_TRUE(available);
}

}

