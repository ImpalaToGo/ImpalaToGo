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

TEST_F(CacheLayerTest, ExplicitHostFlowFileOpenCloseAutoloadTestFileDoesNotExist){
	const char* path = "/test.txt";

	bool available;
	dfsFile file = dfsOpenFile(m_namenodeHdfs, path, O_RDONLY, 0, 0, 0, available);
	ASSERT_TRUE(file != nullptr);
	ASSERT_TRUE(available);
	status::StatusInternal status = dfsCloseFile(m_namenodeHdfs, file);
	ASSERT_TRUE(status == status::StatusInternal::OK);

	// cleanup:
	dfsDelete(m_namenodeHdfs, path, true);
}

TEST_F(CacheLayerTest, TwoClientsRequestSameFileForOpenWhichIsNotExistsInitially){
	const char* path = "/test.txt";
	status::StatusInternal status1;
	status::StatusInternal status2;

	std::mutex mux;
	std::condition_variable condition;
	bool go = false;

	auto future1 = std::async(std::launch::async,
			[&]()->status::StatusInternal{
		bool available;

		std::unique_lock<std::mutex> lock(mux);
		condition.wait(lock, [&] {return go;});

		dfsFile file = dfsOpenFile(m_namenodeDefault, path, O_RDONLY, 0, 0, 0, available);
		CHECK(file != nullptr);
		CHECK(available);
		status1 = dfsCloseFile(m_namenodeDefault, file);
		return status1;
	});
	auto future2 = std::async(std::launch::async,
			[&]()->status::StatusInternal{
		bool available;

		std::unique_lock<std::mutex> lock(mux);
		condition.wait(lock, [&] {return go;});

		dfsFile file = dfsOpenFile(m_namenodeDefault, path, O_RDONLY, 0, 0, 0, available);
		CHECK(file != nullptr);
		CHECK(available);
		status2 = dfsCloseFile(m_namenodeDefault, file);
		return status2;
	});

	// go workers
	go = true;
	condition.notify_all();

	status1 = future1.get();
    status2 = future2.get();

	ASSERT_TRUE(status1 == status::StatusInternal::OK);
	ASSERT_TRUE(status2 == status::StatusInternal::OK);

	// cleanup:
	dfsDelete(m_namenodeDefault, path, true);
}

}

