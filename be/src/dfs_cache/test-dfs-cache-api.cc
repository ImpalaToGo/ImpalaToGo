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
#include <future>
#include <boost/thread/thread.hpp>

#include "dfs_cache/gtest-fixtures.hpp"
#include "dfs_cache/test-utilities.hpp"

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
	const char* path = "hdfs://104.236.39.60:8020/test.txt";

	bool available;
	dfsFile file = dfsOpenFile(m_namenodeHdfs, path, O_RDONLY, 0, 0, 0, available);
	ASSERT_TRUE(file != nullptr);
	ASSERT_TRUE(available);
}

TEST_F(CacheLayerTest, ExplicitHostFlowFileOpenCloseAutoloadTestFileDoesNotExist){
	const char* path = "hdfs://104.236.39.60:8020/test.txt";

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
	const char* path = "hdfs://104.236.39.60:8020/test.txt";
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

		dfsFile file = dfsOpenFile(m_namenodeHdfs, path, O_RDONLY, 0, 0, 0, available);
		CHECK(file != nullptr);
		CHECK(available);
		status1 = dfsCloseFile(m_namenodeHdfs, file);
		return status1;
	});
	auto future2 = std::async(std::launch::async,
			[&]()->status::StatusInternal{
		bool available;

		std::unique_lock<std::mutex> lock(mux);
		condition.wait(lock, [&] {return go;});

		dfsFile file = dfsOpenFile(m_namenodeHdfs, path, O_RDONLY, 0, 0, 0, available);
		CHECK(file != nullptr);
		CHECK(available);
		status2 = dfsCloseFile(m_namenodeHdfs, file);
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
	dfsDelete(m_namenodeHdfs, path, true);
}

// file open-close predicate to being run for multi client
void close_open_file(const char* path, FileSystemDescriptor& fsDescriptor){
	bool available;
	dfsFile file = dfsOpenFile(fsDescriptor, path, O_RDONLY, 0, 0, 0, available);
	ASSERT_TRUE(file != nullptr);
	ASSERT_TRUE(available);
	status::StatusInternal status = dfsCloseFile(fsDescriptor, file);
	ASSERT_TRUE(status == status::StatusInternal::OK);

	// cleanup:
	dfsDelete(fsDescriptor, path, true);
}

TEST_F(CacheLayerTest, OpenCloseHeavyLoadManagedAsync) {
	m_flag = false;

	const int CONTEXT_NUM = 100;

	std::string path = "s3n://impalatogo/test2.txt";

	using namespace std::placeholders;

	auto f1 = std::bind(&close_open_file, ph::_1, ph::_2);

	std::vector<std::future<void>> futures;
	for (int i = 0; i < CONTEXT_NUM; i++) {
		futures.push_back(
				std::move(
						spawn_task(f1, path.c_str(), std::ref(m_namenodeDefault))));
	}

	for (int i = 0; i < CONTEXT_NUM; i++) {
		if (futures[i].valid())
			futures[i].get();
	}

	EXPECT_EQ(futures.size(), CONTEXT_NUM);
}

TEST_F(CacheLayerTest, TestCopyRemoteFileToLocal){
	const char* src = "/home/test/install.sh";
	const char* dst = "/home/elenav/src/ImpalaToGo/analysis/install.sh";
	status::StatusInternal status = dfsCopy(m_namenodeDefault, src, m_namenodelocalFilesystem, dst);
	ASSERT_TRUE(status == status::StatusInternal::OK);
}

/**
 * Scenario :
 * 1. There's the predefined "set of remote data" located on target A (remote equivalent).
 * 2. There's "local" destination B where data is cached.
 * 3. All dataset should be analyzed in two ways - the Model and the Tested Module (dfs_cache).
 * Analysis results should be compared according to Rules and be identical for test to succeed.
 * 4. Rules: dataset files sizes should match for Model output and Tested Module output.
 */
TEST_F(CacheLayerTest, TestPrepareDataSetCompareResult){
	m_dataset_path = "/home/impalauser/data/impala/";

	const char* target      = m_dataset_path.c_str();
	const char* destination = m_cache_path.c_str();

	boost::system::error_code ec;

	// clean cache directory before usage:
	boost::filesystem::remove_all(m_cache_path, ec);
    ASSERT_TRUE(!ec);

	boost::filesystem::create_directory(m_cache_path, ec);
	ASSERT_TRUE(!ec);

    // first check working directories exist:
	ASSERT_TRUE(boost::filesystem::exists(target));
	ASSERT_TRUE(boost::filesystem::exists(destination));

	// get the connection to local file system:
	FileSystemDescriptorBound fsAdaptor(m_namenodelocalFilesystem);
	raiiDfsConnection conn = fsAdaptor.getFreeConnection();
	ASSERT_TRUE(conn.connection() != NULL);

	// get the list of all files within the specified dataset:
	int entries;
    dfsFileInfo* files = fsAdaptor.listDirectory(conn, target, &entries);
    ASSERT_TRUE((files != NULL) && (entries != 0));

    dfsFile file       = nullptr;
    dfsFile remotefile = nullptr;

#define BUFFER_SIZE 17408

    for(int i = 0 ; i < entries; i++){
    	// open file, say its local one:
    	bool available;
    	file = dfsOpenFile(m_namenodelocalFilesystem, files[i].mName,O_RDONLY, 0, 0, 0, available);
    	ASSERT_TRUE((file != NULL) && available);

    	// open "target" file:
    	remotefile = fsAdaptor.fileOpen(conn, files[i].mName, O_RDONLY, 0, 0, 0);
    	ASSERT_TRUE(remotefile != NULL);

    	// now read by blocks and compare:
    	tSize last_read_local = 0;
    	tSize last_read_remote = 0;

    	char* buffer_remote = (char*)malloc(sizeof(char) * BUFFER_SIZE);
    	char* buffer_local = (char*)malloc(sizeof(char) * BUFFER_SIZE);

    	last_read_remote = fsAdaptor.fileRead(conn, remotefile, (void*)buffer_remote, BUFFER_SIZE);
    	last_read_local = fsAdaptor.fileRead(conn, file, (void*)buffer_local, BUFFER_SIZE);

    	ASSERT_TRUE(last_read_remote == last_read_local);

    	for (; last_read_remote > 0;) {
    		// check read bytes count is equals:
        	ASSERT_TRUE(last_read_remote == last_read_local);
        	// compare memory contents we read from files:
        	ASSERT_TRUE(std::memcmp(buffer_remote, buffer_local, last_read_remote));

    		// read next data buffer:
    		last_read_remote = fsAdaptor.fileRead(conn, remotefile, (void*)buffer_remote, BUFFER_SIZE);
    		last_read_local = fsAdaptor.fileRead(conn, file, (void*)buffer_local, BUFFER_SIZE);
    	}
    }
}

/**
 * Test for underlying LRU cache age buckets span reduction.
 * The goal is to get several age buckets hosted by LRU before to reach the cleanup, than check that
 * the cache is still working.
 *
 * Scenario:
 * 1. Cache is being populated with files those fits into different age bags so that the number
 * of age bags is increased. Prerequisites : age bag creation split time should be decreased for this test.
 * 2. Each file is being read directly from "target" and from cache and compared byte by byte.
 * 3. After all dataset is completed in this way, the dataset should be read and compared once again.
 * This will trigger full cache reload.
 */
TEST_F(CacheLayerTest, TestCacheAgebucketSpanReduction){
	m_dataset_path = "/home/impalauser/data/impala/";

	m_timeslice = 10;

	// Initialize cache with 1 Mb
	cacheInit(85, m_cache_path, boost::posix_time::seconds(m_timeslice), 1048576);

	// configure local filesystem:
	cacheConfigureFileSystem(m_namenodelocalFilesystem);

	const char* target      = m_dataset_path.c_str();
	const char* destination = m_cache_path.c_str();

	boost::system::error_code ec;

	// clean cache directory before usage:
	boost::filesystem::remove_all(m_cache_path, ec);
    ASSERT_TRUE(!ec);

	boost::filesystem::create_directory(m_cache_path, ec);
	ASSERT_TRUE(!ec);

    // now check all working directories exist:
	ASSERT_TRUE(boost::filesystem::exists(target));
	ASSERT_TRUE(boost::filesystem::exists(destination));

	// get the connection to local file system:
	FileSystemDescriptorBound fsAdaptor(m_namenodelocalFilesystem);
	raiiDfsConnection conn = fsAdaptor.getFreeConnection();
	ASSERT_TRUE(conn.connection() != NULL);

	// get the list of all files within the specified dataset:
	int entries;
    dfsFileInfo* files = fsAdaptor.listDirectory(conn, target, &entries);
    ASSERT_TRUE((files != NULL) && (entries != 0));

    dfsFile file       = nullptr;
    dfsFile remotefile = nullptr;

#define BUFFER_SIZE 17408

    for(int i = 0 ; i < entries; i++){
    	// open file, say its local one:
    	bool available;
    	file = dfsOpenFile(m_namenodelocalFilesystem, files[i].mName,O_RDONLY, 0, 0, 0, available);
    	ASSERT_TRUE((file != NULL) && available);

    	// open "target" file:
    	remotefile = fsAdaptor.fileOpen(conn, files[i].mName, O_RDONLY, 0, 0, 0);
    	ASSERT_TRUE(remotefile != NULL);

    	// now read by blocks and compare:
    	tSize last_read_local = 0;
    	tSize last_read_remote = 0;

    	char* buffer_remote = (char*)malloc(sizeof(char) * BUFFER_SIZE);
    	char* buffer_local = (char*)malloc(sizeof(char) * BUFFER_SIZE);

    	last_read_remote = fsAdaptor.fileRead(conn, remotefile, (void*)buffer_remote, BUFFER_SIZE);
    	last_read_local = fsAdaptor.fileRead(conn, file, (void*)buffer_local, BUFFER_SIZE);

    	ASSERT_TRUE(last_read_remote == last_read_local);

    	for (; last_read_remote > 0;) {
    		// check read bytes count is equals:
        	ASSERT_TRUE(last_read_remote == last_read_local);
        	// compare memory contents we read from files:
        	ASSERT_TRUE(std:: memcmp(buffer_remote, buffer_local, last_read_remote));

    		// read next data buffer:
    		last_read_remote = fsAdaptor.fileRead(conn, remotefile, (void*)buffer_remote, BUFFER_SIZE);
    		last_read_local = fsAdaptor.fileRead(conn, file, (void*)buffer_local, BUFFER_SIZE);
    	}
    	// each three files,
    	if((i % 3) == 0){
    		// now sleep for "slice duration + 1" to have new age bucket created within the cache:
    		boost::this_thread::sleep( boost::posix_time::seconds(m_timeslice + 1) );
    	}
    }
}
}
