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

TEST_F(CacheLayerTest, DISABLED_ExplicitHostFlowFileOpenCloseAutoloadTestFileDoesNotExist){
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

TEST_F(CacheLayerTest, DISABLED_TwoClientsRequestSameFileForOpenWhichIsNotExistsInitially){
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

TEST_F(CacheLayerTest, DISABLED_OpenCloseHeavyLoadManagedAsync) {
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

TEST_F(CacheLayerTest, DISABLED_TestCopyRemoteFileToLocal){
	const char* src = "/home/test/install.sh";
	const char* dst = "/home/elenav/src/ImpalaToGo/analysis/install.sh";
	status::StatusInternal status = dfsCopy(m_namenodeDefault, src, m_namenodelocalFilesystem, dst);
	ASSERT_TRUE(status == status::StatusInternal::OK);
}

/**
 * General validation for data accessed via cache layer.
 *
 * Scenario :
 * 0. Cache is set with a fixed size.
 *
 * 1. There's the predefined "set of remote data" located on target A (remote equivalent).
 * It's size = (1.5 * cache size)
 *
 * 2. There's "local" destination B where data should be cached.
 * 3. All dataset should be analyzed in two ways - the Model and the Tested Module (dfs_cache).
 * Analysis results should be compared according to Rules and be identical for test to succeed.
 *
 * 4. Rules: dataset files sizes should match for Model output and Tested Module output.
 * 5. Test succeeded in case if all byte-comparisons passed successfully.
 */
TEST_F(CacheLayerTest, TestPrepareDataSetCompareResult){
	m_dataset_path = constants::TEST_DATASET_DEFAULT_LOCATION;

	const char* target      = m_dataset_path.c_str();
	const char* destination = m_cache_path.c_str();

	boost::system::error_code ec;

	SCOPED_TRACE("Reset the cache...");

	// clean cache directory before usage:
	boost::filesystem::remove_all(m_cache_path, ec);
	SCOPED_TRACE(ec.message());
    ASSERT_TRUE(!ec);

	boost::filesystem::create_directory(m_cache_path, ec);
	ASSERT_TRUE(!ec);

    // first check working directories exist:
	ASSERT_TRUE(boost::filesystem::exists(target));
	ASSERT_TRUE(boost::filesystem::exists(destination));

	SCOPED_TRACE("Working directories exist.");

	// check dataset size it least of 1.5 of configured cache size:
    boost::uintmax_t dataset_size = utilities::get_dir_busy_space(target);
    double overlap_ratio = 1.5;
    ASSERT_TRUE(dataset_size / overlap_ratio >= constants::TEST_CACHE_FIXED_SIZE);

    SCOPED_TRACE("Dataset is validated and is ready");

	cacheInit(constants::TEST_CACHE_DEFAULT_FREE_SPACE_PERCENT, constants::TEST_CACHE_DEFAULT_LOCATION,
			boost::posix_time::hours(-1), constants::TEST_CACHE_FIXED_SIZE);
	cacheConfigureFileSystem(m_namenodelocalFilesystem);

	// get the connection to local file system:
	FileSystemDescriptorBound fsAdaptor(m_namenodelocalFilesystem);
	raiiDfsConnection conn = fsAdaptor.getFreeConnection();
	ASSERT_TRUE(conn.connection() != NULL);

	SCOPED_TRACE("Localhost filesystem adaptor is ready");

	// get the list of all files within the specified dataset:
	int entries;
    dfsFileInfo* files = fsAdaptor.listDirectory(conn, target, &entries);
    ASSERT_TRUE((files != NULL) && (entries != 0));
    for(int i = 0; i < entries; i++){
    	std::cout << files[i].mName << std::endl;
    }

    dfsFile file       = nullptr;
    dfsFile remotefile = nullptr;

#define BUFFER_SIZE 17408

    // we need to run twice the functor below to check the cache alive after the cleanup
	boost::function<void()> scenario = [&]() {
		for(int i = 0; i < entries; i++) {
			std::string path(files[i].mName);
			path = path.insert(path.find_first_of("/"), "/");

			// open file, say its local one:
			bool available;
			file = dfsOpenFile(m_namenodelocalFilesystem, path.c_str(), O_RDONLY, 0, 0, 0, available);
			ASSERT_TRUE((file != NULL) && available);

			// open "target" file:
            // and add an extra slash to have the uri "file:///path"
			remotefile = fsAdaptor.fileOpen(conn, path.insert(path.find_first_of("/"), "/").c_str(), O_RDONLY, 0, 0, 0);
			ASSERT_TRUE(remotefile != NULL);

			// now read by blocks and compare:
			tSize last_read_local = 0;
			tSize last_read_remote = 0;

			char* buffer_remote = (char*)malloc(sizeof(char) * BUFFER_SIZE);
			char* buffer_local = (char*)malloc(sizeof(char) * BUFFER_SIZE);

			last_read_remote = fsAdaptor.fileRead(conn, remotefile, (void*)buffer_remote, BUFFER_SIZE);
			last_read_local = dfsRead(m_namenodelocalFilesystem, file, (void*)buffer_local, BUFFER_SIZE);

			ASSERT_TRUE(last_read_remote == last_read_local);

			for (; last_read_remote > 0;) {
				// check read bytes count is equals:
				ASSERT_TRUE(last_read_remote == last_read_local);
				// compare memory contents we read from files:
				ASSERT_TRUE((std::memcmp(buffer_remote, buffer_local, last_read_remote) == 0));

				// read next data buffer:
				last_read_remote = fsAdaptor.fileRead(conn, remotefile, (void*)buffer_remote, BUFFER_SIZE);
				last_read_local = dfsRead(m_namenodelocalFilesystem, file, (void*)buffer_local, BUFFER_SIZE);
			}
			// close file handles, local and remote:
			ASSERT_TRUE(fsAdaptor.fileClose(conn, remotefile) == 0);
			ASSERT_TRUE(dfsCloseFile(m_namenodelocalFilesystem, file) == 0);
		}
	};

	// number of retries may be increased
	for(int iteration = 0; iteration < 2; iteration++)
		scenario();

	SCOPED_TRACE("Test is near to complete, cleanup...");

    // free file info:
    fsAdaptor.freeFileInfo(files, entries);
}

/**
 * Test for underlying LRU cache age buckets span management.
 * The goal is to get several age buckets hosted by LRU before to reach the cleanup, than check that
 * the cache is still working.
 *
 * Scenario:
 * 0. Prerequisites for cache initialization:
 * - age bag creation split time should be decreased;
 * - cache size should be fixed
 *
 * 1. Cache is being populated with files those fits into different age bags so that the number
 * of age bags is increased.
 *
 * 2. When cache population is <= cache capacity, open / read /close each file within already added set.
 * Compare bytes read with the direct read from dataset origin.
 * Keep one of files opened.
 *
 * 3. Proceed with cache population which triggers cache cleanup.
 * 4. After original dataset is completed, each file from this dataset should be read again from cache and
 * compared to data read directly from origin dataset. once again.
 *
 * 5. Close the file that was kept opened on step 2.
 * 6. Test succeeded in case if all byte-comparisons passed successfully.
 */
TEST_F(CacheLayerTest, DISABLED_TestCacheAgebucketSpanReduction){
	m_dataset_path = constants::TEST_CACHE_DEFAULT_LOCATION;

	// age bucket time slice:
	m_timeslice = constants::TEST_CACHE_REDUCED_TIMESLICE;

	// Initialize cache with 1 Mb
	cacheInit(85, m_cache_path, boost::posix_time::seconds(m_timeslice), constants::TEST_CACHE_FIXED_SIZE);

	// configure local filesystem:
	cacheConfigureFileSystem(m_namenodelocalFilesystem);

	const char* target      = m_dataset_path.c_str();
	const char* destination = m_cache_path.c_str();

	boost::system::error_code ec;

	SCOPED_TRACE("Reset the cache...");

	// clean cache directory before usage:
	boost::filesystem::remove_all(m_cache_path, ec);
    ASSERT_TRUE(!ec);

	boost::filesystem::create_directory(m_cache_path, ec);
	ASSERT_TRUE(!ec);

    // now check all working directories exist:
	ASSERT_TRUE(boost::filesystem::exists(target));
	ASSERT_TRUE(boost::filesystem::exists(destination));

	SCOPED_TRACE("Working directories exist.");

	// check dataset size it least of 1.5 of configured cache size:
    boost::uintmax_t dataset_size = utilities::get_dir_busy_space(target);
    double overlap_ratio = 1.5;
    ASSERT_TRUE(dataset_size / overlap_ratio >= constants::TEST_CACHE_FIXED_SIZE);

    SCOPED_TRACE("Dataset is validated and is ready");

	// get the connection to local file system:
	FileSystemDescriptorBound fsAdaptor(m_namenodelocalFilesystem);
	raiiDfsConnection conn = fsAdaptor.getFreeConnection();
	ASSERT_TRUE(conn.connection() != NULL);

	SCOPED_TRACE("Localhost filesystem adaptor is ready");

	// get the list of all files within the specified dataset:
	int entries;
    dfsFileInfo* files = fsAdaptor.listDirectory(conn, target, &entries);
    ASSERT_TRUE((files != NULL) && (entries != 0));

    dfsFile file       = nullptr;
    dfsFile remotefile = nullptr;

#define BUFFER_SIZE 17408

    // go over dataset and track the amount of bytes we already added. Perform the extra steps
    // right before the cache cleanup will be triggered:
    tSize cached_data_size = 0;

    // file handle we leave non-closed before the cleanup is triggered.
    // should be closed at the end of the test
    dfsFile preserved_handle = nullptr;

    // preserve iteration number we left the file handle opened for:
    int preserved_iteration = -1;

    // the functor below read-compares the same file via the cache API and directly, then compare bytes
    boost::function<void(int)> scenario = [&](int i) {
    	// open file, say its local one:
    	bool available;
    	file = dfsOpenFile(m_namenodelocalFilesystem, files[i].mName, O_RDONLY, 0, 0, 0, available);
    	ASSERT_TRUE((file != NULL) && available);

    	// increase cached data size
    	cached_data_size += files[i].mSize;

    	// open "target" file:
    	remotefile = fsAdaptor.fileOpen(conn, files[i].mName, O_RDONLY, 0, 0, 0);
    	ASSERT_TRUE(remotefile != NULL);

    	// now read by blocks and compare:
    	tSize last_read_local = 0;
    	tSize last_read_remote = 0;

    	char* buffer_remote = (char*)malloc(sizeof(char) * BUFFER_SIZE);
    	char* buffer_local = (char*)malloc(sizeof(char) * BUFFER_SIZE);

    	last_read_remote = fsAdaptor.fileRead(conn, remotefile, (void*)buffer_remote, BUFFER_SIZE);
    	last_read_local = dfsRead(m_namenodelocalFilesystem, file, (void*)buffer_local, BUFFER_SIZE);

    	ASSERT_TRUE(last_read_remote == last_read_local);

    	for (; last_read_remote > 0;) {
    		// check read bytes count is equals:
        	ASSERT_TRUE(last_read_remote == last_read_local);
        	// compare memory contents we read from files:
        	ASSERT_TRUE(std:: memcmp(buffer_remote, buffer_local, last_read_remote));

    		// read next data buffer:
    		last_read_remote = fsAdaptor.fileRead(conn, remotefile, (void*)buffer_remote, BUFFER_SIZE);
    		last_read_local = dfsRead(m_namenodelocalFilesystem, file, (void*)buffer_local, BUFFER_SIZE);
    	}
    };

    for(int i = 0 ; i < entries; i++){

    	bool extrascenario = false;
    	// check whether we near to reach the cache size limit on this iteration:
    	if((cached_data_size + files[i].mSize) > constants::TEST_CACHE_FIXED_SIZE){
    		// run the extra scenario:
            extrascenario = true;
    	}

    	// for any iteration except first one
    	if(i != 0){
    		// close file handles, local and remote:
    		ASSERT_TRUE(fsAdaptor.fileClose(conn, remotefile) == 0);
    		// previously opened local handle should not be closed in extra scenario
    		if(!extrascenario)
    			ASSERT_TRUE(dfsCloseFile(m_namenodelocalFilesystem, file) == 0);
    		else{
    			// preserve the previously opened file handle we left non-closed
    			preserved_handle    = file;
    			preserved_iteration = i - 1;
    		}
    	}

    	// run the files comparison
    	scenario(i);

    	// each three files, wait for configured timelice is exceeded to introduce new age bucket on next iteration
    	if((i % 3) == 0){
    		// now sleep for "slice duration + 1" to have new age bucket created within the cache:
    		boost::this_thread::sleep( boost::posix_time::seconds(m_timeslice + 1) );
    	}
    }

	// close file handles if any, local and remote:
    ASSERT_TRUE(remotefile != nullptr);
    ASSERT_TRUE(fsAdaptor.fileClose(conn, remotefile) == 0);

    ASSERT_TRUE(file != nullptr);
    ASSERT_TRUE(dfsCloseFile(m_namenodelocalFilesystem, file) == 0);

    // here, we passed the cache cleanup already.
    // close preserved file and check we can access it.
    ASSERT_TRUE(preserved_handle != nullptr);
    ASSERT_TRUE(dfsCloseFile(m_namenodelocalFilesystem, preserved_handle));

    SCOPED_TRACE("Going to run comparison for preserved file");

    // run the file comparison for preserved iteration:
    scenario(preserved_iteration);

    SCOPED_TRACE("Going to run second dataset validation iteration");

    // and re-run all dataset check completely (another one cache reload):
    for(int i = 0 ; i < entries; i++){
    	scenario(i);

        ASSERT_TRUE(remotefile != nullptr);
        ASSERT_TRUE(fsAdaptor.fileClose(conn, remotefile) == 0);

        ASSERT_TRUE(file != nullptr);
        ASSERT_TRUE(dfsCloseFile(m_namenodelocalFilesystem, file) == 0);
    }

    SCOPED_TRACE("Test is near to complete, cleanup...");

    // free file info:
    fsAdaptor.freeFileInfo(files, entries);
}
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();

}
