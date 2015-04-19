/*
 * @file test-dfs-cache-api.cc
 * @brief contains tests for Cache layer API
 *
 * @date   Nov 17, 2014
 * @author elenav
 */

#include <string>
#include <fcntl.h>
#include <future>
#include <boost/thread/thread.hpp>

#include "dfs_cache/gtest-fixtures.hpp"
#include "dfs_cache/test-utilities.hpp"

namespace impala{

FileSystemDescriptor CacheLayerTest::m_dfsIdentityDefault;
FileSystemDescriptor CacheLayerTest::m_dfsIdentitylocalFilesystem;
FileSystemDescriptor CacheLayerTest::m_dfsIdentityTachyon;

SessionContext CacheLayerTest::m_ctx1 = nullptr;
SessionContext CacheLayerTest::m_ctx2 = nullptr;
SessionContext CacheLayerTest::m_ctx3 = nullptr;
SessionContext CacheLayerTest::m_ctx4 = nullptr;
SessionContext CacheLayerTest::m_ctx5 = nullptr;
SessionContext CacheLayerTest::m_ctx6 = nullptr;

#define BUFFER_SIZE 17408

/** Collects the statistics about file handles usage during cache layer interaction
 *  @param [in] file - file handle to collect the info for
 *  @param [in/out]  - direct_handles counter
 *  @param [in/out]  - cached_handles counter
 *  @param [in/out]  - zero_handles counter
 *  @param [in/out]  - total_handles counter
 */
static void collectFileHandleStat(dfsFile file,
		std::atomic<long>& direct_handles,
		std::atomic<long>& cached_handles,
		std::atomic<long>& zero_handles,
		std::atomic<long>& total_handles){

	if(file == NULL){
		// increment zero handles
		zero_handles.fetch_add(1l);
		std::cout << "Null file handle";
		return;
	}
	// increment total handles
	total_handles.fetch_add(1l);
	// for direct file, increment the number of directly opened handles
	if(file->direct){
		direct_handles.fetch_add(1l);
	}
	else{
		// increment total handles
		cached_handles.fetch_add(1l);
	}
}

/**
 * File open-close-delete predicate to being run for multi client
 * @param path         - file path to open/close it via cache layer API
 * @param fsDescriptor - file system the file belongs to descriptor
 *
 * @param [in/out]  - direct_handles counter
 * @param [in/out]  - cached_handles counter
 * @param [in/out]  - zero_handles counter
 * @param [in/out]  - total_handles counter
 */
static void close_open_file(const char* path, const FileSystemDescriptor& fsDescriptor,
		std::atomic<long>& direct_handles,
		std::atomic<long>& cached_handles,
		std::atomic<long>& zero_handles,
		std::atomic<long>& total_handles){
	bool available;
	dfsFile file = dfsOpenFile(fsDescriptor, path, O_RDONLY, 0, 0, 0, available);
	collectFileHandleStat(file, direct_handles, cached_handles, zero_handles, total_handles);
	ASSERT_TRUE(file != nullptr);
	ASSERT_TRUE(available);
	status::StatusInternal status = dfsCloseFile(fsDescriptor, file);
	ASSERT_TRUE(status == status::StatusInternal::OK);
}

static void open_read_close_file(std::string path, const FileSystemDescriptor& fsDescriptor,
		std::atomic<long>& direct_handles,
		std::atomic<long>& cached_handles,
		std::atomic<long>& zero_handles,
		std::atomic<long>& total_handles){

	// get the connection to local file system:
	FileSystemDescriptorBound fsAdaptor(fsDescriptor);
	raiiDfsConnection conn = fsAdaptor.getFreeConnection();
	ASSERT_TRUE(conn.connection() != NULL);

	bool available;
	dfsFile file = dfsOpenFile(fsDescriptor, (constants::TEST_LOCALFS_PROTO_PREFFIX + path).c_str(), O_RDONLY, 0, 0, 0, available);
	collectFileHandleStat(file, direct_handles, cached_handles, zero_handles, total_handles);
	ASSERT_TRUE((file != NULL) && available);

	// open "target" file:
	// and add an extra slash to have the uri "file:///path"
	dfsFile remotefile = fsAdaptor.fileOpen(conn, (path.insert(0, constants::TEST_LOCALFS_PROTO_PREFFIX + "/")).c_str(), O_RDONLY, 0, 0, 0);
	ASSERT_TRUE(remotefile != NULL);

	// now read by blocks and compare:
	tSize last_read_local = 0;
	tSize last_read_remote = 0;

	char* buffer_remote = (char*)malloc(sizeof(char) * BUFFER_SIZE);
	char* buffer_local = (char*)malloc(sizeof(char) * BUFFER_SIZE);

	last_read_remote = fsAdaptor.fileRead(conn, remotefile, (void*)buffer_remote, BUFFER_SIZE);
	last_read_local = dfsRead(fsDescriptor, file, (void*)buffer_local, BUFFER_SIZE);

	ASSERT_TRUE(last_read_remote == last_read_local);

	for (; last_read_remote > 0;) {
		// check read bytes count is equals:
    	ASSERT_TRUE(last_read_remote == last_read_local);
    	// compare memory contents we read from files:
    	ASSERT_TRUE((std:: memcmp(buffer_remote, buffer_local, last_read_remote) == 0));

		// read next data buffer:
		last_read_remote = fsAdaptor.fileRead(conn, remotefile, (void*)buffer_remote, BUFFER_SIZE);
		last_read_local = dfsRead(fsDescriptor, file, (void*)buffer_local, BUFFER_SIZE);
	}

	free(buffer_remote);
	free(buffer_local);

	// close file handles, local and remote:
	ASSERT_TRUE(fsAdaptor.fileClose(conn, remotefile) == status::StatusInternal::OK);
	ASSERT_TRUE(dfsCloseFile(fsDescriptor, file) == status::StatusInternal::OK);
}

static void open_seek_read_compare_close_file(std::string& path,
		const FileSystemDescriptor& fsDescriptor,
		std::atomic<long>& direct_handles,
		std::atomic<long>& cached_handles,
		std::atomic<long>& zero_handles,
		std::atomic<long>& total_handles){

	// get the connection to local file system:
	FileSystemDescriptorBound fsAdaptor(fsDescriptor);
	raiiDfsConnection conn = fsAdaptor.getFreeConnection();
	ASSERT_TRUE(conn.connection() != NULL);

	// open "cached" file:
	std::string cache_path = constants::TEST_LOCALFS_PROTO_PREFFIX + path;
	bool available;
	dfsFile file = dfsOpenFile(fsDescriptor, cache_path.c_str(), O_RDONLY, 0, 0, 0, available);
	collectFileHandleStat(file, direct_handles, cached_handles, zero_handles, total_handles);
	ASSERT_TRUE((file != NULL) && available);

	// open "target" file:
	// and add an extra slash to have the uri "file:///path"
	dfsFile remotefile = fsAdaptor.fileOpen(conn, (path.insert(0, constants::TEST_LOCALFS_PROTO_PREFFIX + "/").c_str()), O_RDONLY, 0, 0, 0);
	ASSERT_TRUE(remotefile != NULL);

	// now read by blocks and compare:
	tSize last_read_local = 0;
	tSize last_read_remote = 0;

	char* buffer_remote = (char*) malloc(sizeof(char) * BUFFER_SIZE);
	char* buffer_local = (char*) malloc(sizeof(char) * BUFFER_SIZE);

	last_read_remote = fsAdaptor.fileRead(conn, remotefile,
			(void*) buffer_remote, BUFFER_SIZE);
	last_read_local = dfsRead(fsDescriptor, file, (void*) buffer_local,
			BUFFER_SIZE);

	ASSERT_TRUE(last_read_remote == last_read_local);

	// compare memory contents we read from files:
	ASSERT_TRUE((std::memcmp(buffer_remote, buffer_local, last_read_remote) == 0));

	// perform the fileseek on both handles:
	ASSERT_TRUE(dfsSeek(fsDescriptor, file, BUFFER_SIZE + 1) == 0);
	ASSERT_TRUE(fsAdaptor.fileSeek(conn, remotefile, BUFFER_SIZE + 1) == 0);

	// read the data buffer once again:
	last_read_remote = fsAdaptor.fileRead(conn, remotefile,
			(void*) buffer_remote, BUFFER_SIZE);
	last_read_local = dfsRead(fsDescriptor, file, (void*) buffer_local,	BUFFER_SIZE);

	ASSERT_TRUE(last_read_remote == last_read_local);
	// compare memory contents we read from files:
	ASSERT_TRUE((std::memcmp(buffer_remote, buffer_local, last_read_remote) == 0));

	free(buffer_remote);
	free(buffer_local);

	// close file handles, local and remote:
	ASSERT_TRUE(fsAdaptor.fileClose(conn, remotefile) == status::StatusInternal::OK);
	ASSERT_TRUE(dfsCloseFile(fsDescriptor, file) == status::StatusInternal::OK);
}

/**
 * Run "open/close" scenario on one of the files selected in a random way from the given
 * collection
 *
 * @param fsDescriptor - file system the file belongs to descriptor
 * @param filenames    - list of filenames to shuffle among
 *
 * @param [in/out]  - direct_handles counter
 * @param [in/out]  - cached_handles counter
 * @param [in/out]  - zero_handles counter
 * @param [in/out]  - total_handles counter
 */
static void close_open_file_sporadic(const FileSystemDescriptor& fsDescriptor,
		const std::string& fsname,
		const std::vector<std::string>& filenames,
		std::atomic<long>& direct_handles,
		std::atomic<long>& cached_handles,
		std::atomic<long>& zero_handles,
		std::atomic<long>& total_handles){
    std::string path = getRandomFromVector(filenames);
    ASSERT_TRUE(!path.empty());
    std::cout << "open-close sporadic, File selected : \"" << path << "\"." << std::endl;
    // add the file:// suffix:
    close_open_file((fsname + path).c_str(), fsDescriptor,
    		direct_handles, cached_handles, zero_handles, total_handles);
}

/**
 * Run the scenario selected in the random way from the list of defined
 *
 * @param scenarios    - list of predefined scenarios
 * @param fsDescriptor - file system the file belongs to descriptor
 * @param fsPath       - selected filesystem path
 * @param filenames    - dataset filenames
 *
 * @param [in/out]  - direct_handles counter
 * @param [in/out]  - cached_handles counter
 * @param [in/out]  - zero_handles counter
 * @param [in/out]  - total_handles counter
 *
 * @param iterations - number of iterations to run tests within the single method call
 */
static void run_random_scenario(
		const std::vector<ScenarioCase>& scenarios,
		const FileSystemDescriptor& fsDescriptor,
		const std::string& fsPath,
		const std::vector<std::string>& filenames,
		std::atomic<long>& direct_handles,
		std::atomic<long>& cached_handles,
		std::atomic<long>& zero_handles,
		std::atomic<long>& total_handles,
		const int iterations){

	ASSERT_TRUE(scenarios.size() != 0);
	for(int i = 0; i < iterations; i++){
		ScenarioCase scenario = getRandomFromVector(scenarios);

		std::cout << "run random scenario, \"" << scenario.name
				<< "\" selected." << std::endl;
		scenario.scenario(boost::cref(fsDescriptor), boost::cref(fsPath), boost::ref(filenames),
				boost::ref(direct_handles), boost::ref(cached_handles),
				boost::ref(zero_handles), boost::ref(total_handles));
	}
}

static void open_read_compare_close_file_sporadic(const FileSystemDescriptor& fsDescriptor,
		const std::string& fsPath,
		const std::vector<std::string>& filenames,
		std::atomic<long>& direct_handles,
		std::atomic<long>& cached_handles,
		std::atomic<long>& zero_handles,
		std::atomic<long>& total_handles){
    std::string path = getRandomFromVector(filenames);
    ASSERT_TRUE(!path.empty());
    std::cout << "orcc : file selected : \"" << path << "\"." << std::endl;

    open_read_close_file(path, fsDescriptor,
    		direct_handles, cached_handles, zero_handles, total_handles);
}

static void open_seek_read_compare_close_file_sporadic(const FileSystemDescriptor& fsDescriptor,
		const std::string& fsPath,
		const std::vector<std::string>& filenames,
		std::atomic<long>& direct_handles,
		std::atomic<long>& cached_handles,
		std::atomic<long>& zero_handles,
		std::atomic<long>& total_handles){
    std::string path = getRandomFromVector(filenames);
    ASSERT_TRUE(!path.empty());
    std::cout << "osrcc : file selected : \"" << path << "\"." << std::endl;

    open_seek_read_compare_close_file(path, fsDescriptor,
    		direct_handles, cached_handles, zero_handles, total_handles);
}

static void rescan_dataset(const char* dataset_location, std::vector<std::string>& filenames){
	boost::filesystem::recursive_directory_iterator end_iter;

	// iterate the dataset and collect all file paths
	if ( boost::filesystem::exists(dataset_location) && boost::filesystem::is_directory(dataset_location)){
	  for( boost::filesystem::recursive_directory_iterator dir_iter(dataset_location) ; dir_iter != end_iter ; ++dir_iter){
	    if (boost::filesystem::is_regular_file(dir_iter->status()) ){
	    	filenames.push_back((*dir_iter).path().string());
	    }
	  }
	}
}

TEST_F(CacheLayerTest, DISABLED_TachyonTest) {

	std::vector<std::string> dataset;
	dataset.push_back("localhost:19998/eventsSmall/demo_20140629000000000016.csv");
	// rescan the dataset in order to get the collection of filenames.
	//rescan_dataset(m_dataset_path.c_str(), dataset);
	ASSERT_TRUE(dataset.size() != 0);

	// create the list of scenario to run within the test :
	std::vector< ScenarioCase > scenarios;

	// add "open-close" scenario:
	scenarios.push_back({boost::bind(close_open_file_sporadic, _1, _2, _3, _4, _5, _6, _7),
		"Close-Open-Sporadic"});

	// initialize default cache layer (direct access to remote dfs):
    cacheInit();
	cacheConfigureFileSystem(m_dfsIdentityTachyon);

	// get the connection to local file system:
	FileSystemDescriptorBound fsAdaptor(m_dfsIdentityTachyon);
	raiiDfsConnection conn = fsAdaptor.getFreeConnection();
	ASSERT_TRUE(conn.connection() != NULL);

	std::cout << "Tachyon filesystem adaptor is ready" << std::endl;

	const int CONTEXT_NUM = 1;
    const int ITERATIONS = 1;

	using namespace std::placeholders;

	auto f1 = std::bind(&run_random_scenario, ph::_1, ph::_2, ph::_3, ph::_4, ph::_5, ph::_6, ph::_7, ph::_8, ph::_9);

	std::vector<std::future<void>> futures;

	// go with workers
	for (int i = 0; i < CONTEXT_NUM; i++) {
		futures.push_back(
				std::move(
						spawn_task(f1, std::cref(scenarios),
								std::cref(m_dfsIdentityTachyon),
								std::cref(constants::TEST_TACHYONFS_PROTO_PREFIX),
								std::cref(dataset),
								std::ref(m_direct_handles),
								std::ref(m_cached_handles),
								std::ref(m_zero_handles),
								std::ref(m_total_handles),
								ITERATIONS)));
	}

	for (int i = 0; i < CONTEXT_NUM; i++) {
		if (futures[i].valid())
			futures[i].get();
	}

	EXPECT_EQ(futures.size(), CONTEXT_NUM);
}

/*
 * Simultaneous file request arriving from two clients
 *
 * Scenario :
 * 0. Cache is set with any size.
 * 1. 2 requests are issued for the same file and are being fired simultaneously.
 * 2. Test succeeds in case if both clients got the file handle and it is valid.
 */
TEST_F(CacheLayerTest, TwoClientsRequestSameFileForOpenWhichIsNotExistsInitially){
	boost::system::error_code ec;

	std::string data_location = m_dataset_path + constants::TEST_SINGLE_FILE_FROM_DATASET;
	char path[256];
	memset(path, 0, 256);
	data_location.copy(path, data_location.length() + 1, 0);
	ASSERT_TRUE(boost::filesystem::exists(path, ec));

	char filename[256];
	memset(filename, 0, 256);
	data_location = constants::TEST_LOCALFS_PROTO_PREFFIX + data_location;
	data_location.copy(filename, data_location.length() + 1, 0);

	std::cout << "Test data is validated and is ready\n";

	cacheInit(constants::TEST_CACHE_DEFAULT_FREE_SPACE_PERCENT, m_cache_path,
			boost::posix_time::hours(-1), constants::TEST_CACHE_FIXED_SIZE);
	cacheConfigureFileSystem(m_dfsIdentitylocalFilesystem);

	// get the connection to local file system:
	FileSystemDescriptorBound fsAdaptor(m_dfsIdentitylocalFilesystem);
	raiiDfsConnection conn = fsAdaptor.getFreeConnection();
	ASSERT_TRUE(conn.connection() != NULL);

	std::cout << "Localhost filesystem adaptor is ready\n";

	status::StatusInternal status0 = status::StatusInternal::OK;
	status::StatusInternal status1;
	status::StatusInternal status2 = status::StatusInternal::OK;
	status::StatusInternal status3;

	std::mutex mux;
	std::condition_variable condition;
	bool go = false;

	auto future1 = std::async(std::launch::async,
			[&]()->status::StatusInternal{
		bool available;

		std::unique_lock<std::mutex> lock(mux);
		condition.wait(lock, [&] {return go;});
 		dfsFile file = dfsOpenFile(m_dfsIdentitylocalFilesystem, filename, O_RDONLY, 0, 0, 0, available);
		collectFileHandleStat(file, m_direct_handles, m_cached_handles, m_zero_handles, m_total_handles);
		EXPECT_TRUE(file != nullptr);
		EXPECT_TRUE(available);
		if((file == nullptr) || !available)
			status0 = status::StatusInternal::FILE_OBJECT_OPERATION_FAILURE;

		status1 = dfsCloseFile(m_dfsIdentitylocalFilesystem, file);
		return status1;
	});
	auto future2 = std::async(std::launch::async,
			[&]()->status::StatusInternal{
		bool available;

		std::unique_lock<std::mutex> lock(mux);
		condition.wait(lock, [&] {return go;});

		dfsFile file = dfsOpenFile(m_dfsIdentitylocalFilesystem, filename, O_RDONLY, 0, 0, 0, available);
		collectFileHandleStat(file, m_direct_handles, m_cached_handles, m_zero_handles, m_total_handles);
		EXPECT_TRUE(file != nullptr);
		EXPECT_TRUE(available);
		if((file == nullptr) || !available)
					status2 = status::StatusInternal::FILE_OBJECT_OPERATION_FAILURE;
		status3 = dfsCloseFile(m_dfsIdentitylocalFilesystem, file);
		return status2;
	});

	// go workers
	go = true;
	condition.notify_all();

	status1 = future1.get();
    status2 = future2.get();

    ASSERT_TRUE(status0 == status::StatusInternal::OK);
	ASSERT_TRUE(status1 == status::StatusInternal::OK);
	ASSERT_TRUE(status2 == status::StatusInternal::OK);
	ASSERT_TRUE(status3 == status::StatusInternal::OK);

	// be sure we worked directly with a cache:
	ASSERT_TRUE(m_zero_handles.load() == 0);
	ASSERT_TRUE(m_direct_handles.load() == 0);
	ASSERT_TRUE(m_cached_handles.load() == 2);
	ASSERT_TRUE(m_total_handles.load() == 2);
}

/**
 * Simultaneous file request arriving from 50 clients
 *
 * Scenario :
 * 0. Cache is set with a size = 1.5 cache limit.
 * 1. 50 requests are issued, each for the file selected from dataset in random way.
 * All clients are being fired simultaneously.
 * 2. Test succeeds in case if all clients were able to complete scenario "open/close" with a sporadically selected file.
 */
TEST_F(CacheLayerTest, OpenCloseSporadicFileHeavyLoadManagedAsync) {

	std::vector<std::string> dataset;
	// rescan the dataset in order to get the collection of filenames.
	rescan_dataset(m_dataset_path.c_str(), dataset);
	ASSERT_TRUE(dataset.size() != 0);

	cacheInit(constants::TEST_CACHE_DEFAULT_FREE_SPACE_PERCENT, m_cache_path,
			boost::posix_time::hours(-1), constants::TEST_CACHE_FIXED_SIZE);
	cacheConfigureFileSystem(m_dfsIdentitylocalFilesystem);

	// get the connection to local file system:
	FileSystemDescriptorBound fsAdaptor(m_dfsIdentitylocalFilesystem);
	raiiDfsConnection conn = fsAdaptor.getFreeConnection();
	ASSERT_TRUE(conn.connection() != NULL);

	std::cout << "Localhost filesystem adaptor is ready" << std::endl;

	const int CONTEXT_NUM = 50;

	using namespace std::placeholders;

	auto f1 = std::bind(&close_open_file_sporadic, ph::_1, ph::_2, ph::_3, ph::_4, ph::_5, ph::_6, ph::_7);

	std::vector<std::future<void>> futures;
	for (int i = 0; i < CONTEXT_NUM; i++) {
		futures.push_back(
				std::move(
						spawn_task(f1, std::ref(m_dfsIdentitylocalFilesystem),
								std::cref(constants::TEST_LOCALFS_PROTO_PREFFIX),
								std::ref(dataset),
								std::ref(m_direct_handles),
								std::ref(m_cached_handles),
								std::ref(m_zero_handles),
								std::ref(m_total_handles))));
	}

	for (int i = 0; i < CONTEXT_NUM; i++) {
		if (futures[i].valid())
			futures[i].get();
	}

	EXPECT_EQ(futures.size(), CONTEXT_NUM);
}

/**
 * Long term stress test, emulates real multi-client environment.
 *
 * Scenario:
 * 1. Cache is configured with a fixed size.
 * 2. Dataset size = 1.5 cache size.
 * 3. Number of simultaneous clients = 100.
 * 4. There's the list of possible scenarios of cache layer usages (not complete though).
 * Contents:
 * - open - close file. Succeeds in case if file handle != NULL and the handle was closed successfully.
 * - open - read - compare with model - close file. Succeeds if the whole scenario went without assertions.
 * - open - seek - read - compare - close. Succeeds if the whole scenario went without assertions.
 * - create if not exists - write - close - open - compare - close.
 * Succeeds in case if the file written by the model and the file written by the cache layer are byte-identical.
 *
 * 4. Each client runs the scenario selected in the random way.
 * 5. Each client is being involved into particular scenario 1K times
 */
TEST_F(CacheLayerTest, SporadicFileSporadicTestScenarioHeavyLoadManagedAsync) {

	std::vector<std::string> dataset;
	// rescan the dataset in order to get the collection of filenames.
	rescan_dataset(m_dataset_path.c_str(), dataset);
	ASSERT_TRUE(dataset.size() != 0);

	// create the list of scenario to run within the test :
	std::vector< ScenarioCase > scenarios;

	// add "open-close" scenario:
	scenarios.push_back({boost::bind(close_open_file_sporadic, _1, _2, _3, _4, _5, _6, _7),
		"Close-Open-Sporadic"});

	// add "open-read-compare-byte-by-byte" scenario:
    scenarios.push_back({boost::bind(open_read_compare_close_file_sporadic, _1, _2, _3, _4, _5, _6, _7),
    	"Open-Read-Compare-Close-Sporadic"});

	// add "file seek" scenario:
    scenarios.push_back({boost::bind(open_seek_read_compare_close_file_sporadic, _1, _2, _3, _4, _5, _6, _7),
    	"Open-Read-Seek-Compare-Close"});

	// add "file write" scenario:


	cacheInit(constants::TEST_CACHE_DEFAULT_FREE_SPACE_PERCENT, m_cache_path,
			boost::posix_time::hours(-1), constants::TEST_CACHE_FIXED_SIZE);
	cacheConfigureFileSystem(m_dfsIdentitylocalFilesystem);

	// get the connection to local file system:
	FileSystemDescriptorBound fsAdaptor(m_dfsIdentitylocalFilesystem);
	raiiDfsConnection conn = fsAdaptor.getFreeConnection();
	ASSERT_TRUE(conn.connection() != NULL);

	std::cout << "Localhost filesystem adaptor is ready" << std::endl;

	const int CONTEXT_NUM = 50;
    const int ITERATIONS = 100;

	using namespace std::placeholders;

	auto f1 = std::bind(&run_random_scenario, ph::_1, ph::_2, ph::_3, ph::_4, ph::_5, ph::_6, ph::_7, ph::_8, ph::_9);

	std::vector<std::future<void>> futures;

	// go with workers
	for (int i = 0; i < CONTEXT_NUM; i++) {
		futures.push_back(
				std::move(
						spawn_task(f1, std::cref(scenarios),
								std::cref(m_dfsIdentitylocalFilesystem),
								std::cref(constants::TEST_LOCALFS_PROTO_PREFFIX),
								std::cref(dataset),
								std::ref(m_direct_handles),
								std::ref(m_cached_handles),
								std::ref(m_zero_handles),
								std::ref(m_total_handles),
								ITERATIONS)));
	}

	for (int i = 0; i < CONTEXT_NUM; i++) {
		if (futures[i].valid())
			futures[i].get();
	}

	EXPECT_EQ(futures.size(), CONTEXT_NUM);
}

/**
 * Simultaneous file request arriving from 50 clients
 *
 * Scenario :
 * 0. Cache is set with a size = 1.5 cache limit.
 * 1. 50 requests are issued, each for the same file from the dataset.
 * All clients are being fired simultaneously.
 * 2. Test succeeds in case if all clients were able to complete scenario "open/close" with the same file.
 */
TEST_F(CacheLayerTest, OpenCloseHeavyLoadManagedAsync) {
	boost::system::error_code ec;

	std::string data_location = m_dataset_path + constants::TEST_SINGLE_FILE_FROM_DATASET;
	char path[256];
	memset(path, 0, 256);
	data_location.copy(path, data_location.length() + 1, 0);

	ASSERT_TRUE(boost::filesystem::exists(path, ec));

	char filename[256];
	memset(filename, 0, 256);
	data_location = constants::TEST_LOCALFS_PROTO_PREFFIX + data_location;
	data_location.copy(filename, data_location.length() + 1, 0);


	std::cout << "Test data is validated and is ready";

	cacheInit(constants::TEST_CACHE_DEFAULT_FREE_SPACE_PERCENT, m_cache_path,
			boost::posix_time::hours(-1), constants::TEST_CACHE_FIXED_SIZE);
	cacheConfigureFileSystem(m_dfsIdentitylocalFilesystem);

	// get the connection to local file system:
	FileSystemDescriptorBound fsAdaptor(m_dfsIdentitylocalFilesystem);
	raiiDfsConnection conn = fsAdaptor.getFreeConnection();
	ASSERT_TRUE(conn.connection() != NULL);

	std::cout << "Localhost filesystem adaptor is ready";

	const int CONTEXT_NUM = 50;

	using namespace std::placeholders;

	auto f1 = std::bind(&close_open_file, ph::_1, ph::_2, ph::_3, ph::_4, ph::_5, ph::_6);

	std::vector<std::future<void>> futures;
	for (int i = 0; i < CONTEXT_NUM; i++) {
		futures.push_back(
				std::move(
						spawn_task(f1, filename, std::ref(m_dfsIdentitylocalFilesystem),
								std::ref(m_direct_handles),
								std::ref(m_cached_handles),
								std::ref(m_zero_handles),
								std::ref(m_total_handles))));
	}

	for (int i = 0; i < CONTEXT_NUM; i++) {
		if (futures[i].valid())
			futures[i].get();
	}

	EXPECT_EQ(futures.size(), CONTEXT_NUM);
}

/**
 * Simple test that check dfsCopy API.
 *
 * Scenario:
 * 0. Cache is configured with any size.
 * 1. dfsCopy API is used in order to copy the file from dataset location to the cache.
 * 2. Test succeeds if the API succeeds + byte-by-byte source and destination file read
 * comparison succeeds.
 */
TEST_F(CacheLayerTest, TestCopyRemoteFileToLocal){
	boost::system::error_code ec;

	std::string src_location = m_dataset_path + constants::TEST_SINGLE_FILE_FROM_DATASET;
	char src[256];
	memset(src, 0, 256);
	src_location.copy(src, src_location.length() + 1, 0);

	ASSERT_TRUE(boost::filesystem::exists(src, ec));

	std::cout << "Test data is validated and is ready";

	cacheInit(constants::TEST_CACHE_DEFAULT_FREE_SPACE_PERCENT, m_cache_path,
			boost::posix_time::hours(-1), constants::TEST_CACHE_FIXED_SIZE);
	cacheConfigureFileSystem(m_dfsIdentitylocalFilesystem);

	// get the connection to local file system:
	FileSystemDescriptorBound fsAdaptor(m_dfsIdentitylocalFilesystem);
	raiiDfsConnection conn = fsAdaptor.getFreeConnection();
	ASSERT_TRUE(conn.connection() != NULL);

	std::cout << "Localhost filesystem adaptor is ready";

	std::string dst_location = m_cache_path + constants::TEST_SINGLE_FILE_FROM_DATASET;
	char dst[256];
	memset(dst, 0, 256);
	dst_location.copy(dst, dst_location.length() + 1, 0);



	// read both files and compare byte-by-byte their contents:
	dfsFile source      = nullptr;
	dfsFile destination = nullptr;

#define BUFFER_SIZE 17408

	// format source and destination filenames to fit the fs adaptor:
	std::string path_source(src);
	path_source = path_source.insert(0, constants::TEST_LOCALFS_PROTO_PREFFIX + "/");

	std::string path_dest(dst);
	path_dest = path_dest.insert(0, constants::TEST_LOCALFS_PROTO_PREFFIX + "/");

	status::StatusInternal status = dfsCopy(m_dfsIdentitylocalFilesystem, path_source.c_str(), m_dfsIdentitylocalFilesystem, path_dest.c_str());
		ASSERT_TRUE(status == status::StatusInternal::OK);

	// open "source" file:
	source = fsAdaptor.fileOpen(conn, path_source.c_str(), O_RDONLY, 0, 0, 0);
	ASSERT_TRUE(source != NULL);

	// open "destination" file:
	destination = fsAdaptor.fileOpen(conn, path_dest.c_str(), O_RDONLY, 0, 0, 0);
	ASSERT_TRUE(destination != NULL);

	// now read by blocks and compare:
	tSize last_read_local = 0;
	tSize last_read_remote = 0;

	char* buffer_remote = (char*)malloc(sizeof(char) * BUFFER_SIZE);
	char* buffer_local = (char*)malloc(sizeof(char) * BUFFER_SIZE);

	last_read_remote = fsAdaptor.fileRead(conn, source, (void*)buffer_remote, BUFFER_SIZE);
	last_read_local = fsAdaptor.fileRead(conn, destination, (void*)buffer_local, BUFFER_SIZE);

	ASSERT_TRUE(last_read_remote == last_read_local);

	for (; last_read_remote > 0;) {
		// check read bytes count is equals:
		ASSERT_TRUE(last_read_remote == last_read_local);
		// compare memory contents we read from files:
		ASSERT_TRUE((std::memcmp(buffer_remote, buffer_local, last_read_remote) == 0));

		// read next data buffer:
		last_read_remote = fsAdaptor.fileRead(conn, source, (void*)buffer_remote, BUFFER_SIZE);
		last_read_local = fsAdaptor.fileRead(conn, destination, (void*)buffer_local, BUFFER_SIZE);
	}

	free(buffer_remote);
	free(buffer_local);

	// close file handles, local and remote:
	ASSERT_TRUE(fsAdaptor.fileClose(conn, source) == 0);
	ASSERT_TRUE(fsAdaptor.fileClose(conn, destination) == 0);
}

/**
 * Test opening of non-existing file.
 * Test succeeds in case if file handle is NULL and file is marked as "not available"
 */
TEST_F(CacheLayerTest, OpenNonExistingFile) {
	boost::system::error_code ec;

	// compose non-existing path:
	std::string data_location = m_dataset_path + constants::TEST_SINGLE_FILE_FROM_DATASET + "_";
	char path[256];
	memset(path, 0, 256);
	data_location.copy(path, data_location.length() + 1, 0);

	char filename[256];
	memset(filename, 0, 256);
	data_location = constants::TEST_LOCALFS_PROTO_PREFFIX + "/" + data_location;
	data_location.copy(filename, data_location.length() + 1, 0);

    std::cout << "Test data is validated and is ready" << std::endl;

	cacheInit(constants::TEST_CACHE_DEFAULT_FREE_SPACE_PERCENT, m_cache_path,
			boost::posix_time::hours(-1), constants::TEST_CACHE_FIXED_SIZE);
	cacheConfigureFileSystem(m_dfsIdentitylocalFilesystem);

	bool available;
	dfsFile file = dfsOpenFile(m_dfsIdentitylocalFilesystem, filename, O_RDONLY, 0, 0, 0, available);
    // check that the file handle is not available
	ASSERT_TRUE(!available && (file == NULL));
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

	// check dataset size it least of 1.5 of configured cache size:
    boost::uintmax_t dataset_size = utilities::get_dir_busy_space(m_dataset_path.c_str());
    double overlap_ratio = 1.5;
    ASSERT_TRUE(dataset_size / overlap_ratio >= constants::TEST_CACHE_FIXED_SIZE);

    std::cout << "Dataset is validated and is ready";

	cacheInit(constants::TEST_CACHE_DEFAULT_FREE_SPACE_PERCENT, constants::TEST_CACHE_DEFAULT_LOCATION,
			boost::posix_time::hours(-1), constants::TEST_CACHE_FIXED_SIZE);
	cacheConfigureFileSystem(m_dfsIdentitylocalFilesystem);

	// get the connection to local file system:
	FileSystemDescriptorBound fsAdaptor(m_dfsIdentitylocalFilesystem);
	raiiDfsConnection conn = fsAdaptor.getFreeConnection();
	ASSERT_TRUE(conn.connection() != NULL);

	std::cout << "Localhost filesystem adaptor is ready";

	// get the list of all files within the specified dataset:
	int entries;
    dfsFileInfo* files = fsAdaptor.listDirectory(conn, m_dataset_path.c_str(), &entries);
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
			file = dfsOpenFile(m_dfsIdentitylocalFilesystem, path.c_str(), O_RDONLY, 0, 0, 0, available);
			collectFileHandleStat(file, m_direct_handles, m_cached_handles, m_zero_handles, m_total_handles);
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
			last_read_local = dfsRead(m_dfsIdentitylocalFilesystem, file, (void*)buffer_local, BUFFER_SIZE);

			ASSERT_TRUE(last_read_remote == last_read_local);

			for (; last_read_remote > 0;) {
				// check read bytes count is equals:
				ASSERT_TRUE(last_read_remote == last_read_local);
				// compare memory contents we read from files:
				ASSERT_TRUE((std::memcmp(buffer_remote, buffer_local, last_read_remote) == 0));

				// read next data buffer:
				last_read_remote = fsAdaptor.fileRead(conn, remotefile, (void*)buffer_remote, BUFFER_SIZE);
				last_read_local = dfsRead(m_dfsIdentitylocalFilesystem, file, (void*)buffer_local, BUFFER_SIZE);
			}

			free(buffer_remote);
			free(buffer_local);

			// close file handles, local and remote:
			ASSERT_TRUE(fsAdaptor.fileClose(conn, remotefile) == 0);
			ASSERT_TRUE(dfsCloseFile(m_dfsIdentitylocalFilesystem, file) == 0);
		}
	};

	// number of retries may be increased
	for(int iteration = 0; iteration < 2; iteration++)
		scenario();

	std::cout << "Test is near to complete, cleanup...";

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
TEST_F(CacheLayerTest, TestCacheAgebucketSpanReduction){
	// age bucket time slice:
	m_timeslice = constants::TEST_CACHE_REDUCED_TIMESLICE;

	// check dataset size it least of 1.5 of configured cache size:
    boost::uintmax_t dataset_size = utilities::get_dir_busy_space(m_dataset_path.c_str());
    double overlap_ratio = 1.5;
    ASSERT_TRUE(dataset_size / overlap_ratio >= constants::TEST_CACHE_FIXED_SIZE);

    std::cout << "Dataset is validated and is ready";

	// Initialize cache with 1 Mb
	cacheInit(constants::TEST_CACHE_DEFAULT_FREE_SPACE_PERCENT, constants::TEST_CACHE_DEFAULT_LOCATION,
			boost::posix_time::seconds(m_timeslice), constants::TEST_CACHE_FIXED_SIZE);

	// configure local filesystem:
	cacheConfigureFileSystem(m_dfsIdentitylocalFilesystem);

	// get the connection to local file system:
	FileSystemDescriptorBound fsAdaptor(m_dfsIdentitylocalFilesystem);
	raiiDfsConnection conn = fsAdaptor.getFreeConnection();
	ASSERT_TRUE(conn.connection() != NULL);

	std::cout << "Localhost filesystem adaptor is ready";

	// get the list of all files within the specified dataset:
	int entries;
    dfsFileInfo* files = fsAdaptor.listDirectory(conn, m_dataset_path.c_str(), &entries);
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
    	std::string path(files[i].mName);
    	path = path.insert(path.find_first_of("/"), "/");

    	file = dfsOpenFile(m_dfsIdentitylocalFilesystem, path.c_str() , O_RDONLY, 0, 0, 0, available);
    	collectFileHandleStat(file, m_direct_handles, m_cached_handles, m_zero_handles, m_total_handles);
    	ASSERT_TRUE((file != NULL) && available);

    	// increase cached data size
    	cached_data_size += files[i].mSize;

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
    	last_read_local = dfsRead(m_dfsIdentitylocalFilesystem, file, (void*)buffer_local, BUFFER_SIZE);

    	ASSERT_TRUE(last_read_remote == last_read_local);

    	for (; last_read_remote > 0;) {
    		// check read bytes count is equals:
        	ASSERT_TRUE(last_read_remote == last_read_local);
        	// compare memory contents we read from files:
        	ASSERT_TRUE((std:: memcmp(buffer_remote, buffer_local, last_read_remote) == 0));

    		// read next data buffer:
    		last_read_remote = fsAdaptor.fileRead(conn, remotefile, (void*)buffer_remote, BUFFER_SIZE);
    		last_read_local = dfsRead(m_dfsIdentitylocalFilesystem, file, (void*)buffer_local, BUFFER_SIZE);
    	}

		free(buffer_remote);
		free(buffer_local);
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
    			ASSERT_TRUE(dfsCloseFile(m_dfsIdentitylocalFilesystem, file) == 0);
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
    ASSERT_TRUE(dfsCloseFile(m_dfsIdentitylocalFilesystem, file) == 0);

    // here, we passed the cache cleanup already.
    // close preserved file and check we can access it.
    ASSERT_TRUE(preserved_handle != nullptr);
    ASSERT_TRUE(dfsCloseFile(m_dfsIdentitylocalFilesystem, preserved_handle) == 0);

    std::cout << "Going to run comparison for preserved file";

    // run the file comparison for preserved iteration:
    scenario(preserved_iteration);

    std::cout << "Going to run second dataset validation iteration";

    // and re-run all dataset check completely (another one cache reload):
    for(int i = 0 ; i < entries; i++){
    	scenario(i);

        ASSERT_TRUE(remotefile != nullptr);
        ASSERT_TRUE(fsAdaptor.fileClose(conn, remotefile) == 0);

        ASSERT_TRUE(file != nullptr);
        ASSERT_TRUE(dfsCloseFile(m_dfsIdentitylocalFilesystem, file) == 0);
    }

    std::cout << "Test is near to complete, cleanup...";

    // free file info:
    fsAdaptor.freeFileInfo(files, entries);
}

TEST_F(CacheLayerTest, TestOverloadedCacheAddNewItem){

	// check dataset size it least of 1.5 of configured cache size:
    boost::uintmax_t dataset_size = utilities::get_dir_busy_space(m_dataset_path.c_str());
    double overlap_ratio = 1.5;
    ASSERT_TRUE(dataset_size / overlap_ratio >= constants::TEST_CACHE_FIXED_SIZE);

    std::cout << "Dataset is validated and is ready";

	cacheInit(constants::TEST_CACHE_DEFAULT_FREE_SPACE_PERCENT, constants::TEST_CACHE_DEFAULT_LOCATION,
			boost::posix_time::hours(-1), constants::TEST_CACHE_FIXED_SIZE);
	cacheConfigureFileSystem(m_dfsIdentitylocalFilesystem);

	// get the connection to local file system:
	FileSystemDescriptorBound fsAdaptor(m_dfsIdentitylocalFilesystem);
	raiiDfsConnection conn = fsAdaptor.getFreeConnection();
	ASSERT_TRUE(conn.connection() != NULL);

	std::cout << "Localhost filesystem adaptor is ready";

	// get the list of all files within the specified dataset:
	int entries;
    dfsFileInfo* files = fsAdaptor.listDirectory(conn, m_dataset_path.c_str(), &entries);
    ASSERT_TRUE((files != NULL) && (entries != 0));
    for(int i = 0; i < entries; i++){
    	std::cout << files[i].mName << std::endl;
    }

    dfsFile file       = nullptr;
    dfsFile remotefile = nullptr;

    // data size that is already cached
    tSize cached_data_size = 0;

    // create the storage for opened handles, at least of the whole dataset size:
    std::vector<dfsFile> opened_handles(entries);

    // we need to run twice the functor below to check the cache alive after the cleanup
	boost::function<void(int i)> scenario_open = [&](int i) {
		std::string path(files[i].mName);
		path = path.insert(path.find_first_of("/"), "/");

		// open file, say its local one:
		bool available;
		file = dfsOpenFile(m_dfsIdentitylocalFilesystem, path.c_str(), O_RDONLY, 0, 0, 0, available);
		collectFileHandleStat(file, m_direct_handles, m_cached_handles, m_zero_handles, m_total_handles);
		ASSERT_TRUE((file != NULL) && available);

		// preserve opened handle
		opened_handles.push_back(file);

		// increase cached data size
		cached_data_size += files[i].mSize;

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
		last_read_local = dfsRead(m_dfsIdentitylocalFilesystem, file, (void*)buffer_local, BUFFER_SIZE);

		ASSERT_TRUE(last_read_remote == last_read_local);

		for (; last_read_remote > 0;) {
			// check read bytes count is equals:
			ASSERT_TRUE(last_read_remote == last_read_local);
			// compare memory contents we read from files:
			ASSERT_TRUE((std::memcmp(buffer_remote, buffer_local, last_read_remote) == 0));

			// read next data buffer:
			last_read_remote = fsAdaptor.fileRead(conn, remotefile, (void*)buffer_remote, BUFFER_SIZE);
			last_read_local = dfsRead(m_dfsIdentitylocalFilesystem, file, (void*)buffer_local, BUFFER_SIZE);
		}

		free(buffer_remote);
		free(buffer_local);

		// close only "target" handle
		ASSERT_TRUE(fsAdaptor.fileClose(conn, remotefile) == 0);
	};
	for(int i = 0 ; i < entries; i++){
	    	// check whether we near to reach the cache size limit on this iteration:
	    	if((cached_data_size + files[i].mSize) > constants::TEST_CACHE_FIXED_SIZE){
	    		// and check that the next opened handle will be NULL (because the cache is overloaded with
	    		// a data "in use":

	    		std::string path(files[i].mName);
	    		path = path.insert(path.find_first_of("/"), "/");
  				bool available;

  				file = dfsOpenFile(m_dfsIdentitylocalFilesystem, path.insert(path.find_first_of("/"), "/").c_str(), O_RDONLY, 0, 0, 0, available);
  				collectFileHandleStat(file, m_direct_handles, m_cached_handles, m_zero_handles, m_total_handles);
	    		// check that we got direct handle to the file:
	    		ASSERT_TRUE((file != NULL) && available && file->direct);

	    		remotefile = fsAdaptor.fileOpen(conn, path.insert(path.find_first_of("/"), "/").c_str(), O_RDONLY, 0, 0, 0);
	    		ASSERT_TRUE(remotefile != NULL);

	    		// so read from opened handle and compare byte-by-byte read data with the Model adaptor read:
	    		// now read by blocks and compare:
	    		tSize last_read_local = 0;
	    		tSize last_read_remote = 0;

	    		char* buffer_remote = (char*)malloc(sizeof(char) * BUFFER_SIZE);
	    		char* buffer_local = (char*)malloc(sizeof(char) * BUFFER_SIZE);

	    		last_read_remote = fsAdaptor.fileRead(conn, remotefile, (void*)buffer_remote, BUFFER_SIZE);
	    		last_read_local = dfsRead(m_dfsIdentitylocalFilesystem, file, (void*)buffer_local, BUFFER_SIZE);

	    		ASSERT_TRUE(last_read_remote == last_read_local);

	    		for (; last_read_remote > 0;) {
	    			// check read bytes count is equals:
	    			ASSERT_TRUE(last_read_remote == last_read_local);
	    			// compare memory contents we read from files:
	    			ASSERT_TRUE((std::memcmp(buffer_remote, buffer_local, last_read_remote) == 0));

	    			// read next data buffer:
	    			last_read_remote = fsAdaptor.fileRead(conn, remotefile, (void*)buffer_remote, BUFFER_SIZE);
	    			last_read_local = dfsRead(m_dfsIdentitylocalFilesystem, file, (void*)buffer_local, BUFFER_SIZE);
	    		}

				free(buffer_remote);
				free(buffer_local);

	    		// close both handles:
	    		ASSERT_TRUE(dfsCloseFile(m_dfsIdentitylocalFilesystem, file) == 0);
	    		ASSERT_TRUE(fsAdaptor.fileClose(conn, remotefile) == 0);
	    		break;
	    	}
	    	// run the files opening and comparison
	    	scenario_open(i);
	    }

	// close all file handles we preserved before:
	for(auto handle : opened_handles)
		if(handle != NULL)
			ASSERT_TRUE(dfsCloseFile(m_dfsIdentitylocalFilesystem, handle) == 0);

	std::cout << "Test is near to complete, cleanup...";

    // free file info:
    fsAdaptor.freeFileInfo(files, entries);
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();

}
