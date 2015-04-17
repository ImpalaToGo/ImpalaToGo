/**
 * @file gtest-fixtures.hpp
 * @brief contains fixtures for cache layer tests
 *
 * @author elenav
 * @date Oct 29, 2014
 */

#ifndef GTEST_FIXTURES_HPP_
#define GTEST_FIXTURES_HPP_

#include <boost/shared_ptr.hpp>
#include <boost/interprocess/smart_ptr/unique_ptr.hpp>
#include <gtest/gtest.h>

#include <thread>
#include <mutex>
#include <condition_variable>
#include <utility>
#include <cstdlib>

#include "dfs_cache/dfs-cache.h"
#include "dfs_cache/filesystem-mgr.hpp"
#include "dfs_cache/test-utilities.hpp"

namespace impala{

/** Fixture for Cache Manager tests */
class CacheLayerTest : public ::testing::Test {
 protected:
	static FileSystemDescriptor m_dfsIdentityDefault;         /**< default file system as from core-site.xml */
	static FileSystemDescriptor m_dfsIdentitylocalFilesystem; /**< local file system */
	static FileSystemDescriptor m_dfsIdentityTachyon;         /**< tachyon file system */

	static SessionContext m_ctx1;  /**< session context 1 (shell/web client 1) */
	static SessionContext m_ctx2;  /**< session context 2 (shell/web client 2) */
	static SessionContext m_ctx3;  /**< session context 3 (shell/web client 3) */
	static SessionContext m_ctx4;  /**< session context 4 (shell/web client 4) */
	static SessionContext m_ctx5;  /**< session context 5 (shell/web client 5) */
	static SessionContext m_ctx6;  /**< session context 6 (shell/web client 6) */

	std::string  m_cache_path;    /**< cache location */
	std::string  m_dataset_path;  /**< origin dataset location */

	/**< Age buckets management timeslice duration, in seconds */
	int m_timeslice;

	/** signaling we use in async tests */
	std::mutex m_mux;
	bool       m_flag;
	std::condition_variable m_condition;

	/**< number of file handles opened directly from the target */
	std::atomic<long> m_direct_handles;

	/**< number of file handles opened from cache */
	std::atomic<long> m_cached_handles;

	/**< total number of any file handles opened during the test */
	std::atomic<long> m_total_handles;

	/**< number of zero handles received during the test */
	std::atomic<long> m_zero_handles;

    static void SetUpTestCase() {
  	  impala::InitGoogleLoggingSafe("Test_dfs_cache");
  	  impala::InitThreading();

	  m_dfsIdentityDefault.dfs_type = DFS_TYPE::NON_SPECIFIED;
	  m_dfsIdentityDefault.host = "default";
	  m_dfsIdentityDefault.port = 0;
	  m_dfsIdentityDefault.credentials = "";
	  m_dfsIdentityDefault.password = "";
	  m_dfsIdentityDefault.valid = true;

	  m_dfsIdentitylocalFilesystem.dfs_type = DFS_TYPE::local;
	  m_dfsIdentitylocalFilesystem.host = "";
	  m_dfsIdentitylocalFilesystem.port = 0;
	  m_dfsIdentitylocalFilesystem.credentials = "";
	  m_dfsIdentitylocalFilesystem.password = "";
	  m_dfsIdentitylocalFilesystem.valid = true;

	  m_dfsIdentityTachyon.dfs_type = DFS_TYPE::tachyon;
	  m_dfsIdentityTachyon.host = "localhost";
	  m_dfsIdentityTachyon.port = 19998;
	  m_dfsIdentityTachyon.credentials = "";
	  m_dfsIdentityTachyon.password = "";
	  m_dfsIdentityTachyon.valid = true;

	  // reset session contexts
	  m_ctx1 = nullptr;
	  m_ctx2 = nullptr;

    }

    inline void printStat(){
  	  std::cout << "Tear down\n***********************\n Total number of opened file handles : \"" <<
  			  std::to_string(m_total_handles.load()) << "\";\n" <<
  			  "Number of direct file handles : \"" <<
  			  std::to_string(m_direct_handles.load()) << "\";\n" <<
  			  "Number of zero handles : \"" <<
  			  std::to_string(m_zero_handles.load()) << "\";\n" <<
  			  "Number of cached handles : \"" <<
  			  std::to_string(m_cached_handles.load()) << "\".";
    }
	virtual void SetUp() {
		// try to get the ${IMPALA_HOME} environment variable
		const char* env_v_name = constants::IMPALA_HOME_ENV_VARIABLE_NAME.c_str();
		const char* env_v = std::getenv(env_v_name);

		// assign the cache path location relatively to ${IMPALA_HOME}
		// if env variable is set.
		// Assign to default dataset location otherwise
		if(env_v){
			char buff[4096];
			sprintf(buff, "%s%s", env_v, constants::TEST_CACHE_DEFAULT_LOCATION.c_str());
			m_cache_path.assign(buff);
		}
		else
			m_cache_path = constants::TEST_CACHE_DEFAULT_LOCATION;

		// assign the dataset location relatively to ${IMPALA_HOME}
		// if env variable is set.
		// Assign to default dataset location otherwise
		if(env_v){
			char buff[4096];
			sprintf(buff, "%s/testdata/dfs_cache/", env_v);
			m_dataset_path.assign(buff);
		}
		else
			m_dataset_path = constants::TEST_DATASET_DEFAULT_LOCATION;

		boost::system::error_code ec;

		std::cout << "Reset the cache... \"" << m_cache_path << "\"";

		// clean cache directory before usage:
		boost::filesystem::remove_all(m_cache_path, ec);
		SCOPED_TRACE(ec.message());
	    ASSERT_TRUE(!ec);

		boost::filesystem::create_directory(m_cache_path, ec);
		ASSERT_TRUE(!ec);

		std::cout << "Check dataset location exist... \"" << m_dataset_path << "\"";
	    // first check working directories exist:
		ASSERT_TRUE(boost::filesystem::exists(m_dataset_path.c_str(), ec));
		SCOPED_TRACE(ec.message());
		ASSERT_TRUE(!ec);
		ASSERT_TRUE(boost::filesystem::exists(m_cache_path.c_str(), ec));
		SCOPED_TRACE(ec.message());
		ASSERT_TRUE(!ec);

		std::cout << "Working directories exist.";

		m_direct_handles.store(0l);
		m_cached_handles.store(0l);
		m_zero_handles.store(0l);
		m_total_handles.store(0l);
  }

  virtual void TearDown() {
	  // shutdown the cache
	  cacheShutdown();
	  // clean the cache:
	  boost::system::error_code ec;
	  boost::filesystem::remove_all(m_cache_path, ec);
	  ASSERT_TRUE(!ec);

	  // print some statistics:
      printStat();
  }

};
}

#endif /* GTEST_FIXTURES_HPP_ */
