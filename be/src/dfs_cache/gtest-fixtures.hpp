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

#include "dfs_cache/dfs-cache.h"
#include "dfs_cache/filesystem-mgr.hpp"
#include "dfs_cache/test-utilities.hpp"

namespace impala{

/** Fixture for Cache Manager tests */
class CacheLayerTest : public ::testing::Test {
 protected:
	static FileSystemDescriptor m_namenode1;               /**< file system 1 */
	static FileSystemDescriptor m_namenodeHdfs;            /**< file system hdfs */
	static FileSystemDescriptor m_namenodeDefault;         /**< default file system as from core-site.xml */
	static FileSystemDescriptor m_namenodelocalFilesystem; /**< local file system */

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

    static void SetUpTestCase() {
  	  impala::InitGoogleLoggingSafe("Test_dfs_cache");
  	  impala::InitThreading();

	  m_namenode1.dfs_type = DFS_TYPE::OTHER;
	  m_namenode1.host = "";
	  m_namenode1.port = 0;
	  m_namenode1.credentials = "";
	  m_namenode1.password = "";
	  m_namenode1.valid = true;

	  m_namenodeHdfs.dfs_type = DFS_TYPE::hdfs;
	  m_namenodeHdfs.host = "104.236.39.60";
	  m_namenodeHdfs.port = 8020;
	  m_namenodeHdfs.credentials = "";
	  m_namenodeHdfs.password = "";
	  m_namenodeHdfs.valid = true;

	  m_namenodeDefault.dfs_type = DFS_TYPE::NON_SPECIFIED;
	  m_namenodeDefault.host = "default";
	  m_namenodeDefault.port = 0;
	  m_namenodeDefault.credentials = "";
	  m_namenodeDefault.password = "";
	  m_namenodeDefault.valid = true;

	  m_namenodelocalFilesystem.dfs_type = DFS_TYPE::local;
	  m_namenodelocalFilesystem.host = "";
	  m_namenodelocalFilesystem.port = 0;
	  m_namenodelocalFilesystem.credentials = "";
	  m_namenodelocalFilesystem.password = "";
	  m_namenodelocalFilesystem.valid = true;

	  // reset session contexts
	  m_ctx1 = nullptr;
	  m_ctx2 = nullptr;

    }

	virtual void SetUp() {
		m_cache_path   = constants::TEST_CACHE_DEFAULT_LOCATION;
  }

  virtual void TearDown() {
	  // shutdown the cache
	  cacheShutdown();
  }

};
}

#endif /* GTEST_FIXTURES_HPP_ */
