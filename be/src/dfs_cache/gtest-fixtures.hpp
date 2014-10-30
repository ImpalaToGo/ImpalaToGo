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


namespace impala{

/** Mock dfs adaptor implementation */
class TestDFSAdaptor : public RemoteAdaptor{
public:
	~TestDFSAdaptor() = default;

	int connect(boost::shared_ptr<dfsConnection> & conn) { return 0; }
	int disconnect(boost::shared_ptr<dfsConnection> & conn) { return 0; }
	int read(boost::shared_ptr<dfsConnection> & conn) { return 0; }
	int write(boost::shared_ptr<dfsConnection> & conn) { return 0; }
};

/** Fixture for Cache Manager tests */
class CacheMgrTest : public ::testing::Test {
 protected:
	static NameNodeDescriptor m_namenode1;   /**< namenode 1 */

	static SessionContext m_ctx1;  /**< session context 1 (shell/web client 1) */
	static SessionContext m_ctx2;  /**< session context 2 (shell/web client 2) */
	static SessionContext m_ctx3;  /**< session context 3 (shell/web client 3) */
	static SessionContext m_ctx4;  /**< session context 4 (shell/web client 4) */
	static SessionContext m_ctx5;  /**< session context 5 (shell/web client 5) */
	static SessionContext m_ctx6;  /**< session context 6 (shell/web client 6) */

	/** signaling we use in async tests */
	std::mutex m_mux;
	bool       m_flag;
	std::condition_variable m_condition;

  static void SetUpTestCase() {
	  impala::InitGoogleLoggingSafe("Test_dfs_cache");
	  impala::InitThreading();
	  cacheInit();

	  cacheInit();
	  cacheConfigureLocalStorage("/home/elenav/src/ImpalaToGo/be/src/dfs_cache/test_data/");
	  m_namenode1 = {dfs::DFS_TYPE::OTHER, "localhost", 8080, "", "", true};

	  boost::shared_ptr<dfsAdaptorFactory> factory(new dfsAdaptorFactory());
	  boost::shared_ptr<RemoteAdaptor> adaptor(new TestDFSAdaptor());
	  factory->addAdaptor(dfs::DFS_TYPE::OTHER, adaptor);

	  cacheConfigureDFSPluginFactory(factory);
	  // configure some test-purpose namenode:
	  cacheConfigureNameNode(m_namenode1);
  }

  // virtual void TearDown() {}

};
}

#endif /* GTEST_FIXTURES_HPP_ */
