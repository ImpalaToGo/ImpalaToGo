/*
 * gtest-fixtures.hpp
 *
 *  Created on: Oct 5, 2014
 *      Author: elenav
 */

#ifndef GTEST_FIXTURES_HPP_
#define GTEST_FIXTURES_HPP_

#include<boost/shared_ptr.hpp>
#include <boost/interprocess/smart_ptr/unique_ptr.hpp>
#include <gtest/gtest.h>

#include "common-include.hpp"
#include "filesystem-mgr.hpp"


namespace impala{
using namespace filemgmt;

extern FileSystemManager fileMgr;

class TestDFSAdaptor : public RemoteAdaptor{
protected:
	~TestDFSAdaptor() = default;

public:
	int connect(boost::shared_ptr<dfsConnection> & conn) { return 0; }
	int disconnect(boost::shared_ptr<dfsConnection> & conn) { return 0; }
	int read(boost::shared_ptr<dfsConnection> conn) { return 0; }
	int write(boost::shared_ptr<dfsConnection> conn) { return 0; }
};

class FileMgrTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
	  cacheInit();
	  cacheConfigureLocalStorage("/home/elenav/src/ImpalaToGo/be/src/dfs_cache/test_data/");
	  NameNodeDescriptor namenode{dfs::DFS_TYPE::OTHER, "localhost", 8080, "", "", true};

	  boost::shared_ptr<dfsAdaptorFactory> factory(new dfsAdaptorFactory());
	  boost::interprocess::unique_ptr<RemoteAdaptor, RemoteAdaptorDeleter> adaptor(new TestDFSAdaptor());
	  factory->addAdaptor(dfs::DFS_TYPE::OTHER, adaptor);

	  cacheConfigureDFSPluginFactory(factory);
	  cacheConfigureNameNode(namenode);
  }

  // virtual void TearDown() {}
};
}

#endif /* GTEST_FIXTURES_HPP_ */
