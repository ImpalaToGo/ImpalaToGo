/*
 * test-cache-mgr.cc
 *
 *  Created on: Oct 5, 2014
 *      Author: elenav
 */
// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <string>
#include <gtest/gtest.h>
#include <boost/thread.hpp>

#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>

#include "filesystem-mgr.hpp"
#include "gtest-fixtures.hpp"

using namespace boost;
using namespace std;

namespace impala {

using namespace filemgmt;

FileSystemManager fileMgr;  // emulate the situation with API-based libdfs-cache

TEST_F(FileMgrTest, OpenFileAndRead) {
	std::string clusterIdStr = "cluster x";
	NameNodeDescriptor namenode{dfs::DFS_TYPE::OTHER, "localhost", 8080, "", "", true};
    const char* path = "/home/elenav/src/ImpalaToGo/be/src/dfs_cache/test_data/hello.txt";
    int flags =  O_WRONLY | O_CREAT | O_EXCL;
    int buffer_size = 0;
    short int replication = 0;
    size_t block_size = 0;
    bool available;

    dfsFile file = fileMgr.dfsOpenFile(namenode, path, flags, buffer_size, replication, block_size, available);

    EXPECT_TRUE(available);
    EXPECT_FALSE(file == nullptr);
}

/*
TEST(FileSystemMgrTest, TestAndSet) {
  AtomicInt<int> i1;
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(i + 1, i1.UpdateAndFetch(1));
  }

  i1 = 0;

  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(i, i1.FetchAndUpdate(1));
  }
}

// Basic multi-threaded testing
typedef boost::function<void (int64_t, int64_t , AtomicInt<int>*)> Fn;

void IncrementThread(int64_t id, int64_t n, AtomicInt<int>* ai) {
  for (int64_t i = 0; i < n * id; ++i) {
    ++*ai;
  }
}

void DecrementThread(int64_t id, int64_t n, AtomicInt<int>* ai) {
  for (int64_t i = 0; i < n * id; ++i) {
    --*ai;
  }
}

TEST(FileSystemMgrTest, MultipleThreadsIncDec) {
  thread_group increments, decrements;
  vector<int> ops;
  ops.push_back(1000);
  ops.push_back(10000);
  vector<int> num_threads;
  num_threads.push_back(4);
  num_threads.push_back(8);
  num_threads.push_back(16);

  for (vector<int>::iterator thrit = num_threads.begin(); thrit != num_threads.end();
       ++thrit) {
    for (vector<int>::iterator opit = ops.begin(); opit != ops.end(); ++opit) {
      AtomicInt<int> ai = 0;
      for (int i = 0; i < *thrit; ++i) {
        increments.add_thread( new thread(IncrementThread, i, *opit, &ai));
        decrements.add_thread( new thread(DecrementThread, i, *opit, &ai));
      }
      increments.join_all();
      decrements.join_all();
      EXPECT_EQ(ai, 0);
    }
  }
}

void CASIncrementThread(int64_t id, int64_t n, AtomicInt<int>* ai) {
  int oldval = 0;
  int newval = 0;
  bool success = false;
  for (int64_t i = 0; i < n * id; ++i) {
    success = false;
    while ( !success ) {
      oldval = ai->Read();
      newval = oldval + 1;
      success = ai->CompareAndSwap(oldval, newval);
    }
  }
}

void CASDecrementThread(int64_t id, int64_t n, AtomicInt<int>* ai) {
  int oldval = 0;
  int newval = 0;
  bool success = false;
  for (int64_t i = 0; i < n * id; ++i) {
    success = false;
    while ( !success ) {
      oldval = ai->Read();
      newval = oldval - 1;
      success = ai->CompareAndSwap(oldval, newval);
    }
  }
}

TEST(FileSystemMgrTest, MultipleThreadsCASIncDec) {
  thread_group increments, decrements;
  vector<int> ops;
  ops.push_back(10);
  ops.push_back(10000);
  vector<int> num_threads;
  num_threads.push_back(4);
  num_threads.push_back(8);
  num_threads.push_back(16);

  for (vector<int>::iterator thrit = num_threads.begin(); thrit != num_threads.end();
       ++thrit) {
    for (vector<int>::iterator opit = ops.begin(); opit != ops.end(); ++opit) {
      AtomicInt<int> ai = 0;
      for (int i = 0; i < *thrit; ++i) {
        increments.add_thread( new thread(CASIncrementThread, i, *opit, &ai));
        decrements.add_thread( new thread(CASDecrementThread, i, *opit, &ai));
      }
      increments.join_all();
      decrements.join_all();
      EXPECT_EQ(ai, 0);
    }
  }
}

}
*/
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();

}




