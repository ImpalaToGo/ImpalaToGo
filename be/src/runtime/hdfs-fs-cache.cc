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

#include "runtime/hdfs-fs-cache.h"

#include <boost/thread/locks.hpp>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "util/debug-util.h"
#include "util/error-util.h"
#include "util/hdfs-util.h"
#include "util/test-info.h"

using namespace std;
using namespace boost;
using namespace strings;

namespace impala {

scoped_ptr<HdfsFsCache> HdfsFsCache::instance_;

void HdfsFsCache::Init() {
  DCHECK(HdfsFsCache::instance_.get() == NULL);
  HdfsFsCache::instance_.reset(new HdfsFsCache());
}

Status HdfsFsCache::GetConnection(const string& path, dfsFS* fs,
    HdfsFsMap* local_cache) {
  string dfs_identity;
  size_t n = path.find("://");
  if (n == string::npos) {
    if (path.compare(0, string::npos, "file:/", 6)) {
      // Hadoop Path routines strip out consecutive /'s, so recognize 'file:/blah'.
    	dfs_identity = "file:///";
    } else {
      // Path is not qualified, so use the default FS.
    	dfs_identity = "default";
    }
  } else {
    // Path is qualified, i.e. "scheme://authority/path/to/file".  Extract
    // "scheme://authority/".
    n = path.find('/', n + 3);
    if (n == string::npos) {
      return Status(Substitute("Path missing '/' after authority: $0", path));
    }
    // Include the trailling '/' for local filesystem case, i.e. "file:///".
    dfs_identity = path.substr(0, n + 1);
  }
  DCHECK(!dfs_identity.empty());
  // First, check the local cache to avoid taking the global lock.
  if (local_cache != NULL) {
    HdfsFsMap::iterator local_iter = local_cache->find(dfs_identity);
    if (local_iter != local_cache->end()) {
      *fs = local_iter->second;
      return Status::OK;
    }
  }
  // Otherwise, check the global cache.
  {
    lock_guard<mutex> l(lock_);
    HdfsFsMap::iterator i = fs_map_.find(dfs_identity);
    if (i == fs_map_.end()) {
    	// no connection exists.
    	dfsFS conn(dfs_identity);
    	conn.valid = true;
    	// run connection resolver and registration:
    	cacheConfigureFileSystem(conn);
    	fs_map_.insert(make_pair(dfs_identity, conn));
    	*fs = conn;
    } else {
      *fs = i->second;
    }
  }
  DCHECK(fs->valid);
  // Populate the local cache for the next lookup.
  if (local_cache != NULL) {
    local_cache->insert(make_pair(dfs_identity, *fs));
  }
  return Status::OK;
}

Status HdfsFsCache::GetLocalConnection(dfsFS* fs) {
  return GetConnection("file:///", fs);
}

}
