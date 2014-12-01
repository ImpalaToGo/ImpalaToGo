# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

message (STATUS "dfs_cache ${BUILD_OUTPUT_ROOT_DIRECTORY}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/dfs_cache")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/dfs_cache")

add_library(dfs_cache
  filesystem-mgr.cc
  tasks-impl.cc
  dfs-cache.cc
  sync-module.cc
  cache-mgr.cc
  cache-layer-registry.cc
  managed-file.cc
  filesystem-descriptor-bound.cc
  hadoop-fs-adaptive.c
  test-utilities.cc
  utilities.cc
  filesystem-lru-cache.cc
)

ADD_BE_TEST(test-filesystem-mgr)
ADD_BE_TEST(test-cache-manager)
ADD_BE_TEST(test-hadoop-fs-adaptive)
