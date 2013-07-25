#!/bin/bash
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

if [ "x${IMPALA_HOME}" -eq "x"]; then
  echo "IMPALA_HOME must be set"
  exit 1;
fi

cd ${IMPALA_HOME}/thirdparty

echo "Removing everything in ${IMPALA_HOME}/thirdparty"
git clean -xdf

echo "Fetching gtest"
wget http://googletest.googlecode.com/files/gtest-${IMPALA_GTEST_VERSION}.zip
unzip gtest-${IMPALA_GTEST_VERSION}.zip
rm gtest-${IMPALA_GTEST_VERSION}.zip

echo "Fetching snappy"
wget http://snappy.googlecode.com/files/snappy-${IMPALA_SNAPPY_VERSION}.tar.gz
tar xzf snappy-${IMPALA_SNAPPY_VERSION}.tar.gz
rm snappy-${IMPALA_SNAPPY_VERSION}.tar.gz
