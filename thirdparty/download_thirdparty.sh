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

echo "Fetching glog"
wget http://google-glog.googlecode.com/files/glog-${IMPALA_GLOG_VERSION}.tar.gz
tar xzf glog-${IMPALA_GLOG_VERSION}.tar.gz
rm glog-${IMPALA_GLOG_VERSION}.tar.gz

echo "Fetching gflags"
wget http://gflags.googlecode.com/files/gflags-${IMPALA_GFLAGS_VERSION}.zip
unzip gflags-${IMPALA_GFLAGS_VERSION}.zip
rm gflags-${IMPALA_GFLAGS_VERSION}.zip

echo "Fetching gperftools"
wget http://gperftools.googlecode.com/files/gperftools-${IMPALA_GPERFTOOLS_VERSION}.tar.gz
tar xzf gperftools-${IMPALA_GPERFTOOLS_VERSION}.tar.gz
rm gperftools-${IMPALA_GPERFTOOLS_VERSION}.tar.gz

echo "Fetching snappy"
wget http://snappy.googlecode.com/files/snappy-${IMPALA_SNAPPY_VERSION}.tar.gz
tar xzf snappy-${IMPALA_SNAPPY_VERSION}.tar.gz
rm snappy-${IMPALA_SNAPPY_VERSION}.tar.gz

echo "Fetching cyrus-sasl"
wget ftp://ftp.andrew.cmu.edu/pub/cyrus-mail/cyrus-sasl-${IMPALA_CYRUS_SASL_VERSION}.tar.gz
tar xzf cyrus-sasl-${IMPALA_CYRUS_SASL_VERSION}.tar.gz
rm cyrus-sasl-${IMPALA_CYRUS_SASL_VERSION}.tar.gz

echo "Fetching mongoose"
wget http://mongoose.googlecode.com/files/mongoose-${IMPALA_MONGOOSE_VERSION}.tgz
tar xzf mongoose-${IMPALA_MONGOOSE_VERSION}.tgz
rm mongoose-${IMPALA_MONGOOSE_VERSION}.tgz

echo "Fetching Apache Hive"
wget http://archive.cloudera.com/cdh4/cdh/4/hive-${IMPALA_HIVE_VERSION}.tar.gz
tar xzf hive-${IMPALA_HIVE_VERSION}.tar.gz
rm hive-${IMPALA_HIVE_VERSION}.tar.gz

echo "Fetching Apache Thrift"
wget http://archive.apache.org/dist/thrift/${IMPALA_THRIFT_VERSION}/thrift-${IMPALA_THRIFT_VERSION}.tar.gz
tar xzf thrift-${IMPALA_THRIFT_VERSION}.tar.gz
rm thrift-${IMPALA_THRIFT_VERSION}.tar.gz
