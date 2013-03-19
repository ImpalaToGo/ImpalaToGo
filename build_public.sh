#!/usr/bin/env bash
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

# Builds the Cloudera Impala frontend and backend. 
# You must have run thirdparty/download_thirdparty.sh before this script

. bin/impala-config.sh

# Exit on non-true return value
set -e
# Exit on reference to uninitialized variable
set -u

BUILD_THIRDPARTY=0
TARGET_BUILD_TYPE=Debug

for ARG in $*
do
  case "$ARG" in
    -build_thirdparty)
      BUILD_THIRDPARTY=1
    ;;
  esac
done

if [ ! -f ${IMPALA_HOME}/thirdparty/gflags-${IMPALA_GFLAGS_VERSION}/.libs/libgflags.so.2 ]
then
  BUILD_THIRDPARTY=1
fi

if [ ${BUILD_THIRDPARTY} -eq 1 ]
then
  echo "********************************"
  echo " Building third-party libraries "
  echo "********************************"
  ${IMPALA_HOME}/bin/build_thirdparty.sh -noclean
  . bin/impala-config.sh
fi

echo "******************************"
echo " Building Impala backend "
echo "******************************"
# build common and backend
cd $IMPALA_HOME
${IMPALA_HOME}/bin/gen_build_version.py
cmake -DCMAKE_BUILD_TYPE=$TARGET_BUILD_TYPE .
cd $IMPALA_HOME/common/function-registry
make
cd $IMPALA_HOME/common/thrift
make
cd $IMPALA_BE_DIR
make -j4

# build frontend
echo "******************************"
echo " Building Impala frontend "
echo "******************************"
cd $IMPALA_FE_DIR
mvn package -DskipTests=true
