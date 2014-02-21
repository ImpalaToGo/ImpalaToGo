#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
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
#
# This script waits for the Hive HiveServer2 service to become available by attempting
# to create a new session until the session creation succeeds, or a timeout is reached.
# TODO: Consider combining this with wait-for-metastore.py

import time
import getpass
from optparse import OptionParser
from tests.util.thrift_util import create_transport

# Imports required for HiveServer2 Client
from cli_service import TCLIService
from thrift.transport import TTransport, TSocket
from thrift.protocol import TBinaryProtocol

parser = OptionParser()
parser.add_option("--hs2_hostport", dest="hs2_hostport",
                  default="localhost:11050", help="HiveServer2 hostport to wait for.")
parser.add_option("--use_kerberos", action="store_true", default=False,
                  help="Indicates whether the cluster is kerberized.")
options, args = parser.parse_args()

hs2_host, hs2_port = options.hs2_hostport.split(':')
hs2_transport = create_transport(use_kerberos=options.use_kerberos, host=hs2_host,
                                 port=hs2_port, service="hiveserver2")
protocol = TBinaryProtocol.TBinaryProtocol(hs2_transport)
hs2_client = TCLIService.Client(protocol)

# Try to connect to the HiveServer2 service and create a session
now = time.time()
TIMEOUT_SECONDS = 30.0
while time.time() - now < TIMEOUT_SECONDS:
  try:
    hs2_transport.open()
    open_session_req = TCLIService.TOpenSessionReq()
    open_session_req.username = getpass.getuser()
    resp = hs2_client.OpenSession(open_session_req)
    if resp.status.statusCode == TCLIService.TStatusCode.SUCCESS_STATUS:
      close_session_req = TCLIService.TCloseSessionReq()
      close_session_req.sessionHandle = resp.sessionHandle
      hs2_client.CloseSession(close_session_req)
      print "HiveServer2 service is up at %s." % options.hs2_hostport
      exit(0)
  except Exception, e:
    print "Waiting for HiveServer2 at %s..." % options.hs2_hostport
  time.sleep(0.5)

print "HiveServer2 service failed to start within %s seconds." % TIMEOUT_SECONDS
exit(1)
