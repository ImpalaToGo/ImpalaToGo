/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.cli.log;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import com.google.common.base.Charsets;

public class OperationLog {

  private static final String HIVE_ENCODING = Charsets.UTF_8.name();

  // This OperationLogger's name is added to an exclusion list in OperationLogDivertAppender
  private static Logger LOG = Logger.getLogger(OperationLog.class.getName());

  private final String operationLogName;
  private final LinkedStringBuffer operationLogBuffer;
  private final long creationTime;

  OperationLog(String name, int size) {
    this.operationLogName = name;
    this.operationLogBuffer = new LinkedStringBuffer(size);
    this.creationTime = System.currentTimeMillis();
  }

  public void writeOperationLog(String OperationLogMessage) {
    operationLogBuffer.write(OperationLogMessage);
  }

  public String readOperationLog() {
    return operationLogBuffer.read();
  }

  public void resetOperationLog() {
    operationLogBuffer.clear();
  }

  /**
   * The OperationLogOutputStream helps translate a OperationLog to an OutputStream.
   */
  private static class OperationLogOutputStream extends OutputStream {
    private final LinkedStringBuffer backingStore;

    public OperationLogOutputStream(LinkedStringBuffer operationLogBuffer) {
      super();
      backingStore = operationLogBuffer;
    }

    @Override
    public void write(byte[] b) throws IOException {
      backingStore.write(new String(b, HIVE_ENCODING));
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      backingStore.write(new String(b, off, len, HIVE_ENCODING));
    }

    @Override
    public void write(int b) throws IOException {
      byte[] buf = { (byte) b };
      this.write(buf);
    }
  }

  public OutputStream getOutputStream() {
    return new OperationLogOutputStream(operationLogBuffer);
  }

  public String getName() {
    return operationLogName;
  }
}