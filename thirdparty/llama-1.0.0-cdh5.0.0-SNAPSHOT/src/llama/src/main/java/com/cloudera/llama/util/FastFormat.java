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
package com.cloudera.llama.util;

import org.slf4j.helpers.MessageFormatter;

/**
 * Utility class to format message using same syntax as slf4j log messages,
 * using '{}' for argument placement.
 */
public class FastFormat {

  /**
   * Returns a message created using the specified message pattern and
   * arguments.
   *
   * @param messagePattern message pattern, with '{}' for arguments placement
   * @param args arguments, they are applied to the message pattern in order.
   * @return the formatted message
   */
  public static String format(String messagePattern, Object... args) {
    return MessageFormatter.arrayFormat(messagePattern, args).getMessage();
  }

}
