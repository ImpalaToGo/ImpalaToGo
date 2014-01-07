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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public enum ErrorCode {
  TEST(0, "test error {}:{}"),

  INTERNAL_ERROR(1, "internal error"),

  ILLEGAL_ARGUMENT(10, "illegal argument"),

  CLIENT_REGISTERED_WITH_OTHER_CALLBACK(100, "clientId '{}' already registered with a different notification address {}"),
  CLIENT_INVALID_REGISTRATION(101, "clientId '{}' and notification address '{}' already used in two different active registrations '{}' and '{}'"),
  CLIENT_UNKNOWN_HANDLE(102, "unknown handle '{}' "),

  CLIENT_DOES_NOT_OWN_RESERVATION(150, "handle '{}' does not own reservation '{}'"),

  LLAMA_MAX_RESERVATIONS_FOR_QUEUE(160, "Queue '{}' reached its limit of '{}' queued reservations"),

  UNKNOWN_RESERVATION_FOR_EXPANSION(170, "Unknown reservation '{}' for expansion"),
  CANNOT_EXPAND_AN_EXPANSION_RESERVATION(171, "Cannot expand an expansion reservation '{}'"),
  CANNOT_EXPAND_A_RESERVATION_NOT_ALLOCATED(172, "Cannot expand reservation '{}' not in ALLOCATED status, current status '{}'"),
  AM_CANNOT_START(300, "cannot start AM"),
  AM_CANNOT_REGISTER(301, "cannot register AM '{}' for queue '{}'"),
  AM_CANNOT_CREATE(302, "cannot create AM for queue '{}'"),
  AM_TIMED_OUT_STARTING_STOPPING(303, "AM '{}' timed out ('{}' ms) in state '{}' transitioning to '{}' while '{}'"),
  AM_FAILED_WHILE_STARTING_STOPPING(304, "AM '{}' failed while '{}'"),
  AM_CANNOT_GET_NODES(305, "AM '{}' error in getNodes()"),
  AM_NODE_NOT_AVAILABLE(306, "AM '{}' node '{}' not available for '{}'"),
  AM_RESOURCE_OVER_MAX_CPUS(307, "AM '{}' resource request '{}' exceeds maximum cluster CPUs '{}' for a resource"),
  AM_RESOURCE_OVER_MAX_MEMORY(308, "AM '{}' resource request '{}' exceeds maximum cluster memory '{}' for a resource"),
  AM_RESOURCE_OVER_NODE_CPUS(309, "AM '{}' resource request '{}' exceeds maximum node CPUs '{}' for a resource"),
  AM_RESOURCE_OVER_NODE_MEMORY(310, "AM '{}' resource request '{}' exceeds maximum node memory '{}' for a resource"),
  AM_RELEASE_ERROR(311, "AM '{}' cannot release '{}'"),
  
  RESERVATION_USER_NOT_ALLOWED_IN_QUEUE(400, "Reservation from user '{}' with requested queue '{}' denied access to assigned queue '{}'"),
  RESERVATION_USER_TO_QUEUE_MAPPING_NOT_FOUND(401, "No mapping found for reservation from user '{}' with requested queue '{}'")
  ;

  private final int code;
  private final String origTemplate;
  private final String msgTemplate;

  ErrorCode(int code, String msgTemplate) {
    this.code = code;
    origTemplate = msgTemplate;
    this.msgTemplate = this + " - " + msgTemplate;
  }

  public int getCode() {
    return code;
  }

  public String getMessage(Throwable ex, Object... args) {
    return (ex == null) ? FastFormat.format(msgTemplate, args)
      : FastFormat.format(msgTemplate, args) + " : " + ex.toString();
  }

  public static final List<String> ERROR_CODE_DESCRIPTIONS;

  static {
    List<String> list = new ArrayList<String>();
    for (ErrorCode errorCode : ErrorCode.values()) {
      if (errorCode != TEST) {
        list.add(String.format("%4d - %-40s - %s", errorCode.code, errorCode,
            errorCode.origTemplate));
      }
    }
    ERROR_CODE_DESCRIPTIONS = Collections.unmodifiableList(list);
  }

}
