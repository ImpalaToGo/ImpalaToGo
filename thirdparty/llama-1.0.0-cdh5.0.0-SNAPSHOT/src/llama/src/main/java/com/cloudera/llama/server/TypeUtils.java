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
package com.cloudera.llama.server;

import com.cloudera.llama.am.api.Builders;
import com.cloudera.llama.am.api.Expansion;
import com.cloudera.llama.thrift.TLlamaAMReservationExpansionRequest;
import com.cloudera.llama.util.ErrorCode;
import com.cloudera.llama.util.LlamaException;
import com.cloudera.llama.am.api.PlacedReservation;
import com.cloudera.llama.am.api.PlacedResource;
import com.cloudera.llama.am.api.Reservation;
import com.cloudera.llama.am.api.Resource;
import com.cloudera.llama.thrift.TAllocatedResource;
import com.cloudera.llama.thrift.TLlamaAMNotificationRequest;
import com.cloudera.llama.thrift.TLlamaAMReservationRequest;
import com.cloudera.llama.thrift.TLlamaServiceVersion;
import com.cloudera.llama.thrift.TResource;
import com.cloudera.llama.thrift.TStatus;
import com.cloudera.llama.thrift.TStatusCode;
import com.cloudera.llama.thrift.TUniqueId;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.cloudera.llama.util.ExceptionUtils;
import com.cloudera.llama.util.UUID;

public class TypeUtils {
  public static final TStatus OK = new TStatus().setStatus_code(TStatusCode.OK);

  public static TStatus okWithMsgs(List<String> msgs) {
    TStatus ok = new TStatus().setStatus_code(TStatusCode.OK);
    ok.setError_msgs(msgs);
    return ok;
  }

  public static TStatus createError(Throwable ex) {
    ex = ExceptionUtils.getRootCause(ex, LlamaException.class);
    LlamaException llamaEx;
    boolean internalError = false;
    if (ex instanceof LlamaException) {
      llamaEx = (LlamaException) ex;
    } else if (ex instanceof IllegalArgumentException) {
      llamaEx =new LlamaException(ex, ErrorCode.ILLEGAL_ARGUMENT);
    } else {
      llamaEx = new LlamaException(ex, ErrorCode.INTERNAL_ERROR);
      internalError = true;
    }
    TStatus error = new TStatus().setStatus_code((internalError)
                                                 ? TStatusCode.INTERNAL_ERROR
                                                 : TStatusCode.REQUEST_ERROR);
    error.setError_code((short)llamaEx.getErrorCode());
    List<String> msgs = new ArrayList<String>();
    msgs.add(llamaEx.toString());

    try {
      StringWriter writer = new StringWriter();
      PrintWriter pWriter = new PrintWriter(writer);
      llamaEx.printStackTrace(pWriter);
      pWriter.close();
      BufferedReader br= new BufferedReader(new StringReader(writer.toString()));
      String line = br.readLine();
      while (line != null) {
        msgs.add(line);
        line = br.readLine();
      }
      br.close();
    } catch (IOException ioEx) {
      //Cannot happen
    }
    error.setError_msgs(msgs);
    return error;
  }

  public static UUID toUUID(TUniqueId id) {
    return new UUID(id.getHi(), id.getLo());
  }

  public static List<UUID> toUUIDs(List<TUniqueId> ids) {
    List<UUID> uuids = new ArrayList<UUID>(ids.size());
    for (TUniqueId id : ids) {
      uuids.add(toUUID(id));
    }
    return uuids;
  }

  public static TUniqueId toTUniqueId(UUID uuid) {
    return new TUniqueId().setHi(uuid.getMostSignificantBits()).
        setLo(uuid.getLeastSignificantBits());
  }

  public static List<TUniqueId> toTUniqueIds(List<UUID> uuids) {
    List<TUniqueId> ids = new ArrayList<TUniqueId>(uuids.size());
    for (UUID uuid : uuids) {
      ids.add(toTUniqueId(uuid));
    }
    return ids;
  }

  public static Resource toResource(TResource resource, NodeMapper nodeMapper) {
    UUID resourceId = toUUID(resource.getClient_resource_id());
    int vCpuCores = resource.getV_cpu_cores();
    int memoryMb = resource.getMemory_mb();
    String location = nodeMapper.getNodeManager(resource.getAskedLocation());
    Resource.Locality locality = Resource.Locality.valueOf(
        resource.getEnforcement().toString());
    Resource.Builder builder = Builders.createResourceBuilder();
    return builder.setResourceId(resourceId).setLocationAsk(location).
        setLocalityAsk(locality).setCpuVCoresAsk(vCpuCores).
        setMemoryMbsAsk(memoryMb).build();
  }

  public static List<Resource> toResourceList(List<TResource> tResources,
      NodeMapper nodeMapper) {
    List<Resource> resources = new ArrayList<Resource>(tResources.size());
    for (TResource tResource : tResources) {
      resources.add(toResource(tResource, nodeMapper));
    }
    return resources;
  }

  public static Reservation toReservation(TLlamaAMReservationRequest request,
      NodeMapper nodeMapper, String queue) {
    UUID handle = toUUID(request.getAm_handle());
    boolean isGang = request.isGang();
    List<Resource> resources = toResourceList(request.getResources(),
        nodeMapper);
    Reservation.Builder builder = Builders.createReservationBuilder();
    return builder.setHandle(handle).setUser(request.getUser()).setQueue(queue).
        setResources(resources).setGang(isGang).build();
  }

  public static Expansion toExpansion(
      TLlamaAMReservationExpansionRequest request, NodeMapper nodeMapper) {
    UUID handle = toUUID(request.getAm_handle());
    Resource resource = toResource(request.getResource(), nodeMapper);
    Expansion.Builder builder = Builders.createExpansionBuilder();
    return builder.setHandle(handle).
        setExpansionOf(toUUID(request.getExpansion_of())).
        setResource(resource).build();
  }

  public static TAllocatedResource toTAllocatedResource(PlacedResource
      resource, NodeMapper nodeMapper) {
    TAllocatedResource tResource = new TAllocatedResource();
    tResource.setReservation_id(toTUniqueId(resource.getReservationId()));
    tResource.setClient_resource_id(toTUniqueId(resource.getResourceId()));
    tResource.setRm_resource_id("TODO"); //TODO flatten container ids resource.getRmResourceIds());
    tResource.setV_cpu_cores((short) resource.getCpuVCores());
    tResource.setMemory_mb(resource.getMemoryMbs());
    tResource.setLocation(nodeMapper.getDataNode(resource.getLocation()));
    return tResource;
  }

  @SuppressWarnings("unchecked")
  public static TLlamaAMNotificationRequest createHearbeat(UUID clientId) {
    TLlamaAMNotificationRequest request = new TLlamaAMNotificationRequest();
    request.setVersion(TLlamaServiceVersion.V1);
    request.setAm_handle(toTUniqueId(clientId));
    request.setHeartbeat(true);

    request.setAllocated_reservation_ids(Collections.EMPTY_LIST);
    request.setAllocated_resources(Collections.EMPTY_LIST);
    request.setRejected_reservation_ids(Collections.EMPTY_LIST);
    request.setRejected_client_resource_ids(Collections.EMPTY_LIST);
    request.setLost_client_resource_ids(Collections.EMPTY_LIST);
    request.setPreempted_reservation_ids(Collections.EMPTY_LIST);
    request.setPreempted_client_resource_ids(Collections.EMPTY_LIST);
    return request;
  }

  public static TLlamaAMNotificationRequest toAMNotification(UUID handle,
      List<Object> rrList, NodeMapper nodeMapper) {
    TLlamaAMNotificationRequest request = new TLlamaAMNotificationRequest();
    request.setVersion(TLlamaServiceVersion.V1);
    request.setAm_handle(toTUniqueId(handle));
    request.setHeartbeat(false);

    for (Object rr : rrList) {
      if (rr instanceof PlacedReservation) {
        PlacedReservation reservation = (PlacedReservation) rr;
        switch (reservation.getStatus()) {
          case ALLOCATED:
            request.addToAllocated_reservation_ids(toTUniqueId(
                reservation.getReservationId()));
            break;
          case REJECTED:
            request.addToRejected_reservation_ids(toTUniqueId(
                reservation.getReservationId()));
            break;
          case LOST:
            request.addToLost_reservation_ids(toTUniqueId(
                reservation.getReservationId()));
            break;
          case PREEMPTED:
            request.addToPreempted_reservation_ids(toTUniqueId(
                reservation.getReservationId()));
            break;
          case RELEASED:
            request.addToAdmin_released_reservation_ids(toTUniqueId(
                reservation.getReservationId()));
            break;
        }
      } else if (rr instanceof PlacedResource) {
        PlacedResource resource = (PlacedResource) rr;
        switch (resource.getStatus()) {
          case ALLOCATED:
            request.addToAllocated_resources(toTAllocatedResource(
                resource, nodeMapper));
            break;
          case REJECTED:
            request.addToRejected_client_resource_ids(toTUniqueId(
                resource.getResourceId()));
            break;
          case LOST:
            request.addToLost_client_resource_ids(toTUniqueId(
                resource.getResourceId()));
            break;
          case PREEMPTED:
            request.addToPreempted_client_resource_ids(toTUniqueId(
                resource.getResourceId()));
            break;
        }
      } else {
        throw new IllegalArgumentException("List should contain " +
            "PlacedReservation or PlaceResource objects only, it has a " +
            rr.getClass());
      }
    }
    return request;
  }

  public static boolean isOK(TStatus status) {
    return status.getStatus_code() == TStatusCode.OK;
  }
}
