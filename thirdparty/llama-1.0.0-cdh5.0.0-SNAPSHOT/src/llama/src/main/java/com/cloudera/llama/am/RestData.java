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
package com.cloudera.llama.am;

import com.cloudera.llama.am.api.LlamaAMEvent;
import com.cloudera.llama.am.api.LlamaAMListener;
import com.cloudera.llama.am.api.PlacedReservation;
import com.cloudera.llama.am.api.PlacedResource;
import com.cloudera.llama.server.ClientInfo;
import com.cloudera.llama.server.ClientNotificationService;
import com.cloudera.llama.util.UUID;
import com.cloudera.llama.util.VersionInfo;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.module.SimpleModule;
import org.codehaus.jackson.map.ser.SerializerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RestData implements LlamaAMListener,
    ClientNotificationService.Listener {

  private static Logger LOG = LoggerFactory.getLogger(RestData.class);

  private static final Map<String, String> VERSION_INFO =
      new LinkedHashMap<String, String>();
  
  static {
    VERSION_INFO.put("llamaVersion", VersionInfo.getVersion());
    VERSION_INFO.put("llamaBuiltDate", VersionInfo.getBuiltDate());
    VERSION_INFO.put("llamaBuiltBy", VersionInfo.getBuiltBy());
    VERSION_INFO.put("llamaScmUri", VersionInfo.getSCMURI());
    VERSION_INFO.put("llamaScmRevision", VersionInfo.getSCMRevision());
    VERSION_INFO.put("llamaSourceMD5", VersionInfo.getSourceMD5());
    VERSION_INFO.put("llamaHadoopVersion", VersionInfo.getHadoopVersion());
  }

  public static final String REST_VERSION_KEY = "llamaRestJsonVersion";
  public static final String REST_VERSION_VALUE = "1.0.0";

  static final String VERSION_INFO_KEY = "llamaVersionInfo";
  static final String RESERVATIONS_COUNT_KEY = "reservationsCount";
  static final String QUEUES_SUMMARY_KEY = "queuesSummary";
  static final String CLIENTS_SUMMARY_KEY = "clientsSummary";
  static final String NODES_SUMMARY_KEY = "nodesSummary";

  static final String SUMMARY_DATA = "summaryData";
  static final String ALL_DATA = "allData";
  static final String RESERVATION_DATA = "reservationData";
  static final String QUEUE_DATA = "queueData";
  static final String HANDLE_DATA = "handleData";
  static final String NODE_DATA = "nodeData";

  static final String COUNT = "count";
  static final String RESERVATIONS = "reservations";
  static final String CLIENT_INFOS = "clientInfos";
  static final String NODES_CROSSREF = "nodesCrossref";
  static final String HANDLES_CROSSREF = "handlesCrossref";
  static final String QUEUES_CROSSREF = "queuesCrossref";

  static final String CLIENT_INFO = "clientInfo";
  static final String QUEUE = "queue";
  static final String NODE = "node";

  private final ObjectMapper jsonMapper;
  private final ReadWriteLock lock;
  private final Map<UUID, PlacedReservation> reservationsMap;
  private final Map<UUID, List<PlacedReservation>> handleReservationsMap;
  private final Map<String, List<PlacedReservation>> queueReservationsMap;
  private final Map<String, List<PlacedReservation>> nodeReservationsMap;
  private final Map<UUID, ClientInfo> clientInfoMap;
  private final Set<UUID> hasBeenBackedOff;


  private ObjectMapper createJsonMapper() {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule("LlamaModule",
        new Version(1, 0, 0, null));
    module.addSerializer(new UUIDSerializer());
    module.addSerializer(new ClientInfoImplSerializer());
    module.addSerializer(new PlacedReservationSerializer());
    module.addSerializer(new PlacedResourceSerializer());
    mapper.registerModule(module);
    return mapper;
  }

  public RestData() {
    jsonMapper = createJsonMapper();
    lock = new ReentrantReadWriteLock(true);
    reservationsMap = new LinkedHashMap<UUID, PlacedReservation>();
    handleReservationsMap = new LinkedHashMap<UUID, List<PlacedReservation>>();
    queueReservationsMap = new TreeMap<String, List<PlacedReservation>>();
    nodeReservationsMap = new TreeMap<String, List<PlacedReservation>>();
    clientInfoMap = new LinkedHashMap<UUID, ClientInfo>();
    hasBeenBackedOff = new HashSet<UUID>();
  }

  public void onRegister(ClientInfo clientInfo) {
    lock.writeLock().lock();
    try {
      clientInfoMap.put(clientInfo.getHandle(), clientInfo);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void onUnregister(ClientInfo clientInfo) {
    lock.writeLock().lock();
    try {
      if (clientInfoMap.remove(clientInfo.getHandle()) != null) {
        List<PlacedReservation> list =
            new ArrayList<PlacedReservation>(reservationsMap.values());
        int count = 0;
        for (PlacedReservation reservation : list) {
          if (reservation.getHandle().equals(clientInfo.getHandle())) {
            delete(reservation, true);
            count++;
          }
        }
        LOG.debug("onUnregister({}), dropped '{}' reservations",
            clientInfo.getHandle(), count);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void onEvent(LlamaAMEvent event) {
    lock.writeLock().lock();
    try {
      for (PlacedReservation reservation : event.getReservationChanges()) {
        LOG.debug("onEvent({})", reservation);
        if (verifyHandle(reservation)) {
          if (!reservation.getStatus().isFinal()) {
            if (!reservationsMap.containsKey(reservation.getReservationId())) {
              add(reservation);
            } else {
              update(reservation);
            }
            if (reservation.getStatus() == PlacedReservation.Status.BACKED_OFF) {
              hasBeenBackedOff.add(reservation.getReservationId());
            }
          } else {
            delete(reservation, true);
            hasBeenBackedOff.remove(reservation.getReservationId());
          }
        } else {
          delete(reservation, false);
          LOG.debug("Handle not known anymore for reservation '{}'",
              reservation);
        }
      }
      for (PlacedResource resource : event.getResourceChanges()) {
        LOG.debug("onEvent({})", resource);
        update(resource);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  private boolean verifyHandle(PlacedReservation reservation) {
    return clientInfoMap.containsKey(reservation.getHandle());
  }

  private <K,V> void addToMapList(Map<K, List<V>> map, K key, V value) {
    List<V> list = map.get(key);
    if (list == null) {
      list = new ArrayList<V>();
      map.put(key, list);
    }
    list.add(value);
  }

  private void add(PlacedReservation reservation) {
    reservationsMap.put(reservation.getReservationId(), reservation);
    addToMapList(handleReservationsMap, reservation.getHandle(), reservation);
    addToMapList(queueReservationsMap, reservation.getQueue(), reservation);
    for (PlacedResource resource : reservation.getPlacedResources()) {
      addToMapList(nodeReservationsMap, resource.getLocationAsk(), reservation);
    }
  }

  private <K, V> void updateToMapList(Map<K, List<V>> map, K key, V value,
      String msg) {
    List<V> list = map.get(key);
    if (list != null) {
      int index = list.indexOf(value);
      if (index >= 0) {
        list.set(index, value);
      } else {
        LOG.error("RestData update inconsistency, key '{}' not found in {}",
            key, msg);
      }
    } else {
      LOG.error("RestData update inconsistency, value '{}' not found in {}",
          value, msg);
    }
  }

  private void updateResource( PlacedResource resource, 
      PlacedReservation reservation) {
    boolean actualLocation = false;
    List<PlacedReservation> list = 
        nodeReservationsMap.get(resource.getLocationAsk());
    if (list == null) {
      list = nodeReservationsMap.get(resource.getLocation());
      actualLocation = true;
    }
    if (list != null) {
      int index = list.indexOf(reservation);
      if (index >= 0) {
        PlacedReservation oldReservation = list.get(index);
        PlacedResource oldResource = null;
        List<PlacedResource> oldResources = oldReservation.getPlacedResources();
        for (int i = 0; oldResource == null && i < oldResources.size(); i++) {
          if (oldResources.get(i).getResourceId().
              equals(resource.getResourceId())) {
            oldResource = oldResources.get(i);
          }
        }
        if (oldResource == null) {
          LOG.error("RestData update inconsistency resource '{}' not found " +
              "in nodeReservations", resource);
        } else {
          if (actualLocation || 
              resource.getLocation() == null ||
              oldResource.getLocationAsk().equals(resource.getLocation())) {
            list.set(index, reservation);
          } else {
            list.remove(index);
            if (list.isEmpty()) {
              nodeReservationsMap.remove(resource.getLocationAsk());
            }
            list = nodeReservationsMap.get(resource.getLocation());
            if (list == null) {
              list = new ArrayList<PlacedReservation>();
              nodeReservationsMap.put(resource.getLocation(), list);
            }
            list.add(reservation);
          }
        }
      } else {
        LOG.error("RestData update inconsistency, key '{}' not found " +
            "in nodeReservations", reservation.getReservationId());
      }
    } else {
      LOG.error("RestData update inconsistency, value '{}' not found " +
            "in nodeReservations", reservation);
    }
  }

  private void update(PlacedReservation reservation) {
    reservationsMap.put(reservation.getReservationId(), reservation);
    updateToMapList(handleReservationsMap, reservation.getHandle(), reservation,
        "handleReservationsMap");
    updateToMapList(queueReservationsMap, reservation.getQueue(), reservation,
        "queueReservationsMap");
    for (PlacedResource resource : reservation.getPlacedResources()) {
      updateResource(resource, reservation);
    }
  }

  private void update(PlacedResource resource) {
    PlacedReservation reservation = reservationsMap.get(resource.getReservationId());
    if (reservation != null) {
      int idx = reservation.getPlacedResources().indexOf(resource);
      if (idx > -1) {
        reservation.getPlacedResources().set(idx, resource);
        updateResource(resource, reservation);
      } else{
        LOG.error("RestData update inconsistency, resource '{}' not found " +
            "in reservation '{}'", resource, reservation);
      }
    }
  }

  private <K, V> boolean deleteFromMapList(Map<K, List<V>> map, K key, V value) {
    boolean deleted = true;
    List<V> list = map.get(key);
    if (list != null) {
      int index = list.indexOf(value);
      if (index >= 0) {
        list.remove(index);
      } else {
        deleted = false;
      }
      if (list.isEmpty()) {
        map.remove(key);
      }
    } else {
      deleted = false;
    }
    return deleted;
  }

  private void delete(PlacedReservation reservation, boolean log) {
    reservationsMap.remove(reservation.getReservationId());
    if (!deleteFromMapList(handleReservationsMap, reservation.getHandle(),
        reservation)) {
      if (log) {
        LOG.warn(
            "RestData delete inconsistency, reservation '{}' not found in handle",
            reservation);
      }
    }
    if (!deleteFromMapList(queueReservationsMap, reservation.getQueue(),
        reservation)) {
      if (log) {
        LOG.warn(
            "RestData delete inconsistency, reservation '{}' not found in queue",
            reservation);
      }
    }
    for (PlacedResource resource : reservation.getPlacedResources()) {
      boolean deleted = deleteFromMapList(nodeReservationsMap,
          resource.getLocationAsk(), reservation);
      if (resource.getLocation() != null) {
        deleted = deleteFromMapList(nodeReservationsMap,
            resource.getLocation(), reservation) || deleted;
      }
      if (!deleted) {
        if (log) {
          LOG.warn(
              "RestData delete inconsistency, reservation '{}' not found in " +
                  "location nor actualLocation",
              reservation);
        }
      }
    }
  }

  public static class UUIDSerializer extends
      SerializerBase<UUID> {

    protected UUIDSerializer() {
      super(UUID.class);
    }

    @Override
    public void serialize(UUID value, JsonGenerator jgen,
        SerializerProvider provider)
        throws IOException {
      jgen.writeString(value.toString());
    }

    @Override
    public JsonNode getSchema(SerializerProvider provider, Type typeHint)
        throws JsonMappingException {
      return createSchemaNode("string");
    }
  }

  public static class ClientInfoImplSerializer extends
      SerializerBase<ClientInfoImpl> {

    protected ClientInfoImplSerializer() {
      super(ClientInfoImpl.class);
    }

    @Override
    public void serialize(ClientInfoImpl value, JsonGenerator jgen,
        SerializerProvider provider)
        throws IOException {
      jgen.writeStartObject();
      jgen.writeObjectField("clientId", value.getClientId());
      jgen.writeObjectField("handle", value.getHandle());
      jgen.writeStringField("callbackAddress", value.getCallbackAddress());
      jgen.writeNumberField("reservations", value.getReservations());
      jgen.writeEndObject();
    }

    @Override
    public JsonNode getSchema(SerializerProvider provider, Type typeHint)
        throws JsonMappingException {
      return createSchemaNode("object");
    }
  }
    public class PlacedReservationSerializer extends
      SerializerBase<PlacedReservation> {

    protected PlacedReservationSerializer() {
      super(PlacedReservation.class);
    }

    @Override
    public void serialize(PlacedReservation value, JsonGenerator jgen,
        SerializerProvider provider)
        throws IOException {
      jgen.writeStartObject();
      jgen.writeObjectField("reservationId", value.getReservationId());
      jgen.writeStringField("placedOn", formatDateTime(value.getPlacedOn()));
      jgen.writeObjectField("handle", value.getHandle());
      jgen.writeStringField("queue", value.getQueue());
      jgen.writeBooleanField("gang", value.isGang());
      jgen.writeBooleanField("queued", value.isQueued());
      jgen.writeStringField("status", value.getStatus().toString());
      jgen.writeBooleanField("hasBeenBackedOff",
          hasBeenBackedOff.contains(value.getReservationId()));
      jgen.writeObjectField("resources", value.getResources());
      jgen.writeEndObject();
    }

    @Override
    public JsonNode getSchema(SerializerProvider provider, Type typeHint)
        throws JsonMappingException {
      return createSchemaNode("object");
    }
  }

  private static String flattenToString(List<Object> list) {
    StringBuilder sb = new StringBuilder();
    String separator = "";
    for (Object o : list) {
      sb.append(separator).append(o);
      separator = ", ";
    }
    return sb.toString();
  }

  public static class PlacedResourceSerializer extends
      SerializerBase<PlacedResource> {

    public PlacedResourceSerializer() {
      super(PlacedResource.class);
    }

    @Override
    public void serialize(PlacedResource value, JsonGenerator jgen,
        SerializerProvider provider)
        throws IOException {
      jgen.writeStartObject();
      jgen.writeObjectField("resourceId", value.getResourceId());
      jgen.writeStringField("locationAsk", value.getLocationAsk());
      jgen.writeStringField("locality", value.getLocalityAsk().toString());
      jgen.writeNumberField("cpuVCoresAsk", value.getCpuVCoresAsk());
      jgen.writeNumberField("memoryMbsAsk", value.getMemoryMbsAsk());
      jgen.writeObjectField("handle", value.getHandle());
      jgen.writeStringField("queue", value.getQueue());
      jgen.writeObjectField("reservationId", value.getReservationId());
      jgen.writeObjectField("rmResourceId", value.getRmResourceId());
      jgen.writeStringField("actualLocation", value.getLocation());
      jgen.writeNumberField("actualCpuVCores", value.getCpuVCores());
      jgen.writeNumberField("actualMemoryMb", value.getMemoryMbs());
      jgen.writeStringField("status", value.getStatus().toString());
      jgen.writeEndObject();
    }

    @Override
    public JsonNode getSchema(SerializerProvider provider, Type typeHint)
        throws JsonMappingException {
      return createSchemaNode("object");
    }
  }

  public static class NotFoundException extends Exception {
    public NotFoundException() {
    }
  }

  @SuppressWarnings("unchecked")
  private void writeAsJson(String payloadType, Object obj, Writer out)
      throws IOException, NotFoundException {
    if (obj != null) {
      Map map = new LinkedHashMap();
      map.put(REST_VERSION_KEY, REST_VERSION_VALUE);
      map.put(payloadType, obj);
      jsonMapper.defaultPrettyPrintingWriter().writeValue(out, map);
    } else {
      throw new NotFoundException();
    }
  }

  @SuppressWarnings("unchecked")
  private <K, V> List<Map> createMapSummaryList(String itemName, Map<K,
      List<V>> map) {
    List<Map> summary = new ArrayList<Map>();
    for (Map.Entry<K, List<V>> entry : map.entrySet()) {
      Map item = new LinkedHashMap();
      item.put(itemName, entry.getKey());
      item.put(COUNT, entry.getValue().size());
      summary.add(item);
    }
    return summary;
  }

  public static class ClientInfoImpl implements ClientInfo {
    private ClientInfo clientInfo;
    private int reservations;

    public ClientInfoImpl(ClientInfo clientInfo, int reservations) {
      this.clientInfo = clientInfo;
      this.reservations = reservations;
    }
    @Override
    public UUID getClientId() {
      return clientInfo.getClientId();
    }

    @Override
    public UUID getHandle() {
      return clientInfo.getHandle();
    }

    @Override
    public String getCallbackAddress() {
      return clientInfo.getCallbackAddress();
    }

    public int getReservations() {
      return reservations;
    }
  }

  private List<ClientInfoImpl> createClientInfoSummary() {
    Map<UUID, Integer> summary = new LinkedHashMap<UUID, Integer>();
    for (Map.Entry<UUID, List<PlacedReservation>> entry :
        handleReservationsMap.entrySet()) {
      summary.put(entry.getKey(), entry.getValue().size());
    }
    List<ClientInfoImpl> list = new ArrayList<ClientInfoImpl>(
        clientInfoMap.size());
    for (Map.Entry<UUID, ClientInfo> entry : clientInfoMap.entrySet()) {
      Integer count = summary.get(entry.getKey());
      count = (count != null) ? count : 0;
      list.add(new ClientInfoImpl(entry.getValue(), count));
    }
    return list;
  }

  @SuppressWarnings("unchecked")
  public void writeSummaryAsJson(Writer out)
      throws IOException, NotFoundException {
    lock.readLock().lock();
    try {
      Map summary = new LinkedHashMap();
      summary.put(VERSION_INFO_KEY, VERSION_INFO);
      summary.put(RESERVATIONS_COUNT_KEY, reservationsMap.size());
      summary.put(QUEUES_SUMMARY_KEY, createMapSummaryList(QUEUE,
          queueReservationsMap));
      summary.put(CLIENTS_SUMMARY_KEY, createClientInfoSummary());
      summary.put(NODES_SUMMARY_KEY, createMapSummaryList(NODE,
          nodeReservationsMap));
      writeAsJson(SUMMARY_DATA, summary, out);
    } finally {
      lock.readLock().unlock();
    }
  }

  @SuppressWarnings("unchecked")
  private <K> Map<K, List<UUID>> createCrossRef(Map<K, List<PlacedReservation>>
      map) {
    Map<K, List<UUID>> crossRef = new LinkedHashMap<K, List<UUID>>();
    for (Map.Entry<K, List<PlacedReservation>> entry : map.entrySet()) {
      K key = entry.getKey();
      List<UUID> list = crossRef.get(key);
      if (list == null) {
        list = new ArrayList<UUID>();
        crossRef.put(key, list);
      }
      for (PlacedReservation value : entry.getValue()) {
        list.add(value.getReservationId());
      }
    }
    return crossRef;
  }

  @SuppressWarnings("unchecked")
  public void writeAllAsJson(Writer out)
      throws IOException, NotFoundException {
    lock.readLock().lock();
    try {
      Map all = new LinkedHashMap();
      all.put(VERSION_INFO_KEY, VERSION_INFO);
      all.put(RESERVATIONS, reservationsMap);
      all.put(CLIENT_INFOS, createClientInfoSummary());
      all.put(QUEUES_CROSSREF, createCrossRef(queueReservationsMap));
      all.put(HANDLES_CROSSREF, createCrossRef(handleReservationsMap));
      all.put(NODES_CROSSREF, createCrossRef(nodeReservationsMap));
      writeAsJson(ALL_DATA, all, out);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void writeReservationAsJson(UUID reservationId, Writer out)
      throws IOException, NotFoundException {
    lock.readLock().lock();
    try {
      PlacedReservation r = reservationsMap.get(reservationId);
      if (r == null) {
        throw new NotFoundException();
      }
      writeAsJson(RESERVATION_DATA, r, out);
    } finally {
      lock.readLock().unlock();
    }
  }

  @SuppressWarnings("unchecked")
  public void writeHandleReservationsAsJson(UUID handle, Writer out)
      throws IOException, NotFoundException {
    lock.readLock().lock();
    try {
      ClientInfo ci = clientInfoMap.get(handle);
      if (ci == null) {
        throw new NotFoundException();
      }
      List<PlacedReservation> prs = handleReservationsMap.get(handle);
      prs = (prs != null) ? prs : Collections.EMPTY_LIST;
      Map map = new LinkedHashMap();
      map.put(CLIENT_INFO, ci);
      map.put(RESERVATIONS, prs);
      writeAsJson(HANDLE_DATA, map, out);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void writeQueueReservationsAsJson(String queue, Writer out)
      throws IOException, NotFoundException {
    lock.readLock().lock();
    try {
      List<PlacedReservation> l = queueReservationsMap.get(queue);
      if (l == null) {
        throw new NotFoundException();
      }
      writeAsJson(QUEUE_DATA, l, out);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void writeNodeResourcesAsJson(String node, Writer out)
      throws IOException, NotFoundException {
    lock.readLock().lock();
    try {
      List<PlacedReservation> l = nodeReservationsMap.get(node);
      if (l == null) {
        throw new NotFoundException();
      }
      writeAsJson(NODE_DATA, l, out);
    } finally {
      lock.readLock().unlock();
    }
  }

  private static final String ISO8601_UTC_MASK = "yyyy-MM-dd'T'HH:mm'Z'";
  private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

  private static String formatDateTime(long epoc) {
    DateFormat dateFormat = new SimpleDateFormat(ISO8601_UTC_MASK);
    dateFormat.setTimeZone(UTC);
    Date date = new Date(epoc);
    return dateFormat.format(date);
  }

}
