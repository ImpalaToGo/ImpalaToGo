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
package com.cloudera.llama.am.yarn;

import com.cloudera.llama.am.api.LlamaAM;
import com.cloudera.llama.util.ErrorCode;
import com.cloudera.llama.util.LlamaException;
import com.cloudera.llama.am.api.PlacedResource;
import com.cloudera.llama.am.api.RMResource;
import com.cloudera.llama.am.spi.RMConnector;
import com.cloudera.llama.am.spi.RMEvent;
import com.cloudera.llama.am.spi.RMListener;
import com.cloudera.llama.util.NamedThreadFactory;
import com.cloudera.llama.util.UUID;
import com.codahale.metrics.MetricRegistry;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class YarnRMConnector implements RMConnector, Configurable,
    AMRMClientAsync.CallbackHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(YarnRMConnector.class);

  public static final String PREFIX_KEY = LlamaAM.PREFIX_KEY + "yarn.";

  public static final String AM_PRIORITY_KEY = PREFIX_KEY + "priority";
  public static final int AM_PRIORITY_DEFAULT = 0;

  public static final String APP_MONITOR_TIMEOUT_KEY = PREFIX_KEY +
      "app.monitor.timeout.ms";
  public static final long APP_MONITOR_TIMEOUT_DEFAULT = 30000;

  public static final String APP_MONITOR_POLLING_KEY = PREFIX_KEY +
      "app.monitor.polling.ms";
  public static final long APP_MONITOR_POLLING_DEFAULT = 200;

  public static final String HEARTBEAT_INTERVAL_KEY = PREFIX_KEY +
      "app.heartbeat.interval.ms";
  public static final int HEARTBEAT_INTERNAL_DEFAULT = 200;

  public static final String CONTAINER_HANDLER_QUEUE_THRESHOLD_KEY = PREFIX_KEY
      + "container.handler.queue.threshold";
  public static final int CONTAINER_HANDLER_QUEUE_THRESHOLD_DEFAULT = 10000;

  public static final String CONTAINER_HANDLER_THREADS_KEY = PREFIX_KEY +
      "container.handler.threads";
  public static final int CONTAINER_HANDLER_THREADS_DEFAULT = 10;

  public static final String HADOOP_USER_NAME_KEY = PREFIX_KEY +
      "hadoop.user.name";
  public static final String HADOOP_USER_NAME_DEFAULT = "llama";

  public static final String ADVERTISED_HOSTNAME_KEY = PREFIX_KEY +
      "advertised.hostname";
  public static final String ADVERTISED_PORT_KEY = PREFIX_KEY +
      "advertised.port";
  public static final String ADVERTISED_TRACKING_URL_KEY = PREFIX_KEY +
      "advertised.tracking.url";


  private static final int SLEEP_TIME_SEC = 315360000; //10 years

  private Configuration conf;
  private Configuration yarnConf;
  private boolean includePortInNodeName;
  private RMListener llamaCallback;
  private UserGroupInformation ugi;
  private YarnClient yarnClient;
  private AMRMClientAsync<LlamaContainerRequest> amRmClientAsync;
  private NMClient nmClient;
  private ApplicationId appId;
  private final Map<String, Resource> nodes;
  private Resource maxResource;
  private int containerHandlerQueueThreshold;
  private BlockingQueue<ContainerHandler> containerHandlerQueue;
  private ThreadPoolExecutor containerHandlerExecutor;

  public YarnRMConnector() {
    nodes = Collections.synchronizedMap(new HashMap<String, Resource>());
  }

  @Override
  public void setMetricRegistry(MetricRegistry metricRegistry) {
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    includePortInNodeName = getConf().getBoolean
        (YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_USE_PORT_FOR_NODE_NAME);
    yarnConf = new YarnConfiguration();
    for (Map.Entry entry : getConf()) {
      yarnConf.set((String) entry.getKey(), (String) entry.getValue());
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setLlamaAMCallback(RMListener callback) {
    llamaCallback = callback;
  }

  private UserGroupInformation createUGIForApp() throws Exception {
    String userName = getConf().get(HADOOP_USER_NAME_KEY,
        HADOOP_USER_NAME_DEFAULT);
    UserGroupInformation llamaUGI = UserGroupInformation.getLoginUser();
    return UserGroupInformation.createProxyUser(userName, llamaUGI);
  }

  @Override
  public void start() throws LlamaException {
    try {
      ugi = createUGIForApp();
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          _start();
          return null;
        }
      });
    } catch (Throwable ex) {
      throw new LlamaException(ex, ErrorCode.AM_CANNOT_START);
    }
  }

  @Override
  public void stop() {
    if (ugi != null) {
      ugi.doAs(new PrivilegedAction<Void>() {
        @Override
        public Void run() {
          _stop();
          return null;
        }
      });
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void register(final String queue) throws LlamaException {
    try {
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          _initYarnApp(queue);
          return null;
        }
      });
      containerHandlerQueueThreshold = getConf().getInt(
          CONTAINER_HANDLER_QUEUE_THRESHOLD_KEY,
          CONTAINER_HANDLER_QUEUE_THRESHOLD_DEFAULT);
      containerHandlerQueue = new LinkedBlockingQueue<ContainerHandler>();
      int threads = getConf().getInt(CONTAINER_HANDLER_THREADS_KEY,
          CONTAINER_HANDLER_THREADS_DEFAULT);
      // funny down-casting and up-casting because javac gets goofy here
      containerHandlerExecutor = new ThreadPoolExecutor(threads, threads, 0,
          TimeUnit.SECONDS, (BlockingQueue<Runnable>) (BlockingQueue)
          containerHandlerQueue,
          new NamedThreadFactory("llama-container-handler"));
      containerHandlerExecutor.prestartAllCoreThreads();
    } catch (Exception ex) {
      throw new LlamaException(ex, ErrorCode.AM_CANNOT_REGISTER, appId, queue);
    }
  }

  public String getNodeName(NodeId nodeId) {
    return (includePortInNodeName) ? nodeId.getHost() + ":" + nodeId.getPort()
                                   : nodeId.getHost();
  }

  private void _start() throws Exception {
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(yarnConf);
    yarnClient.start();
  }

  private void _stop() {
    if (yarnClient != null) {
      yarnClient.stop();
      yarnClient = null;
    }
  }
  
  private void _initYarnApp(String queue) throws Exception {
    NMTokenCache nmTokenCache = new NMTokenCache();
    nmClient = NMClient.createNMClient();
    nmClient.setNMTokenCache(nmTokenCache);
    nmClient.init(yarnConf);
    nmClient.start();
    appId = _createApp(yarnClient, queue);
    _monitorAppState(yarnClient, appId, ACCEPTED, false);
    ugi.addToken(yarnClient.getAMRMToken(appId));
    int heartbeatInterval = getConf().getInt(HEARTBEAT_INTERVAL_KEY,
        HEARTBEAT_INTERNAL_DEFAULT);
    AMRMClient<LlamaContainerRequest> amRmClient = AMRMClient.createAMRMClient();
    amRmClient.setNMTokenCache(nmTokenCache);
    amRmClientAsync = AMRMClientAsync.createAMRMClientAsync(amRmClient, 
        heartbeatInterval, YarnRMConnector.this);
    amRmClientAsync.init(yarnConf);
    amRmClientAsync.start();
    String urlWithoutScheme = getConf().get(ADVERTISED_TRACKING_URL_KEY,
        "http://").substring("http://".length());
    RegisterApplicationMasterResponse response = amRmClientAsync
        .registerApplicationMaster(
            getConf().get(ADVERTISED_HOSTNAME_KEY, ""),
            getConf().getInt(ADVERTISED_PORT_KEY, 0), urlWithoutScheme);
    maxResource = response.getMaximumResourceCapability();
    for (NodeReport nodeReport : yarnClient.getNodeReports()) {
      if (nodeReport.getNodeState() == NodeState.RUNNING) {
        String nodeKey = getNodeName(nodeReport.getNodeId());
        nodes.put(nodeKey, nodeReport.getCapability());
        LOG.debug("Added node '{}' with '{}' cpus and '{}' memory",
            nodeKey, nodeReport.getCapability().getVirtualCores(),
            nodeReport.getCapability().getMemory());
      }
    }
    LOG.debug("Started AM '{}' for '{}' queue", appId, queue);
  }

  private ApplicationId _createApp(YarnClient rmClient, String queue)
      throws LlamaException {
    try {
      // Create application
      YarnClientApplication newApp = rmClient.createApplication();
      ApplicationId appId = newApp.getNewApplicationResponse().
          getApplicationId();

      // Create launch context for app master
      ApplicationSubmissionContext appContext = Records.newRecord(
          ApplicationSubmissionContext.class);

      // set the application id
      appContext.setApplicationId(appId);

      // set the application name
      appContext.setApplicationName("Llama for " + queue);

      appContext.setApplicationType("LLAMA");

      // Set the priority for the application master
      Priority pri = Records.newRecord(Priority.class);
      int priority = getConf().getInt(AM_PRIORITY_KEY, AM_PRIORITY_DEFAULT);
      pri.setPriority(priority);
      appContext.setPriority(pri);

      // Set the queue to which this application is to be submitted in the RM
      appContext.setQueue(queue);

      // Set up the container launch context for the application master
      ContainerLaunchContext amContainer = Records.newRecord(
          ContainerLaunchContext.class);
      appContext.setAMContainerSpec(amContainer);

      // unmanaged AM
      appContext.setUnmanagedAM(true);

      // setting max attempts to 1 to avoid warning from Yarn RM
      // as the AM is unmanaged, it doesn't really matter.
      appContext.setMaxAppAttempts(1);

      // Submit the application to the applications manager
      return rmClient.submitApplication(appContext);
    } catch (Exception ex) {
      throw new LlamaException(ex, ErrorCode.AM_CANNOT_CREATE, queue);
    }
  }

  private static final Set<YarnApplicationState> ACCEPTED = EnumSet.of
      (YarnApplicationState.ACCEPTED);

  private static final Set<YarnApplicationState> STOPPED = EnumSet.of(
      YarnApplicationState.KILLED, YarnApplicationState.FAILED,
      YarnApplicationState.FINISHED);

  private ApplicationReport _monitorAppState(YarnClient rmClient,
      ApplicationId appId, Set<YarnApplicationState> states,
      boolean calledFromStopped)
      throws LlamaException {
    String action = calledFromStopped ? "stopping" : "starting";
    try {
      long timeout = getConf().getLong(APP_MONITOR_TIMEOUT_KEY,
          APP_MONITOR_TIMEOUT_DEFAULT);

      long polling = getConf().getLong(APP_MONITOR_POLLING_KEY,
          APP_MONITOR_POLLING_DEFAULT);

      long start = System.currentTimeMillis();
      ApplicationReport report = rmClient.getApplicationReport(appId);
      while (!states.contains(report.getYarnApplicationState())) {
        if (System.currentTimeMillis() - start > timeout) {
          throw new LlamaException(ErrorCode.AM_TIMED_OUT_STARTING_STOPPING,
              appId, timeout, report.getYarnApplicationState(), states,
              action);
        }
        Thread.sleep(polling);
        report = rmClient.getApplicationReport(appId);
      }
      return report;
    } catch (Exception ex) {
      if (!calledFromStopped) {
        _stop(FinalApplicationStatus.FAILED, "Could not start, error: " + ex, 
            true);
      }
      throw new LlamaException(ex, ErrorCode.AM_FAILED_WHILE_STARTING_STOPPING,
          appId, action);
    }
  }

  @Override
  public void unregister() {
    ugi.doAs(new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        _stop(FinalApplicationStatus.SUCCEEDED, "Stopped by AM", false);
        return null;
      }
    });
  }

  private synchronized void _stop(FinalApplicationStatus status, String msg,
      boolean stopYarnClient) {
    if (containerHandlerExecutor != null) {
      containerHandlerExecutor.shutdownNow();
      containerHandlerExecutor = null;
    }
    if (amRmClientAsync != null) {
      LOG.debug("Stopping AM '{}'", appId);
      try {
        amRmClientAsync.unregisterApplicationMaster(status, msg, "");
      } catch (Exception ex) {
        LOG.warn("Error un-registering AM client, " + ex, ex);
      }
      amRmClientAsync.stop();
      amRmClientAsync = null;
    }
    if (stopYarnClient) {
      if (yarnClient != null) {
        try {
          ApplicationReport report = _monitorAppState(yarnClient, appId, STOPPED,
              true);
          if (report.getFinalApplicationStatus()
              != FinalApplicationStatus.SUCCEEDED) {
            LOG.warn("Problem stopping application, final status '{}'",
                report.getFinalApplicationStatus());
          }
        } catch (Exception ex) {
          LOG.warn("Error stopping application, " + ex, ex);
        }
        yarnClient.stop();
        yarnClient = null;
      }
    }
    if (nmClient != null) {
      //TODO this is introducing a deadlock
      //nmClient.stop();
    }
  }

  @Override
  public List<String> getNodes() throws LlamaException {
    List<String> nodes = new ArrayList<String>();
    try {
      List<NodeReport> nodeReports = 
          yarnClient.getNodeReports(NodeState.RUNNING);
      for (NodeReport nodeReport : nodeReports) {
        nodes.add(getNodeName(nodeReport.getNodeId()));
      }
      return nodes;
    } catch (Throwable ex) {
      throw new LlamaException(ex, ErrorCode.AM_CANNOT_GET_NODES, appId);
    }
  }

  private static final String[] RACKS = new String[0];

  class LlamaContainerRequest extends AMRMClient.ContainerRequest {
    private RMResource placedResource;

    public LlamaContainerRequest(RMResource resource)
        throws LlamaException {
      super(Resource.newInstance(resource.getMemoryMbsAsk(),
                resource.getCpuVCoresAsk()),
            new String[]{ resource.getLocationAsk()},
            RACKS,
            getRequestPriority(resource.getMemoryMbsAsk(),
                resource.getCpuVCoresAsk(), resource.getLocalityAsk()),
            (resource.getLocalityAsk() !=
                com.cloudera.llama.am.api.Resource.Locality.MUST)
      );
      this.placedResource = resource;
    }

    public RMResource getResourceAsk() {
      return placedResource;
    }
  }

  private void verifyResources(Collection<RMResource> resources)
      throws LlamaException {
    for (RMResource r : resources) {
      Resource nodeCapabilites = nodes.get(r.getLocationAsk());
      if (nodeCapabilites == null) {
        throw new LlamaException(ErrorCode.AM_NODE_NOT_AVAILABLE, appId,
            r.getLocationAsk(), r);
      }
      if (r.getCpuVCoresAsk() > maxResource.getVirtualCores()) {
        throw new LlamaException(ErrorCode.AM_RESOURCE_OVER_MAX_CPUS,
            appId, r, maxResource.getVirtualCores());
      }
      if (r.getMemoryMbsAsk() > maxResource.getMemory()) {
        throw new LlamaException(ErrorCode.AM_RESOURCE_OVER_MAX_MEMORY,
            appId, r, maxResource.getMemory());
      }
      if (r.getCpuVCoresAsk() > nodeCapabilites.getVirtualCores()) {
        throw new LlamaException(ErrorCode.AM_RESOURCE_OVER_NODE_CPUS,
            appId, r, nodeCapabilites.getVirtualCores());
      }
      if (r.getMemoryMbsAsk() > nodeCapabilites.getMemory()) {
        throw new LlamaException(ErrorCode.AM_RESOURCE_OVER_NODE_MEMORY,
            appId, r, nodeCapabilites.getMemory());
      }
    }
  }

  private void _reserve(Collection<RMResource> resources)
      throws LlamaException {
    verifyResources(resources);
    for (RMResource resource : resources) {
      LOG.debug("Adding container request for '{}'", resource);
      LlamaContainerRequest request = new LlamaContainerRequest(resource);
      amRmClientAsync.addContainerRequest(request);
      resource.getRmData().put("request", request);
    }
  }

  @Override
  public void reserve(final Collection<RMResource> resources)
      throws LlamaException {
    try {
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          _reserve(resources);
          return null;
        }
      });
    } catch (Throwable ex) {
      if (ex.getCause() instanceof LlamaException) {
        throw (LlamaException) ex.getCause();
      } else {
        throw new RuntimeException(ex);
      }
    }
  }

  private void _release(Collection<RMResource> resources)
      throws LlamaException {
    for (RMResource resource : resources) {
      boolean released = false;
      LlamaContainerRequest request = (LlamaContainerRequest)
          resource.getRmData().get("request");
      if (request != null) {
        LOG.debug("Releasing container request for '{}'", resource);
        amRmClientAsync.removeContainerRequest(request);
        released = true;
      }
      Container container = (Container) resource.getRmData().get("container");
      if (container != null) {
        LOG.debug("Releasing container '{}' for '{}'", container, resource);
        containerToResourceMap.remove(container.getId());
        queue(new ContainerHandler(ugi, resource, container, Action.STOP));
        released = true;
      }
      if (!released) {
        LOG.debug("Missing RM payload, ignoring release of container " +
            "request for '{}'", resource);
      }
    }
  }

  @Override
  public void release(final Collection<RMResource> resources,
      boolean doNotCache) throws LlamaException {
    try {
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          _release(resources);
          return null;
        }
      });
    } catch (Throwable ex) {
      throw new LlamaException(ex, ErrorCode.AM_RELEASE_ERROR, resources);
    }
  }

  @Override
  public boolean reassignResource(Object rmResourceId, UUID resourceId) {
    return containerToResourceMap.replace((ContainerId)rmResourceId,
        resourceId) != null;
  }

  public void emptyCache() throws LlamaException {
  }

  ConcurrentHashMap<ContainerId, UUID> containerToResourceMap =
      new ConcurrentHashMap<ContainerId, UUID>();

  @Override
  public void onContainersCompleted(List<ContainerStatus> containerStatuses) {
    List<RMEvent> changes = new ArrayList<RMEvent>();
    for (ContainerStatus containerStatus : containerStatuses) {
      ContainerId containerId = containerStatus.getContainerId();
      UUID resourceId = containerToResourceMap.remove(containerId);
      // we have the containerId only if we did not release it.
      if (resourceId != null) {
        switch (containerStatus.getExitStatus()) {
          case ContainerExitStatus.SUCCESS:
            LOG.warn("It should never happen, container for resource '{}' " +
                "exited on its own", resourceId);
            //reporting it as LOST for the client to take corrective measures.
            changes.add(RMEvent.createStatusChangeEvent(resourceId,
                PlacedResource.Status.LOST));
            break;
          case ContainerExitStatus.PREEMPTED:
            LOG.warn("Container for resource '{}' has been preempted",
                resourceId);
            changes.add(RMEvent.createStatusChangeEvent(resourceId,
                PlacedResource.Status.PREEMPTED));
            break;
          case ContainerExitStatus.ABORTED:
          default:
            LOG.warn("Container for resource '{}' has been lost, exit status" +
                " '{}'", resourceId, containerStatus.getExitStatus());
            changes.add(RMEvent.createStatusChangeEvent(resourceId,
                PlacedResource.Status.LOST));
            break;
        }
      }
    }
    llamaCallback.onEvent(changes);
  }

  private enum Action {START, STOP}

  class ContainerHandler implements Runnable {
    final private UserGroupInformation ugi;
    final private UUID clientResourceId;
    final private Container container;
    final private Action action;

    public ContainerHandler(UserGroupInformation ugi,
        RMResource placedResource, Container container, Action action) {
      this.ugi = ugi;
      this.clientResourceId = placedResource.getResourceId();
      this.container = container;
      this.action = action;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() {
      try {
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            if (action == Action.START) {
              LOG.debug("Starting container '{}' process for resource '{}' " +
                  "at node '{}'", container.getId(), clientResourceId,
                  container.getNodeId());
              ContainerLaunchContext ctx =
                  Records.newRecord(ContainerLaunchContext.class);
              ctx.setEnvironment(Collections.EMPTY_MAP);
              ctx.setLocalResources(Collections.EMPTY_MAP);
              ctx.setCommands(Arrays.asList("sleep", Integer.toString(
                  SLEEP_TIME_SEC)));
              nmClient.startContainer(container, ctx);
            } else {
              nmClient.stopContainer(container.getId(), container.getNodeId());
            }
            return null;
          }
        });
      } catch (Exception ex) {
        LOG.warn(
            "Could not {} container '{}' for resource '{}' at node '{}': {}'",
            action, container.getId(), clientResourceId,
            getNodeName(container.getNodeId()), ex.toString(), ex);
        if (action == Action.START) {
          List<RMEvent> changes = new ArrayList<RMEvent>();
          changes.add(RMEvent.createStatusChangeEvent(clientResourceId,
              PlacedResource.Status.LOST));
          llamaCallback.onEvent(changes);
        }
      }
    }
  }

  private void queue(ContainerHandler handler) {
    containerHandlerQueue.add(handler);
    int size = containerHandlerQueue.size();
    if (size > containerHandlerQueueThreshold) {
      LOG.warn("Container handler queue over '{}' threshold at '{}'",
          containerHandlerQueueThreshold, size);
    }
  }

  private RMEvent createResourceAllocation(RMResource resources,
      Container container) {
    return RMEvent.createAllocationEvent(resources.getResourceId(),
        getNodeName(container.getNodeId()),
        container.getResource().getVirtualCores(),
        container.getResource().getMemory(), container.getId().toString(),
        resources.getRmData());
  }

  @Override
  public void onContainersAllocated(List<Container> containers) {
    List<RMEvent> changes = new ArrayList<RMEvent>();
    // no need to use a ugi.doAs() as this is called from within Yarn client
    for (Container container : containers) {
      List<? extends Collection<LlamaContainerRequest>> matchingContainerReqs =
          amRmClientAsync.getMatchingRequests(container.getPriority(),
              getNodeName(container.getNodeId()), container.getResource());

      if (!matchingContainerReqs.isEmpty()) {
        LlamaContainerRequest req = null;
        Iterator<? extends Collection<LlamaContainerRequest>> it1 =
            matchingContainerReqs.iterator();
        while (req == null && it1.hasNext()) {
          Iterator<LlamaContainerRequest> it2 = it1.next().iterator();
          while (req == null && it2.hasNext()) {
            req = it2.next();
            LOG.trace("Matching container '{}' resource '{}'", container,
                req.getResourceAsk());
          }
        }
        if (req == null) {
          LOG.error("There was a match for container '{}', " +
              "LlamaContainerRequest cannot be NULL", container);
        } else {
          RMResource resource = req.getResourceAsk();

          LOG.debug("New allocation for '{}' container '{}', node '{}'",
              resource, container.getId(), container.getNodeId());

          resource.getRmData().put("container", container);
          containerToResourceMap.put(container.getId(),
              resource.getResourceId());
          changes.add(createResourceAllocation(resource, container));
          amRmClientAsync.removeContainerRequest(req);
          LOG.trace("Reservation resource '{}' removed from YARN", resource);

          queue(new ContainerHandler(ugi, resource, container, Action.START));
        }
      }
    }
    llamaCallback.onEvent(changes);
  }

  @Override
  public void onShutdownRequest() {
    llamaCallback.stoppedByRM();

    LOG.warn("Yarn requested AM to shutdown");

    // no need to use a ugi.doAs() as this is called from within Yarn client
    _stop(FinalApplicationStatus.FAILED, "Shutdown by Yarn", true);
  }

  @Override
  public void onNodesUpdated(List<NodeReport> nodeReports) {
    LOG.debug("Received nodes update for '{}' nodes", nodeReports.size());
    for (NodeReport nodeReport : nodeReports) {
      if (nodeReport.getNodeState() == NodeState.RUNNING) {
        String nodeKey = getNodeName(nodeReport.getNodeId());
        nodes.put(nodeKey, nodeReport.getCapability());
        LOG.debug("Added node '{}' with '{}' cpus and '{}' memory",
            nodeKey, nodeReport.getCapability().getVirtualCores(),
            nodeReport.getCapability().getMemory());
      } else {
        LOG.debug("Removed node '{}'", nodeReport.getNodeId());
        nodes.remove(getNodeName(nodeReport.getNodeId()));
      }
    }
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public void onError(final Throwable ex) {
    LOG.error("Error in Yarn client: {}", ex.toString(), ex);
    llamaCallback.stoppedByRM();
    // no need to use a ugi.doAs() as this is called from within Yarn client
    _stop(FinalApplicationStatus.FAILED, "Error in Yarn client: " + ex
        .toString(), true);
  }

  /**
   * YARN only allows one resource size per priority, so map resource sizes
   * to priorities.
   * Should be able to remove this when YARN-314 is fixed and choose purely on
   * locality.
   */
  static Priority getRequestPriority(int mbs, int vcores,
      com.cloudera.llama.am.api.Resource.Locality locality) {
    // Lower values mean higher priority
    // More restrictive localities should get higher priority because they are
    // harder to satisfy
    // Higher values should get higher priority because they are harder to satisfy
    // Giving memory priority over CPU isn't ideal, but the alternative isn't any better
    int priority;
    switch (locality) {
      case MUST:
        priority = 1000000;
        break;
      case PREFERRED:
        priority = 2000000;
        break;
      default:
        priority = 3000000;
        break;
    }

    priority -= mbs;
    priority -= vcores;
    return Priority.newInstance(priority);
  }

}
