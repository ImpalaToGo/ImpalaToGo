package com.impalatogo.management.simulator;

import java.util.List;
import java.util.Map;

/**
 * Created by david on 9/22/14. This interface represent information about the
 * current cluster topology, health and other information which is of interest
 * to the user interface.
 */
public interface IClusterState {
	public List<IClusterNodeInfo> getNodesInfo();

	public List<IImpalaTaskInfo> getCurrentProcesses();

	public Map<String, IProcessResourceInfo> getProcessResourcesById(
			String processID);
}
