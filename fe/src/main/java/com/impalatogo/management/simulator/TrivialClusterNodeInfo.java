package com.impalatogo.management.simulator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by david on 9/22/14. This is static information about node hardware.
 *
 */
public class TrivialClusterNodeInfo implements IClusterNodeInfo {

	ICpuInfo cpuInfo;
	List<IStorageInfo> storageInfo = new ArrayList<IStorageInfo>();
	private static AtomicInteger currentId = new AtomicInteger(0);
	String name = String.format("Random_Name_#%d", currentId.incrementAndGet());

	@Override
	public String getNodeName() {
		return name;
	}

	@Override
	public long getMemorySize() {
		return SIZE_64G;
	}

	@Override
	public List<IStorageInfo> getStorageInfo() {
		return storageInfo;
	}

	@Override
	public ICpuInfo getCPUInfo() {
		return cpuInfo;
	}

	@Override
	public long getNetworkBandwidthToDFS() {
		return E100M_ETHERNET;
	}

	@Override
	public long getNetworkBandwidthInsideCluster() {
		return GIGABIT_ETHERNET;
	}

	@Override
	public long getFreeCacheSpace() {
		return SIZE_200G;
	}

	void setCpuInfo(ICpuInfo iCpuInfo) {
		this.cpuInfo = iCpuInfo;

	}

}
