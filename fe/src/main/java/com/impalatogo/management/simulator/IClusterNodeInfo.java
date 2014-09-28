package com.impalatogo.management.simulator;

import java.util.List;

public interface IClusterNodeInfo {

	public static final int SIZE_200G = 1024 * 1024 * 1024 * 200;
	public static final int SIZE_64G = 1024 * 1024 * 1024 * 64;
	public static final long GIGABIT_ETHERNET = 1024 * 1024 * 1024;
	public static final long E100M_ETHERNET = 100 * 1024 * 1024;

	String getNodeName();

	long getMemorySize();

	List<IStorageInfo> getStorageInfo();

	ICpuInfo getCPUInfo();

	long getNetworkBandwidthToDFS();

	long getNetworkBandwidthInsideCluster();

	long getFreeCacheSpace();

}