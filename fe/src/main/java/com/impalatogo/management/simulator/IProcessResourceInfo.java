package com.impalatogo.management.simulator;

public interface IProcessResourceInfo {

	public abstract long getDFSBandwidthUsed();

	public abstract long getClusterBandwidthUsed();

	public abstract int getCPUCurrentUse();

	public abstract long getLocalDiskBandwidthUse();

	public abstract long getLocalDiskSpaceUse();

	public abstract long getMemoryUse();

}