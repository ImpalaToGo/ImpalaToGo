package com.impalatogo.management.simulator;

import java.util.List;

/**
 * Created by david on 9/22/14.
 * This is static information about node hardware.
 *
 */
public class ClusterNodeInfo {
    public final static long GIGABIT_ETHERNET = 1024 * 1024 * 1024;
    public String getNodeName()
    {
        return "RANDOM_NAME";
    }

    public long getMemorySize()
    {
        return 1024 * 1024 * 1024 * 64;
    }
    public List<DiskInfo> getDiscsInfo()
    {
        return null;
    }
    public CPUInfo getCPUInfo(){
        return null;
    }
    public long getNetworkBandwidthToDFS(){
        return GIGABIT_ETHERNET;
    }

    public long getNetworkBandwidthInsideCluster(){
        return GIGABIT_ETHERNET;
    }

    public long getFreeCacheSpace()
    {
        return 1024*1024*1024*200;
    }
}
