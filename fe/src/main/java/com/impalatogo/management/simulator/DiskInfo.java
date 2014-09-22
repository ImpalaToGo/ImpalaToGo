package com.impalatogo.management.simulator;

/**
 * Created by david on 9/22/14.
 * Real implementation will require running something like IOMeter.
 */
public class DiskInfo {
    enum DiskType {FLASH, MAGNETIC, REMOTE}
    public DiskType getDiskType()
    {
        return DiskType.FLASH;
    }
    public long getIOPs()
    {
        return 1000;
    }
    public long getReadBandwidth()
    {
        return 1024 * 1024 * 100;
    }
    public long getWriteBandwidth()
    {
        return 1024 * 1024 * 100;
    }

}
