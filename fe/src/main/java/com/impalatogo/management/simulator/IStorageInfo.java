package com.impalatogo.management.simulator;

/**
 * Created by david on 9/22/14.
 * Real implementation will require running something like IOMeter.
 */
public interface IStorageInfo {
    
    long getIOPs();
    /**in byte/S*/
    long getReadBandwidth();
    /**in byte/S*/
    long getWriteBandwidth();
    /**in nS*/
    long getReadTime();
    /**in nS*/
    long getWriteTime();
    boolean isLocal();
}
