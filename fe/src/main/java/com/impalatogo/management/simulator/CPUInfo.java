package com.impalatogo.management.simulator;

/**
 * Created by david on 9/22/14.
 * This class represent information about CPU installed on the machine.
 * On many clouds it can vary so information can be useful.
 */
public class CPUInfo {
    public String getModel()
    {
        return "XEON2452";
    }
    public int getNumOfCores()
    {
        return 4;
    }
    public int getNumOfThreads()
    {
        return 8;
    }
    public int getSpeedMeasure()
    {
        return 100;
    }
}

