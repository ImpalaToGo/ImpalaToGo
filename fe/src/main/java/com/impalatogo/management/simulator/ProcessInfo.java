package com.impalatogo.management.simulator;

import java.util.Date;

/**
 * Created by david on 9/22/14.
 */
public class ProcessInfo {
    enum ProcessType {QUERY, CACHE_WARM_UP}
    enum ProcessStatus {WAITING_TO_START, RUNNING, FINISHED, FAIELD}
    public String getProcessID()
    {
        return "SomeID";
    }
    public String getProcessDefinitionText()
    {
        return "SomeText";
    }
    public ProcessType getProcessType()
    {
        return ProcessType.QUERY;
    }
    public Date getProcessStartTime()
    {
        return new Date();
    }
    public Date getProcessEndTime()
    {
        return new Date();
    }
    public int getProgress()
    {
        return 33;
    }

}
