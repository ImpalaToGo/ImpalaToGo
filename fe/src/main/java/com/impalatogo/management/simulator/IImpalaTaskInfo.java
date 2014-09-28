package com.impalatogo.management.simulator;

import java.util.Date;

public interface IImpalaTaskInfo {
    enum TaskType {QUERY, CACHE_WARM_UP}
    enum TaskStatus {WAITING_TO_START, RUNNING, FINISHED, FAIELD}
	String getProcessID();

	String getProcessDefinitionText();

	TaskType getProcessType();

	TaskStatus getTaskStatus();
	
	Date getProcessStartTime();

	Date getProcessEndTime();

	int getProgress();

}