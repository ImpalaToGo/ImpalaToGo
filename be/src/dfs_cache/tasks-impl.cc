/** @file request-definitions.cpp
 *  @brief Implementation of cache-managed entities
 *
 *  @date   Oct 01, 2014
 *  @author elenav
 */

#include <boost/lambda/lambda.hpp>
#include "dfs_cache/tasks-impl.hpp"
#include "dfs_cache/sync-module.hpp"

/**
 * @namespace impala
 */
namespace impala{
/**
 * @namespace request
 */
namespace request{

/******************************************   Single tasks ***********************************************************/

taskOverallStatus FileEstimateTask::run_internal(){
	if(m_functor == NULL){
       return taskOverallStatus::FAILURE;
	}

	// say task is in progress:
	m_status = taskOverallStatus::IN_PROGRESS;

	// run the functor, share the synchronization objects required for cancellation support.
	// pass addresses of synchronization objects

	m_functor( m_progress->namenode, m_progress->dfsPath.c_str(), this);

	// check cancellation flag:
	if(condition() == true)
		return m_status = taskOverallStatus::CANCELED_CONFIRMED;
	/*if(status.code() == TStatusCode::OK)
		return m_status = taskOverallStatus::COMPLETED_OK;
	else
		return m_status = taskOverallStatus::FAILURE;
		*/
	return taskOverallStatus::COMPLETED_OK;
}

void FileEstimateTask::callback(){
	if(m_callback == NULL){
		return;
	}
	// copy file progress.
	m_callback(*(progress().get()));
}

taskOverallStatus FileEstimateTask::cancel(bool async){

	taskOverallStatus status = taskOverallStatus::IS_NOT_MANAGED;
	if(m_cancelation == NULL){
		return status;
	}
	try{
		m_cancelation(async, this);

		// if async request handling is required, we will be here immediately and return
		if(async)
			return taskOverallStatus::CANCELATION_SENT;
		else
			// return status.code() == TStatusCode::OK ? taskOverallStatus::CANCELED_CONFIRMED : taskOverallStatus::FAILURE;
            return taskOverallStatus::CANCELED_CONFIRMED;
	}
	catch(...){
		status = taskOverallStatus::FAILURE;
	}
	return status;
}

taskOverallStatus FileDownloadTask::run_internal(){
	if(m_functor == NULL){
       return taskOverallStatus::FAILURE;
	}

	// say task is in progress:
	m_status = taskOverallStatus::IN_PROGRESS;

	// run the functor, share the cancellation token
	m_functor( m_progress->namenode, m_progress->dfsPath.c_str(), this);

	// check cancellation flag:
	if(condition() == true)
		return m_status = taskOverallStatus::CANCELED_CONFIRMED;
	/*if(status.code() == TStatusCode::OK)
		return m_status = taskOverallStatus::COMPLETED_OK;
	else
		return m_status = taskOverallStatus::FAILURE;
		*/
	return taskOverallStatus::COMPLETED_OK;
}

void FileDownloadTask::callback(){
	if(m_callback == NULL){
		return;
	}
	// copy file progress.
	m_callback(*(progress().get()));
}

taskOverallStatus FileDownloadTask::cancel(bool async){

	taskOverallStatus status = taskOverallStatus::IS_NOT_MANAGED;
	if(m_cancelation == NULL){
		return status;
	}
	try{
		m_cancelation(async, this);

		// if async request handling is required, we will be here immediately and return
		if(async)
			return taskOverallStatus::CANCELATION_SENT;
		else
			// return status.code() == TStatusCode::OK ? taskOverallStatus::CANCELED_CONFIRMED : taskOverallStatus::FAILURE;
            return taskOverallStatus::CANCELED_CONFIRMED;
	}
	catch(...){
		status = taskOverallStatus::FAILURE;
	}
	return status;
}

/******************************************   Compound tasks ***********************************************************/

taskOverallStatus EstimateDatasetTask::run_internal(){
    // This task should enqueue all bound misc tasks into workers queue.
	// Subscribe each subrequest to a local class to report for completion:
	SingleFileProgressCompletedCallback callback =
			boost::bind(boost::mem_fn(&EstimateDatasetTask::reportSingleFileIsCompletedCallback), this, _1);

	SingleFileMakeProgressFunctor functor =
			boost::bind(boost::mem_fn(&Sync::estimateTimeToGetFileLocally), m_syncModule,
					_1, _2, _3);
	for(auto file : m_files){
		FileEstimateTask* task = new FileEstimateTask(callback, functor, m_cancelation, m_namenode, file);
		// TODO use scheduler for tasks execution
		// add "single file estimate" task into the queue.
	    m_boundrequests.push_back(task);
	}

	// Send all tasks to a thread pool:
	// and wait for them to complete:


   return taskOverallStatus::IN_PROGRESS;
}

taskOverallStatus EstimateDatasetTask::cancel(bool async){

	// First run cancellation of all bound single-file-prepare requests
	bool subrequest_failure = false;
	for (auto & request : m_boundrequests)
	{
		if(request->status() == taskOverallStatus::IN_PROGRESS){
			m_status = request->cancel(async);
			if(m_status != taskOverallStatus::CANCELATION_SENT ||
					m_status != taskOverallStatus::CANCELED_CONFIRMED)
				subrequest_failure = true;
		}
	}
	m_status = subrequest_failure ? taskOverallStatus::FAILURE : m_status;

    m_cancelation(async, this);
    // if async, we will go out from here immediately.
    if(async)
       	return taskOverallStatus::CANCELATION_SENT;

    // else wait on condition variable before return
	return taskOverallStatus::CANCELED_CONFIRMED;
}

std::list<FileProgress> EstimateDatasetTask::progress(){
	std::list<FileProgress> progress;
      for(auto item : m_boundrequests)
    	  progress.push_back(*(item->progress().get()));
      return progress;
}
status::StatusInternal EstimateDatasetTask::reportSingleFileIsCompletedCallback(FileProgress const & progress){
    return status::StatusInternal::OK;
}

void EstimateDatasetTask::callback(){
}


taskOverallStatus PrepareDatasetTask::run_internal(){
    // This task should enqueue all bound misc tasks into workers queue.
	// Subscribe each subrequest to a local class to report for completion:
	SingleFileProgressCompletedCallback callback =
			boost::bind(boost::mem_fn(&PrepareDatasetTask::reportSingleFileIsCompletedCallback), this, _1);

	SingleFileMakeProgressFunctor functor =
			boost::bind(boost::mem_fn(&Sync::prepareFile), m_syncModule, _1, _2, _3);
	for(auto file : m_files){
		FileDownloadTask* task = new FileDownloadTask(callback, functor, m_cancelation, m_namenode, file);
		// TODO use scheduler for tasks execution
		// add "single file download" task into the queue.
	    m_boundrequests.push_back(task);
	}

   return taskOverallStatus::IN_PROGRESS;
}

taskOverallStatus PrepareDatasetTask::cancel(bool async){

	// First run cancellation of all bound single-file-prepare requests
	bool subrequest_failure = false;
	for (auto & request : m_boundrequests)
	{
		if(request->status() == taskOverallStatus::IN_PROGRESS){
			m_status = request->cancel(async);
			if(m_status != taskOverallStatus::CANCELATION_SENT ||
					m_status != taskOverallStatus::CANCELED_CONFIRMED)
				subrequest_failure = true;
		}
	}
	m_status = subrequest_failure ? taskOverallStatus::FAILURE : m_status;

    m_cancelation(async, this);
    // if async, we will go out from here immediately.
    if(async)
       	return taskOverallStatus::CANCELATION_SENT;

    // else wait on condition variable before return
	return taskOverallStatus::CANCELED_CONFIRMED;
}

std::list<FileProgress> PrepareDatasetTask::progress(){
	std::list<FileProgress> progress;
      for(auto item : m_boundrequests)
    	  progress.push_back(*(item->progress().get()));
      return progress;
}

status::StatusInternal PrepareDatasetTask::reportSingleFileIsCompletedCallback(FileProgress const & progress){
    return status::StatusInternal::OK;
}

void PrepareDatasetTask::callback(){
     // query all subtasks about their status
	 // call the callback.
     std::list<FileProgress> progress;
     for(auto item : m_boundrequests){
    	 progress.push_back(*(item->progress().get()));
     }
	 m_callback(m_session, progress, performance(), (m_status != taskOverallStatus::FAILURE), m_condition);
}

} // request
} // impala
