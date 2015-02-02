/** @file tasks-impl.cc
 *  @brief Implementation of cache layer tasks
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

std::ostream& operator<<(std::ostream& out, const taskOverallStatus value){
    static std::map<taskOverallStatus, std::string> strings;
    if (strings.size() == 0){
#define INSERT_ELEMENT(p) strings[p] = #p
        INSERT_ELEMENT(NOT_RUN);
        INSERT_ELEMENT(PENDING);
        INSERT_ELEMENT(IN_PROGRESS);
        INSERT_ELEMENT(COMPLETED_OK);
        INSERT_ELEMENT(FAILURE);
        INSERT_ELEMENT(CANCELATION_SENT);
        INSERT_ELEMENT(CANCELED_CONFIRMED);
        INSERT_ELEMENT(NOT_FOUND);
        INSERT_ELEMENT(IS_NOT_MANAGED);
#undef INSERT_ELEMENT
    }
    return out << strings[value];
}

std::ostream& operator<<(std::ostream& out, const status::StatusInternal value){
    static std::map<status::StatusInternal, std::string> strings;
    if (strings.size() == 0){
#define INSERT_ELEMENT(p) strings[p] = #p
        INSERT_ELEMENT(status::OK);
        INSERT_ELEMENT(status::OPERATION_ASYNC_SCHEDULED);
        INSERT_ELEMENT(status::OPERATION_ASYNC_REJECTED);
        INSERT_ELEMENT(status::FINALIZATION_IN_PROGRESS);
        INSERT_ELEMENT(status::REQUEST_IS_NOT_FOUND);
        INSERT_ELEMENT(status::REQUEST_FAILED);
        INSERT_ELEMENT(status::NAMENODE_IS_NOT_CONFIGURED);
        INSERT_ELEMENT(status::NAMENODE_CONNECTION_FAILED);
        INSERT_ELEMENT(status::DFS_ADAPTOR_IS_NOT_CONFIGURED);
        INSERT_ELEMENT(status::DFS_OBJECT_DOES_NOT_EXIST);
        INSERT_ELEMENT(status::DFS_NAMENODE_IS_NOT_REACHABLE);
        INSERT_ELEMENT(status::DFS_OBJECT_OPERATION_FAILURE);
        INSERT_ELEMENT(status::FILE_OBJECT_OPERATION_FAILURE);
        INSERT_ELEMENT(status::CACHE_IS_NOT_READY);
        INSERT_ELEMENT(status::CACHE_OBJECT_NOT_FOUND);
        INSERT_ELEMENT(status::CACHE_OBJECT_OPERATION_FAILURE);
        INSERT_ELEMENT(status::CACHE_OBJECT_UNDER_FINALIZATION);
        INSERT_ELEMENT(status::CACHE_OBJECT_IS_FORBIDDEN);
        INSERT_ELEMENT(status::NOT_IMPLEMENTED);
        INSERT_ELEMENT(status::NO_STATUS);
#undef INSERT_ELEMENT
    }
    return out << strings[value];
}

/**
 * @namespace request
 */
namespace request{

/***********************************************************************************************************************/
/******************************************   Single tasks ***********************************************************/
/***********************************************************************************************************************/

/***************************************  File Estimate task *********************************************************/
void FileEstimateTask::run_internal(){
	if(m_functor == NULL){
		LOG (ERROR) << "File estimate task is not initialized with a \"do work\" predicate." << "\n";
        this->m_status = taskOverallStatus::FAILURE;
	}

	// say task is in progress:
	m_status = taskOverallStatus::IN_PROGRESS;

	// run the functor and share itself allowing the worker to access the cancellation context if needed
	status::StatusInternal runstatus = m_functor( m_progress->namenode, m_progress->dfsPath.c_str(), this);

	LOG (INFO) << "File Estimate Task was executed with the worker status : \"" << runstatus << "\". \n";
    if(runstatus == status::StatusInternal::OK)
    	m_status = taskOverallStatus::COMPLETED_OK;
    else
    	m_status = taskOverallStatus::FAILURE;
}

void FileEstimateTask::callback(){
	if(m_callback == NULL){
		return;
	}
    // send file progress
	m_callback(progress());
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
            return taskOverallStatus::CANCELED_CONFIRMED;
	}
	catch(...){
		status = taskOverallStatus::FAILURE;
	}
	return status;
}

void FileEstimateTask::finalize(){
	// this task does not require finalization
}

/***************************************  File Download task *********************************************************/

void FileDownloadTask::run_internal(){
	if(m_functor == NULL){
		LOG (ERROR) << "File download task is not initialized with a \"do work\" predicate." << "\n";
        this->m_status = taskOverallStatus::FAILURE;
	}

	// say task is in progress:
	m_status = taskOverallStatus::IN_PROGRESS;

	// run the functor and share itself allowing the worker to access the cancellation context if needed
	status::StatusInternal runstatus;
	int retry = 3;
	// compare expected bytes and local bytes and retry for 3 times if not equal:
    do{
    	runstatus = m_functor( m_progress->namenode, m_progress->dfsPath.c_str(), this);
    	LOG (INFO) << "File Download Task was executed with the worker status : \"" << runstatus << "\". \n";
    }while((m_progress->localBytes != m_progress->estimatedBytes) || (retry-- > 0));

    // if still no equality for locally collected bytes and remote bytes, report an error:
    if(m_progress->localBytes != m_progress->estimatedBytes){
    	LOG (ERROR) << "File Download Task detected file inconsistency for \"" << m_progress->dfsPath.c_str() << "\".\n";
    	m_progress->error = true;
    	m_progress->errdescr = "Local file is not consistent with remote origin";
    	m_progress->progressStatus = FileProgressStatus::fileProgressStatus::FILEPROGRESS_INCONSISTENT_DATA;
    }

    if((runstatus == status::StatusInternal::OK) &&
    		(m_progress->progressStatus == FileProgressStatus::fileProgressStatus::FILEPROGRESS_COMPLETED_OK))
    	m_status = taskOverallStatus::COMPLETED_OK;
    else
    	m_status = taskOverallStatus::FAILURE;
}

void FileDownloadTask::callback(){
	if(m_callback == NULL){
		return;
	}
	// copy file progress.
	m_callback(progress());
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

void FileDownloadTask::finalize(){
	// this task does not require finalization
}

/***********************************************************************************************************************/
/******************************************   Compound tasks ***********************************************************/
/***********************************************************************************************************************/

/****************************************  Dataset Estimate task *******************************************************/

void EstimateDatasetTask::run_internal(){
	this->m_status = taskOverallStatus::IN_PROGRESS;

	// This task should run subrequests. If sync execution is requested, we wait here for all tasks being executed.
	// In asyn scenario, this thread will finish once all subrequests will be successfully enqueued for processing to a thread pool.
	// Subscribe each subrequest to a local class to report for completion:
	SingleFileProgressCompletedCallback callback =
			boost::bind(boost::mem_fn(&EstimateDatasetTask::reportSingleFileIsCompletedCallback), this, _1);

	SingleFileMakeProgressFunctor functor =
			boost::bind(boost::mem_fn(&Sync::estimateTimeToGetFileLocally), m_syncModule,
					_1, _2, _3);

	CancellationFunctor cancelation = boost::bind(boost::mem_fn(&Sync::cancelFileMakeProgress), m_syncModule, _1, _2);
	for(auto file : m_files){
		boost::shared_ptr<FileEstimateTask> taskptr(new FileEstimateTask(callback, functor, cancelation, m_namenode, file));
		// TODO use scheduler for tasks execution
		// add "single file estimate" task into the queue.
	    m_boundrequests.push_back(taskptr);
	}

	// in async scenario, offer all tasks to a thread pool basing on their order.
    if(this->async()){
    	for(auto item : m_boundrequests){
    		if(!m_pool->Offer(item)){
    			LOG (WARNING) << "failed to schedule the estimate file subrequests. Possible reason is the pool shutdown." << "\n";
    			status(taskOverallStatus::INTERRUPTED_EXTERNAL);
    			return; // do not wait for all subrequests executed! this will not happen..
    		}
    	}

    	{
    		// All subrequests are offered to a thread pool, give a signal that the pool can be used now
    		boost::lock_guard<boost::mutex> lockshceduling(m_controlDataSetScheduledMux);
    		m_controlDataSetScheduledFlag = true;
    		m_controlDataSetScheduledCondition.notify_all();
    	}

    	// and wait on this thread while all of them to be completed.
    	// do not replace with guard or scoped lock here as it will prevent the wait()
    	// from correct reacquiring the mutex.
    	boost::unique_lock<boost::mutex> lockcompletion(m_controlDataSetCompletionMux);
    	// if the condition is not satisfied (lambda returns false), wait() unlocks the mutex and put the thread onto blocked/waiting state.
    	// when condition variable is notified, the thread awakes and acquire the mutex again.
    	// Then it check for condition and if it is satisfied (lambda returns true), returns from wait()
    	// with locked mutex. If condition flag is false, unlock the mutex and wait again.
    	m_controlDataSetCompletionCondition.wait(lockcompletion, [this]{ return m_controlDataSetCompletionFlag; });
    	lockcompletion.unlock();
    	return;
    }

    // Sync scenario. Run all tasks in a sync way.
    for(auto task : m_boundrequests){
    	task->operator ()();
    }
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

	// invoke cancellation
    m_cancelation(requestIdentity{session(), timestampstr()}, m_namenode, this->priority(), true, this->async());

    // if async, we will go out from here immediately.
    if(async)
       	return taskOverallStatus::CANCELATION_SENT;

    // else wait on condition variable before return
	return taskOverallStatus::CANCELED_CONFIRMED;
}

std::list<boost::shared_ptr<FileProgress> > EstimateDatasetTask::progress(){
	std::list<boost::shared_ptr<FileProgress> > progress;
      for(auto item : m_boundrequests)
    	  progress.push_back(item->progress());
      return progress;
}

void EstimateDatasetTask::reportSingleFileIsCompletedCallback(const boost::shared_ptr<FileProgress>& progress){
	if(status() == taskOverallStatus::INTERRUPTED_EXTERNAL){
		LOG (INFO) << "Parent Estimate DataSet task is interrupted." << "\n";
	}
	if(m_remainedFiles == 0){
		LOG (ERROR) << "Bug in EstimateDatasetTask implementation" << "\n";
		// anyway try to release the async locked scenario:
		if(this->async()){
			boost::lock_guard<boost::mutex> lock(m_controlDataSetCompletionMux);
			m_controlDataSetCompletionFlag = true;
			m_controlDataSetCompletionCondition.notify_all();
		}
		return;
	}

	if(progress->error){
		LOG (WARNING) << "File \"" << progress->dfsPath << "\" is NOT estimated due to error : \"" << progress->errdescr << "\".\n";
	}
	else
		LOG (INFO) << "File \"" << progress->dfsPath << "\" is estimated with a size : " << progress->estimatedBytes << "; time : " <<
		progress->estimatedTime << ".\n";

	// decrement number of remained subtasks
	--m_remainedFiles;
	if(m_remainedFiles != 0){
		// just do nothing
		return;
	}

	// All subtasks are done.
	status(taskOverallStatus::COMPLETED_OK);

	// Summarize the overall task status.
	for(auto request : m_boundrequests){
		if(request->failure() ){
			status(taskOverallStatus::FAILURE);
		}
	}

	// In async scenario,
	// notify the thread running this compound task that all subtasks are done within this task.
	if(this->async()){
		boost::lock_guard<boost::mutex> lock(m_controlDataSetCompletionMux);
		m_controlDataSetCompletionFlag = true;
		m_controlDataSetCompletionCondition.notify_all();
	}
}

void EstimateDatasetTask::callback(){
	// callback to the client
	// query all subtasks about their status
	// call the callback.
	std::list<boost::shared_ptr<FileProgress> > _progress = this->progress();
	std::time_t estimatedtime = 0;
	for(auto item : _progress){
		estimatedtime += item->estimatedTime;
	}
	m_callback(m_session, _progress, estimatedtime, (m_status != taskOverallStatus::FAILURE), m_controlCancelationCompletionFlag, status());
}

void EstimateDatasetTask::finalize(){
	// then callback to the cache manager
    m_functor(requestIdentity{session(), timestampstr()}, m_namenode, this->priority(), false, this->async());
}

/****************************************  Dataset Prepare task *******************************************************/

void PrepareDatasetTask::run_internal(){
    // This task should enqueue all bound misc tasks into workers queue.
	// Subscribe each subrequest to a local class to report for completion:
	SingleFileProgressCompletedCallback callback =
			boost::bind(boost::mem_fn(&PrepareDatasetTask::reportSingleFileIsCompletedCallback), this, _1);

	SingleFileMakeProgressFunctor functor =
			boost::bind(boost::mem_fn(&Sync::prepareFile), m_syncModule, _1, _2, _3);

	CancellationFunctor cancelation = boost::bind(boost::mem_fn(&Sync::cancelFileMakeProgress), m_syncModule, _1, _2);

	for(auto file : m_files){
		boost::shared_ptr<FileDownloadTask> taskptr(new FileDownloadTask(callback, functor, cancelation, m_namenode, file));
		// TODO use scheduler for tasks execution
		// add "single file download" task into the queue.
	    m_boundrequests.push_back(taskptr);
	}

	// in async scenario, offer all tasks to a thread pool basing on their order.
    if(this->async()){
    	for(auto item : m_boundrequests){
    		if(!m_pool->Offer(item)){
    			LOG (WARNING) << "failed to schedule the prepare file subrequests. Possible reason is the pool shutdown." << "\n";
    			status(taskOverallStatus::INTERRUPTED_EXTERNAL);
    			return; // do not wait for all subrequests executed! this will not happen..
    		}
    	}

    	{
    		// All subrequests are offered to a thread pool, give a signal that the pool can be used now
    		boost::lock_guard<boost::mutex> lockshceduling(m_controlDataSetScheduledMux);
    		m_controlDataSetScheduledFlag = true;
    		m_controlDataSetScheduledCondition.notify_all();
    	}

    	// and wait on this thread while all of them to be completed.
    	// do not replace with guard or scoped lock here as it will prevent the wait()
    	// from correct reacquiring the mutex.
    	boost::unique_lock<boost::mutex> lockcompletion(m_controlDataSetCompletionMux);
    	// if the condition is not satisfied (lambda returns false), wait() unlocks the mutex and put the thread onto blocked/waiting state.
    	// when condition variable is notified, the thread awakes and acquire the mutex again.
    	// Then it check for condition and if it is satisfied (lambda returns true), returns from wait()
    	// with locked mutex. If condition flag is false, unlock the mutex and wait again.
    	m_controlDataSetCompletionCondition.wait(lockcompletion, [this]{ return m_controlDataSetCompletionFlag; });
    	lockcompletion.unlock();
    	return;
    }

    // Sync scenario. Run all tasks in a sync way.
    for(auto task : m_boundrequests){
    	task->operator ()();
    }
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

    m_cancelation(requestIdentity{session(), timestampstr()}, m_namenode, this->priority(), true, this->async());
    // if async, we will go out from here immediately.
    if(async)
       	return taskOverallStatus::CANCELATION_SENT;

    // else wait on condition variable before return
	return taskOverallStatus::CANCELED_CONFIRMED;
}

std::list<boost::shared_ptr<FileProgress> > PrepareDatasetTask::progress(){
	std::list<boost::shared_ptr<FileProgress> > progress;
      for(auto item : m_boundrequests)
    	  progress.push_back(item->progress());
      return progress;
}

void PrepareDatasetTask::reportSingleFileIsCompletedCallback(const boost::shared_ptr<FileProgress>& progress){
	if(status() == taskOverallStatus::INTERRUPTED_EXTERNAL){
		LOG (INFO) << "Parent Prepare DataSet task is interrupted." << "\n";
	}

	if(m_remainedFiles == 0){
		LOG (ERROR) << "Bug in PrepareDatasetTask implementation" << "\n";
		// anyway try to release the async locked scenario:
		if(this->async()){
			boost::lock_guard<boost::mutex> lock(m_controlDataSetCompletionMux);
			m_controlDataSetCompletionFlag = true;
			m_controlDataSetCompletionCondition.notify_all();
		}
		return;
	}

	if(progress->error){
		LOG (WARNING) << "File \"" << progress->dfsPath << "\" is NOT prepared due to error : \"" << progress->errdescr << "\".\n";
	}
	else
		LOG (INFO) << "File \"" << progress->dfsPath << "\" is loaded with a size : " <<
			std::to_string(progress->localBytes) << "; time : " <<
		progress->estimatedTime << ".\n";

	// decrement number of remained subtasks
	--m_remainedFiles;
	if(m_remainedFiles != 0){
		// just do nothing
		return;
	}

	// All subtasks are done.
	status(taskOverallStatus::COMPLETED_OK);

	// Summarize the overall task status.
	for(auto request : m_boundrequests){
		if(request->failure() ){
			status(taskOverallStatus::FAILURE);
		}
	}

	// All subtasks are done. In async scenario,
	// notify the thread running this compound task that all subtasks are done within this task.
	if(this->async()){
		boost::lock_guard<boost::mutex> lock(m_controlDataSetCompletionMux);
		m_controlDataSetCompletionFlag = true;
		m_controlDataSetCompletionCondition.notify_all();
	}
}

void PrepareDatasetTask::callback(){
	 m_callback(m_session, this->progress(), this->performance(), (m_status != taskOverallStatus::FAILURE), m_controlCancelationCompletionFlag, status());
}

void PrepareDatasetTask::finalize(){
	// then callback to the cache manager
    m_functor(requestIdentity{session(), timestampstr()}, this->m_namenode, this->priority(), false, this->async());
}

} // request
} // impala
