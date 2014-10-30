/*
 * @file cache-mgr.cpp
 * @brief implementation of Cache Manager
 *
 *  Created on: Oct 3, 2014
 *      Author: elenav
 */

/**
 * @namespace impala
 */
#include <string.h>
#include <boost/lexical_cast.hpp>
#include <boost/multi_index/identity.hpp>
#include <boost/multi_index_container.hpp>
#include "cache-mgr.hpp"

namespace impala {

#define MAX_PATH 256
#define PORT_LEN 5

/************************************************************************************************************************************************/
/***************************************   Utilities  *******************************************************************************************/
/************************************************************************************************************************************************/

bool operator==(MonitorRequest const & request1, MonitorRequest const & request2)
{
    return request1.session() == request2.session() && request1.timestamp() == request2.timestamp();
}

std::size_t hash_value(MonitorRequest const& request) {
    std::size_t seed = 0;
    boost::hash_combine(seed, request.session());
    boost::hash_combine(seed, request.timestampstr());
    return seed;
}

bool operator==(HistoricalCacheRequest const & request1, HistoricalCacheRequest const & request2)
{
    return request1.identity.ctx == request2.identity.ctx && request1.identity.timestamp == request2.identity.timestamp;
}

std::size_t hash_value(HistoricalCacheRequest const& request) {
    std::size_t seed = 0;
    boost::hash_combine(seed, request.identity.ctx);
    boost::hash_combine(seed, request.identity.timestamp);
    return seed;
}

/* Singleton instance */
boost::scoped_ptr<CacheManager> CacheManager::instance_;

/************************************************************************************************************************************************/
/***************************************   Initialization and shutdown  *************************************************************************/
/************************************************************************************************************************************************/
void CacheManager::init() {
  if(CacheManager::instance_.get() == NULL)
	  CacheManager::instance_.reset(new CacheManager());
}

/** The order of operations within the shutdown scenario is mandatory */
status::StatusInternal CacheManager::shutdown(bool force, bool updateClients){
	if(m_shutdownFlag){
		LOG (WARNING) << "shutdown already requested. No actions will be taken." << "\n";
		return status::StatusInternal::FINALIZATION_IN_PROGRESS;
	}

	// publish the shutdown flag to this module
	m_shutdownFlag = true;

	// Now cleanup all cache manager queues to prevent new data to be found by dispatchers on the current iteration.y
	// If any request is already on the fly, it will be canceled ASAP and there's no interest in its statistic more.
	finalize<ClientRequests, MonitorRequest>(&m_activeHighRequests, &m_highrequestsMux);
	finalize<ClientRequests, MonitorRequest>(&m_activeLowRequests, &m_lowrequestsMux);
	finalize<ClientRequests, MonitorRequest>(&m_syncRequestsQueue, &m_SyncRequestsMux);

	// finalize the history:
	// finalize<HistoryOfRequests, HistoricalCacheRequest>(&m_HistoryRequests, &m_HistoryMux);
	HistoryOfRequests().swap(m_HistoryRequests);

    // shutdown thread pools in a graceful way so that they will finish all enqueued work while will not
	// accept newly offered if happens to be offered with.
	m_shortpool.Shutdown(); // if dispatcher is on a blocking offer now, it will be unblocked after this.
	                        // if dispatcher is on blocking wait for compound request to complete blocking offering,
	                        // as compound request will be unblocked, the dispatcher will be unblocked as well.
	m_longpool.Shutdown();

	// wait for pools to complete jobs that were already on the fly
	m_shortpool.Join();
	m_longpool.Join();

	// set data condition variables for all dispatchers so that they are able to awake and read shutdown flag (see condition 1 for dispatcher).
	boost::unique_lock<boost::mutex> datalockLong(m_highrequestsMux);
	m_controlHighRequestsArrival.notify_all();
	datalockLong.unlock();

	boost::unique_lock<boost::mutex> datalockShort(m_lowrequestsMux);
	m_controlLowRequestsArrival.notify_all();
	datalockShort.unlock();

	// and wait for dispatchers to finalize:
	// wait for short requests dispatcher to finalize:
	boost::unique_lock<boost::mutex> lockshortpool(m_shortThreadIsDoneMux);
	m_shortthreadIsDoneCondition.wait(lockshortpool, [this] { return m_shortThreadIsDoneFlag; });
	lockshortpool.unlock();

	// wait for long requests dispatcher to finalize:
	boost::unique_lock<boost::mutex> locklongpool(m_longThreadIsDoneMux);
	m_longthreadIsDoneCondition.wait(locklongpool, [this] { return m_longThreadIsDoneFlag; } );
	locklongpool.unlock();

	// this is redundant due to the architecture of dispatchers working function.
	m_LowPriorityQueueThread->Join();
	m_HighPriorityQueueThread->Join();

	// cleanup all
	m_LowPriorityQueueThread.reset();
	m_HighPriorityQueueThread.reset();

	return status::StatusInternal::OK;
}

/************************************************************************************************************************************************/
/***************************************   Internals  *******************************************************************************************/
/************************************************************************************************************************************************/

template<typename IndexType_, typename ItemType_>
void CacheManager::finalize(IndexType_* queue, boost::mutex* mux){
	boost::lock_guard<boost::mutex> lock(*mux);
	// Cancel everything that now in a running state
	std::for_each(queue->begin(), queue->end(), [](boost::shared_ptr<ItemType_> request){
		if(request->status() == taskOverallStatus::IN_PROGRESS)
			request->cancel(); });

	// clean the queue
	IndexType_().swap(*queue);
}

void CacheManager::dispatcherProc(dfsThreadPool* pool, int threadnum, const boost::shared_ptr<request::Task>& task ){
    if(m_shutdownFlag || task->invalidated())
    	return;
	task->operator ()();
}

/* Dispatcher may be locked in following places and so should be unlocked in one or another way:
 * - wait for request with "NOT_RUN" status appears in the queue                       (condition 1)
 * - wait on blocking Offer() operation to a bound thread pool                         (condition 2)
 * - wait for signal from compound task that those task completed its own offerings.   (condition 3)
 *
 * It routes compound request to a pool which is shared with that compound request and therefore is available
 * for compound requests offerings. As compound requests along with their subtasks should run FIFO,
 * dispatcher, once scheduled the compound task onto the pool, should wait for that task to complete its own scheduling -
 * in order to accept new compound requests and offer them to a shared pool.
 *
 * Finalization scenario:
 * dispatcher once detecting finalization flag, should stop process the bound queue of requests and go out.
 * How this happens:
 * - if it is waiting on the condition (1) - it releases the lock and go out
 * - if it dropped from Offer() - condition 2 - it simply go out
 * - if it dropped from the waiting on condition 3 - it simply make a new try and detects the finalization flag is set,
 * so no new iteration happens.
 *
 * In any case, when the main dispatcher loop finishes, dispatcher should signal the condition "dispatcher finalized".
 * Thus we know no handler exists for requests queue and release them safely.
 * */
void CacheManager::dispatchRequest(requestPriority priority){
		// select work instrumentation.
		boost::mutex*              mux;               // mutex to guard the selected collection
		ClientRequests*            requests;          // queue to analyze for arrivals
        boost::condition_variable* conditionvariable; // condition variable to wait on
        dfsThreadPool*             pool;              // pool to offer the request

        // shutdown sync:
        bool*                       shutdownFlag;      // shutdown confirmation by dispatcher
        boost::mutex*               shutdownMux;       // mutex to guard the confirmation
        boost::condition_variable*  shutdownCondition; // shutdown confirmation event

		switch(priority){
		case requestPriority::HIGH:

			mux               = &m_highrequestsMux;
			requests          = &m_activeHighRequests;
			conditionvariable = &m_controlHighRequestsArrival;
			pool              = &m_shortpool;

			shutdownFlag      = &m_shortThreadIsDoneFlag;
			shutdownMux       = &m_shortThreadIsDoneMux;
			shutdownCondition = &m_shortthreadIsDoneCondition;
			break;
		case requestPriority::LOW:

			mux               = &m_lowrequestsMux;
			requests          = &m_activeLowRequests;
			conditionvariable = &m_controlLowRequestsArrival;
			pool              = &m_longpool;

			shutdownFlag      = &m_longThreadIsDoneFlag;
			shutdownMux       = &m_longThreadIsDoneMux;
			shutdownCondition = &m_longthreadIsDoneCondition;
			break;

		case requestPriority::NOT_SET:
			return;
		}

		// do work while global shutdown flag is not received
		while(!m_shutdownFlag){
			// single request
			boost::shared_ptr<MonitorRequest>           request;
			// iterator through requests
			ClientRequests::nth_index_iterator<0>::type iterator;

			boost::unique_lock<boost::mutex> lock(*mux);
			// Wait for either shutdown happens or "not run" request detected in the supervised list of active requests
            (*conditionvariable).wait(lock, [&](){ return [&](){
            	auto it = std::find_if(requests->begin(), requests->end(),
            			[&](boost::shared_ptr<MonitorRequest> req){ return req->status() == taskOverallStatus::NOT_RUN; } );
            			if(it != requests->end())
            				return true;
            			return false;
            }() || m_shutdownFlag; });

            // if shutdown requested, interrupt everything. (Condition 1)
        	if(m_shutdownFlag){
        		lock.unlock();
        		LOG (INFO) << "dispatcher detects shutdown condition and is finalizing." << "\n";
        		break;
        	}

            // Get the top "not run" iterator from the queue of requests
        	auto it = std::find_if(requests->begin(), requests->end(),
        			[&](boost::shared_ptr<MonitorRequest> req){ return req->status() == taskOverallStatus::NOT_RUN; } );

        	if(it == requests->end()){
            	// Why we are here?
            	LOG (WARNING) << "False invitation to handle request" << "\n";
            	lock.unlock();
            	continue;
        	}
        	// here's new request to run!
        	request = (*it);
            lock.unlock();

            if(request == NULL){
            	LOG (WARNING) << "No request found!" << "\n";
            	continue;
            }

            // set "PENDING" status to this request.
            request->status(taskOverallStatus::PENDING);

            // Requests queue should be unlocked until now as further operation is blocking.
            // if there's request found, offer it to a corresponding thread pool.
            if(!pool->Offer(request)){                                                         // (Condition 2)
            	// check that the shutdown event was signaled:
            	if(m_shutdownFlag)
            		LOG (INFO) << priority << " priority requests dispatch exiting due shutdown. Thread pool was shutdown." << "\n";
            	else
            		LOG (ERROR) << priority << " priority requests dispatch exiting due to unknown reason." << "\n";
            	// go out anyway.
            	break;
            }
            // wait here until the request will finish its offering to a thread pool
            // or until the external shutdown requested.
            request->waitScheduled([&](){ return m_shutdownFlag || request->scheduled(); });
		}
		// If here, check for shutdown flag:
		if(m_shutdownFlag){
			*shutdownFlag = true;
			// if shutdown was requested, notify it was performed on this dispatcher:
			boost::lock_guard<boost::mutex> lock(*shutdownMux);
			(*shutdownCondition).notify_one();
		}
	}

void CacheManager::finalizeUserRequest(const requestIdentity& requestIdentity, const NameNodeDescriptor & namenode,
		requestPriority priority, bool cancelled, bool async ){

	if(m_shutdownFlag){
		LOG (INFO) << "finalizeUserRequest : " << "request finalization canceled. Global finalization is in progress" << "\n";
		return;
	}

	// select work instrumentation.
	boost::mutex*   mux;              // mutex to guard the selected collection
	ClientRequests* requests;         // queue to analyze for arrivals
    std::string     message;          // message to specify the queue which is handled.

	// If request is async
	if(async){
		switch(priority){
			case requestPriority::HIGH:
				mux      = &m_highrequestsMux;
				requests = &m_activeHighRequests;
				message  = "high priority queue";
				break;
			case requestPriority::LOW:
				mux      = &m_lowrequestsMux;
				requests = &m_activeLowRequests;
				message  = "low priority queue";
				break;

			case requestPriority::NOT_SET:
				LOG (WARNING) << "non-prioritized request reached finalization and cannot be fnalized." << "\n";
				return;
			}
	}
	else { // request is sync
		mux      = &m_SyncRequestsMux;
		requests = &m_syncRequestsQueue;
		message  = "sync requests eue";
	}

	// Async scenario is handled here:
	boost::mutex::scoped_lock activeLock(*mux);
	// get the request from the active requests:
	RequestsBySessionAndTimestampTag& filtered_requests = requests->get<session_timestamp_tag>();
	auto it = filtered_requests.find(boost::make_tuple(requestIdentity.timestamp, requestIdentity.ctx));

	// if anything found, remove it from the "active" requests and place it to "history":
    if(it == filtered_requests.end()){
    	// nothing to do, just log the BUG
    	LOG (ERROR) << "Finalize request. Unable to locate request in " << message << " to finalize it. Request timestamp : "
    			<< requestIdentity.timestamp << "\n";
    	return;
    }

    // if there's request found, get it before it will be removed from active requests.
    boost::shared_ptr<MonitorRequest> request = (*it);

    // and remove it from active requests:
    requests->remove_if([&](boost::shared_ptr<MonitorRequest> request) { return request->session() == requestIdentity.ctx &&
        		request->timestampstr() == requestIdentity.timestamp; });
    activeLock.unlock();

    // create the history request from this one:
    boost::shared_ptr<HistoricalCacheRequest> historical(new HistoricalCacheRequest());
    historical->canceled    = (request->status() == taskOverallStatus::CANCELED_CONFIRMED ||
    						   request->status() == taskOverallStatus::CANCELATION_SENT);
    historical->identity    = requestIdentity;
    historical->status      = request->status();
    historical->performance = request->performance();
    historical->progress    = request->progress();
    historical->succeed     = (request->status() == taskOverallStatus::COMPLETED_OK);

    boost::unique_lock<boost::mutex> historyLock(m_HistoryMux);
    // now add it to History, add it first as most recent:
    m_HistoryRequests.push_front(std::move(historical));
    historyLock.unlock();

    LOG (INFO) << "Finalize request. Request was moved to history. Status : " << request->status() << "; Request timestamp : " << requestIdentity.timestamp << "\n";
}

/************************************************************************************************************************************************/
/***************************************   API  *************************************************************************************************/
/************************************************************************************************************************************************/

status::StatusInternal CacheManager::cacheEstimate(SessionContext session, const NameNodeDescriptor & namenode, const DataSet& files, time_t& time,
		   CacheEstimationCompletedCallback callback, requestIdentity & requestIdentity, bool async) {

	if(m_shutdownFlag){
		LOG (INFO) << "cacheEstimate : " << "request will not be handled. Finalization is in progress" << "\n";
		return status::StatusInternal::FINALIZATION_IN_PROGRESS;
	}

    // subscribe the request to come back to cache manager when it will be finished for further management
    DataSetRequestCompletionFunctor functor = boost::bind(boost::mem_fn(&CacheManager::finalizeUserRequest), this, _1, _2, _3, _4, _5);

    // create the task "CacheEstimationTask"
    boost::shared_ptr<MonitorRequest> request(new request::EstimateDatasetTask(callback, functor, functor, session, namenode, m_syncModule,
    		&m_shortpool, files, async));

    // assign the request identity
    requestIdentity.ctx = session;
    requestIdentity.timestamp = request->timestampstr();

    if(async){
    	boost::lock_guard<boost::mutex> lock(m_highrequestsMux);
    	// send task to the queue for further processing:
    	auto it = m_activeHighRequests.push_back(request);
    	// if there was a problem to insert the request, reply it
    	if(!it.second){
    		LOG (WARNING) << "Unable to schedule estimate request for processing." << "\n";
    		return status::StatusInternal::OPERATION_ASYNC_REJECTED;
    	}
    	// notify new data arrival
    	m_controlHighRequestsArrival.notify_all();
    	// if async, go out immediately:
		return status::StatusInternal::OPERATION_ASYNC_SCHEDULED;
    }

    // request is sync and should be marked as that so that it will not be handled by scheduler
    boost::unique_lock<boost::mutex> lock(m_SyncRequestsMux);
    // now add it to History, add it first as most recent:
    auto it = m_syncRequestsQueue.push_back(request);
    lock.unlock();

	// if there was a problem to insert the request, reply it
	if(!it.second){
		LOG (WARNING) << "Unable to schedule estimate request for processing." << "\n";
		std::cout << "Unable to schedule estimate request for processing.\n";
		return status::StatusInternal::OPERATION_ASYNC_REJECTED;
	}

    // and executed on the caller thread:
    request->operator ()();
	// in a sync way, need to wait for task completion.
    // check for request status:
    taskOverallStatus status = request->status();
	return status == taskOverallStatus::COMPLETED_OK ? status::StatusInternal::OK : status::StatusInternal::REQUEST_FAILED;
}

status::StatusInternal CacheManager::cachePrepareData(SessionContext session, const NameNodeDescriptor & namenode, const DataSet& files,
     		  PrepareCompletedCallback callback, requestIdentity & requestIdentity){

	if(m_shutdownFlag){
		LOG (INFO) << "cachePrepareData : " << "request will not be handled. Finalization is in progress" << "\n";
		return status::StatusInternal::FINALIZATION_IN_PROGRESS;
	}

    // subscribe the request to come back to cache manager when it will be finished for further management
    DataSetRequestCompletionFunctor functor = boost::bind(boost::mem_fn(&CacheManager::finalizeUserRequest), this, _1, _2, _3, _4, _5);

    // create the task "Cache Prepare Task"
    boost::shared_ptr<MonitorRequest> request(new request::PrepareDatasetTask(callback, functor, functor, session, namenode, m_syncModule, &m_longpool, files));

    // assign the request identity
    requestIdentity.ctx = session;
    requestIdentity.timestamp = request->timestampstr();

    {
    	boost::lock_guard<boost::mutex> lock(m_lowrequestsMux);
    	// send task to the queue for further processing:
    	auto it = m_activeLowRequests.push_back(request);
    	// if there was a problem to insert the request, reply it
    	if(!it.second){
    		LOG (WARNING) << "Unable to schedule prepare request for processing." << "\n";
    		std::cout << "Unable to schedule prepare request for processing.\n";
    		return status::StatusInternal::OPERATION_ASYNC_REJECTED;
    	}
    	// notify new data arrival
    	m_controlLowRequestsArrival.notify_all();
    }

    // if async, go out immediately:
    return status::StatusInternal::OPERATION_ASYNC_SCHEDULED;
}

status::StatusInternal CacheManager::cacheCancelPrepareData(const requestIdentity & requestIdentity){

	if(m_shutdownFlag){
		LOG (INFO) << "cacheCancelPrepareData : " << "request will not be handled. Finalization is in progress" << "\n";
		return status::StatusInternal::FINALIZATION_IN_PROGRESS;
	}

	boost::unique_lock<boost::mutex> lock(m_lowrequestsMux);

	// get the request from the active requests:
	RequestsBySessionAndTimestampTag& requests = m_activeLowRequests.get<session_timestamp_tag>();
	auto it = requests.find(boost::make_tuple(requestIdentity.timestamp, requestIdentity.ctx));

	// if nothing found, report the warning about missed request to cancel
	if(it == requests.end()){
		// nothing to do, just log the BUG
		LOG (WARNING) << "Unable to locate request to cancel it, no actions will be taken. Request timestamp : " << requestIdentity.timestamp << "\n";
		return status::StatusInternal::REQUEST_IS_NOT_FOUND;
	}

	// if there's request found, send the cancellation request to it
	boost::shared_ptr<MonitorRequest> request = (*it);
	lock.unlock();

	// cancel the task async
	taskOverallStatus status = request->cancel(true);
	LOG (INFO) << "Request was cancelled. Cancelation status :\"" << status << ";  Request timestamp : " << requestIdentity.timestamp << "; Request status : " << request->status() << "\n";

	return status::StatusInternal::OK;
}

status::StatusInternal CacheManager::cacheCheckPrepareStatus(const requestIdentity & requestIdentity,
		std::list<boost::shared_ptr<FileProgress> > & progress, request_performance& performance){

	if(m_shutdownFlag){
		LOG (INFO) << "cacheCheckPrepareStatus : " << "request will not be handled. Finalization is progress" << "\n";
		return status::StatusInternal::FINALIZATION_IN_PROGRESS;
	}
	LOG (INFO) << "Check Prepare status for Request timestamp : " << requestIdentity.timestamp << "\n";
	boost::unique_lock<boost::mutex> lock(m_lowrequestsMux);

	// get the request from the active requests:
	RequestsBySessionAndTimestampTag& requests = m_activeLowRequests.get<session_timestamp_tag>();
	auto it = requests.find(boost::make_tuple(requestIdentity.timestamp, requestIdentity.ctx));

	// if nothing found in "active requests", log this and try to find in "history"
	if(it != requests.end()){
		// if there's request found, query it for progress and report back to client
		boost::shared_ptr<MonitorRequest> request = (*it);
		lock.unlock();

		progress = request->progress();
		performance = request->performance();

		LOG (INFO) << "Request is found among \"Active\". Request timestamp : " << requestIdentity.timestamp << "; Request status : " << request->status() << "\n";
	    return status::StatusInternal::OK;
	}
	lock.unlock();

	boost::unique_lock<boost::mutex> hilock(m_HistoryMux);

	// get the request from the history requests:
	HistoricalRequestsBySessionAndTimestampTag& historical = m_HistoryRequests.get<session_timestamp_tag>();
	auto hit = historical.find(boost::make_tuple(requestIdentity.timestamp, requestIdentity.ctx));

	// if nothing found in "active requests", log this and try to find in "history"
	if(hit != historical.end()){
		// if there's request found, query it for progress and report back to client
		boost::shared_ptr<HistoricalCacheRequest> request = (*hit);
		hilock.unlock();

		progress = request->progress;
		performance = request->performance;

		LOG (INFO) << "Request is found in \"History\". Request timestamp : " << requestIdentity.timestamp << "; Request status : " << request->status << "\n";
	    return status::StatusInternal::OK;
	}
	hilock.unlock();

	LOG (WARNING) << "Request is not found! Request timestamp : " << requestIdentity.timestamp << "\n";
	// TODO : check for requested request timestamp. If it less than auto-cleanup treshold, report the error in logging level.
	return status::StatusInternal::REQUEST_IS_NOT_FOUND;
}

bool CacheManager::getFile(const NameNodeDescriptor & namenode, const char* path, ManagedFile::File*& file){
	return m_registry->getFileByPath(path, file);
}

} /** namespace impala */


