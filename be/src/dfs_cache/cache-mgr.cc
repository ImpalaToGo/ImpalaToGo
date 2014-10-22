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

#include "cache-mgr.hpp"

namespace impala {

#define MAX_PATH 256
#define PORT_LEN 5

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

boost::scoped_ptr<CacheManager> CacheManager::instance_;

void CacheManager::init() {
  if(CacheManager::instance_.get() == NULL)
	  CacheManager::instance_.reset(new CacheManager());
}


status::StatusInternal CacheManager::shutdown(bool force, bool updateClients){

	return status::StatusInternal::NOT_IMPLEMENTED;
}


void CacheManager::finalizeUserRequest(const requestIdentity& requestIdentity, const NameNodeDescriptor & namenode,
		bool cancelled ){
	boost::unique_lock<boost::mutex> lock(m_requestsMux);

    // get the request from the active requests:
	RequestsBySessionAndTimestampTag& requests = m_activeRequests.get<session_timestamp_tag>();
	auto it = requests.find(boost::make_tuple(requestIdentity.ctx, requestIdentity.timestamp));

	// if anything found, remove it from the "active" requests and place it to "history":
    if(it == requests.end()){
    	// nothing to do, just log the BUG
    	LOG (ERROR) << "Unable to locate request to finalize it. Request timestamp : " << requestIdentity.timestamp << "\n";
    	return;
    }

    // if there's request found, get it before it will be removed from active requests.
    boost::shared_ptr<MonitorRequest> request = (*it);

    // and remove it from active requests:
    m_activeRequests.remove_if([&](boost::shared_ptr<MonitorRequest> request) { return request->session() == requestIdentity.ctx &&
        		request->timestampstr() == requestIdentity.timestamp; });
    lock.unlock();

    boost::unique_lock<boost::mutex> lock1(m_HistoryMux);
    // now add it to History, add it first as most recent:
    m_HistoryRequests.push_front(request);

    LOG (INFO) << "Request was moved to history. Status : " << request->status() << "; Request timestamp : " << requestIdentity.timestamp << "\n";
}

status::StatusInternal CacheManager::cacheEstimate(SessionContext session, const NameNodeDescriptor & namenode, const DataSet& files, time_t& time,
		   CacheEstimationCompletedCallback callback, requestIdentity & requestIdentity, bool async) {

    // subscribe the request to come back to cache manager when it will be finished for further management
    DataSetRequestCompletionFunctor functor = boost::bind(boost::mem_fn(&CacheManager::finalizeUserRequest), this, _1, _2, _3);

    // create the task "CacheEstimationTask"
    boost::shared_ptr<MonitorRequest> request(new request::EstimateDatasetTask(callback, functor, functor, session, namenode, m_syncModule, files));

    // assign the request identity
    requestIdentity.ctx = session;
    requestIdentity.timestamp = request->timestampstr();

    if(async){
    	boost::mutex::scoped_lock lock(m_requestsMux);
    	// send task to the queue for further processing:
    	m_activeRequests.push_back(request);

    	// if async, go out immediately:
		return status::StatusInternal::OPERATION_ASYNC_SCHEDULED;
    }

	// in a sync way, need to wait for task completion.
	return status::StatusInternal::NOT_IMPLEMENTED;
}

status::StatusInternal CacheManager::cachePrepareData(SessionContext session, const NameNodeDescriptor & namenode, const DataSet& files,
     		  PrepareCompletedCallback callback, requestIdentity & requestIdentity){
    // subscribe the request to come back to cache manager when it will be finished for further management
    DataSetRequestCompletionFunctor functor = boost::bind(boost::mem_fn(&CacheManager::finalizeUserRequest), this, _1, _2, _3);

    // create the task "Cache Prepare Task"
    boost::shared_ptr<MonitorRequest> request(new request::PrepareDatasetTask(callback, functor, functor, session, namenode, m_syncModule, files));

    // assign the request identity
    requestIdentity.ctx = session;
    requestIdentity.timestamp = request->timestampstr();


    boost::mutex::scoped_lock lock(m_requestsMux);
    // send task to the queue for further processing:
    m_activeRequests.push_back(request);

    // if async, go out immediately:
    return status::StatusInternal::OPERATION_ASYNC_SCHEDULED;
}

status::StatusInternal CacheManager::cacheCancelPrepareData(const requestIdentity & requestIdentity){
	boost::unique_lock<boost::mutex> lock(m_requestsMux);

	// get the request from the active requests:
	RequestsBySessionAndTimestampTag& requests = m_activeRequests.get<session_timestamp_tag>();
	auto it = requests.find(boost::make_tuple(requestIdentity.ctx, requestIdentity.timestamp));

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
	LOG (INFO) << "Check Prepare status for Request timestamp : " << requestIdentity.timestamp << "\n";
	boost::unique_lock<boost::mutex> lock(m_requestsMux);

	// get the request from the active requests:
	RequestsBySessionAndTimestampTag& requests = m_activeRequests.get<session_timestamp_tag>();
	auto it = requests.find(boost::make_tuple(requestIdentity.ctx, requestIdentity.timestamp));

	// if nothing found in "active requests", log this and try to find in "history"
	if(it != requests.end()){
		// if there's request found, query it for progress and report back to client
		boost::shared_ptr<MonitorRequest> request = (*it);
		lock.unlock();

		progress = request->progress();
		LOG (INFO) << "Request is found among \"Active\". Request timestamp : " << requestIdentity.timestamp << "; Request status : " << request->status() << "\n";
	    return status::StatusInternal::OK;
	}
	lock.unlock();

	boost::unique_lock<boost::mutex> hilock(m_HistoryMux);

	// get the request from the history requests:
	requests = m_HistoryRequests.get<session_timestamp_tag>();
	auto hit = requests.find(boost::make_tuple(requestIdentity.ctx, requestIdentity.timestamp));

	// if nothing found in "active requests", log this and try to find in "history"
	if(hit != requests.end()){
		// if there's request found, query it for progress and report back to client
		boost::shared_ptr<MonitorRequest> request = (*it);
		hilock.unlock();

		progress = request->progress();
		LOG (INFO) << "Request is found in \"History\". Request timestamp : " << requestIdentity.timestamp << "; Request status : " << request->status() << "\n";
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


