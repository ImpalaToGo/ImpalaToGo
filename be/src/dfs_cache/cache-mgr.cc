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

boost::scoped_ptr<CacheManager> CacheManager::instance_;

void CacheManager::init() {
  if(CacheManager::instance_.get() == NULL)
	  CacheManager::instance_.reset(new CacheManager());
}

status::StatusInternal CacheManager::shutdown(bool force, bool updateClients){

	return status::StatusInternal::NOT_IMPLEMENTED;
}

status::StatusInternal CacheManager::cacheEstimate(SessionContext session, const NameNodeDescriptor & namenode, const std::list<const char*>& files, time_t& time,
		   CacheEstimationCompletedCallback callback, bool async) {
	// create the task "CacheEstimationTask" and enqueue it for processing.
    boost::mutex::scoped_lock lock(m_requestsMux);

    //m_clientRequests.push_back(new request::EstimateDatasetTask(callback, functor, ))
	// if async, go out immediately:
	if(async)
		return status::StatusInternal::NOT_IMPLEMENTED;
	// in a sync way, need to wait for task completion.

	return status::StatusInternal::NOT_IMPLEMENTED;
}

status::StatusInternal CacheManager::cachePrepareData(SessionContext session, const NameNodeDescriptor & namenode, const std::list<const char*>& files,
     		  PrepareCompletedCallback callback){
	return status::StatusInternal::NOT_IMPLEMENTED;
}

status::StatusInternal CacheManager::cacheCancelPrepareData(SessionContext session){
	return status::StatusInternal::NOT_IMPLEMENTED;
}

status::StatusInternal CacheManager::cacheCheckPrepareStatus(SessionContext session, std::list<FileProgress*>& progress, request_performance& performance){
	return status::StatusInternal::NOT_IMPLEMENTED;
}

status::StatusInternal CacheManager::freeFileProgressList(std::list<FileProgress*>& progress){
	return status::StatusInternal::NOT_IMPLEMENTED;
}

bool CacheManager::getFile(const NameNodeDescriptor & namenode, const char* path, ManagedFile::File*& file){
	return m_registry->getFileByPath(path, file);
}

} /** namespace impala */


