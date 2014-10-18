/*
 * @file sync-module.cpp
 * @brief implementation of sync module
 *
 *  Created on: Oct 3, 2014
 *      Author: elenav
 */

/**
 * @namespace impala
 */

#include "dfs_cache/sync-module.hpp"

namespace impala {

status::StatusInternal Sync::estimateTimeToGetFileLocally(const NameNodeDescriptor & namenode, const char* path,
		request::MakeProgressTask<FileProgress>* const & task){

    // set the progress directly to the task
	return status::StatusInternal::OK;
}

status::StatusInternal Sync::prepareFile(const NameNodeDescriptor & namenode, const char* path, request::MakeProgressTask<FileProgress>* const & task){
	// Get the Namenode adaptor from the registry for requested namenode:
	boost::shared_ptr<NameNodeDescriptorBound> namenodeAdaptor = (*m_registry->getNamenode(namenode));
    if(namenodeAdaptor == nullptr){
    	// no namenode adaptor configured, go out
    	return status::StatusInternal::NAMENODE_IS_NOT_CONFIGURED;
    }

    boost::shared_ptr<dfsConnection> connection = (*namenodeAdaptor->getFreeConnection());
    boost::shared_ptr<RemoteAdaptor> adaptor    = namenodeAdaptor->adaptor();

    // get the file progress reference:
    boost::shared_ptr<FileProgress> fp = task->progress();
    adaptor->read(connection);


    // Pure academic part.
    //
	// Suppose download in progress.
	int bytes_read = 0;
	// while no cancellation and we still something to read, proceed.
	boost::mutex* mux;
	boost::condition_variable_any* conditionvar;
	task->mux(mux);
	task->conditionvar(conditionvar);

	while(!task->condition() && bytes_read != 0){
		boost::mutex::scoped_lock lock(*mux);
		// do read a block
		bytes_read = 10;
		lock.unlock();
	}
	// check cancellation condition to know we had the cancellation. If so, notify the caller (no matter if it waits for this or no):
	conditionvar->notify_all();

	return status::StatusInternal::OK;
}

status::StatusInternal Sync::cancelFileMakeProgress(bool async, request::CancellableTask* const & task){
	boost::mutex* mux;
	boost::condition_variable_any* conditionvar;
	task->mux(mux);
	task->conditionvar(conditionvar);

	boost::mutex::scoped_lock lock(*mux);
	// set the cancellation flag
	bool* condition;
	task->condition(condition);
    *condition = true;

	if(!async){
		conditionvar->wait(*mux);
		// update the history!1
	} // else return immediately
	return status::StatusInternal::OK;
}

status::StatusInternal Sync::validateLocalCache(bool& valid){
	return status::StatusInternal::NOT_IMPLEMENTED;
}
}


