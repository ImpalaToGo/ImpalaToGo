/*
 * async-execution-layer.hpp
 *
 *  Created on: Oct 14, 2014
 *      Author: elenav
 */

#ifndef ASYNC_EXECUTION_LAYER_HPP_
#define ASYNC_EXECUTION_LAYER_HPP_

#include <boost/thread/mutex.hpp>
#include "dfs_cache/common-include.hpp"
#include "dfs_cache/tasks-impl.hpp"
#include "dfs_cache/cache-work-pool.hpp"

/** @namespace impala */
namespace impala{

/** Represents the layer responsible for asynchronous tasks */
class RequestsSchedulingLayer{
private:
	boost::mutex            m_pendingMux;         /**< sync for pending requests queue */
	boost::mutex            m_HistoryMux;         /**< sync for history of requests set */

	cache::ThreadPool<12>   m_threadPool;         /**< thread pool */

public:
	/** add request for execution */
    void addRequest(SessionContext context, const request::RunnableTask& task, bool async = true){

    	if(async){
    		boost::mutex::scoped_lock(m_mux);
    		m_threadPool.add_task(task);
    	}
    	else // run the task directly. Add the reference to it into the history.
    		task();
    }
    /** Cancel the request issued for specified client context *
     * @param[in] context - request's client context
     */
    void cancelRequest(SessionContext context);

};
}



#endif /* ASYNC_EXECUTION_LAYER_HPP_ */
