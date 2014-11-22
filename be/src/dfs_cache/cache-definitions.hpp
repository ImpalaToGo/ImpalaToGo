/** @file cache-definitions.h
 *  @brief Impala Cache layer API.
 *
 *  This module inherits the facilities of libhdfs (regarding file system operations, the only difference is that all file system operations
 *  are running locally now, not on remote DFS like with libhdfs) and accompanies them with with cache facilities (for details, see further)
 *
 *  Facilities:
 *  1. Publish the cache management APIs (prefixed with "cache").
 *  These APIs serve to give the ability to schedule caching operations and subscribe / poll for their completion and status.
 *  Underlying access the requested DFS is implemented via configured DFS adaptors (Plugins).
 *
 *  2. Publish FileSystem API to work with files (prefixed with "dfs").
 *  TODO: prefix dfs is inherited from similar "hdfs" - from libhdfs.
 *  This prefix should be changed to "fs" to show that the requested API is running locally.
 *
 *  @date   Sep 29, 2014
 *  @author elenav
 */

#ifndef CACHE_DEFINITIONS_H_
#define CACHE_DEFINITIONS_H_

#include <boost/thread.hpp>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/composite_key.hpp>
#include <boost/interprocess/smart_ptr/unique_ptr.hpp>
#include <boost/tuple/tuple.hpp>

#include <list>

#include "common/logging.h"
#include "dfs_cache/task.hpp"
#include "dfs_cache/managed-file.hpp"
#include "dfs_cache/filesystem-lru-cache.hpp"

#if !defined(NDEBUG)
#define BOOST_MULTI_INDEX_ENABLE_INVARIANT_CHECKING
#define BOOST_MULTI_INDEX_ENABLE_SAFE_MODE
#endif

/**
 * @namespace impala
 */
namespace impala {

/** Type for Registry of managed files */
typedef FileSystemLRUCache FileRegistry;

/** Represent MonitorRequest, the Client Request to be tracked by client for progress */
typedef request::SessionBoundTask<std::list<boost::shared_ptr<FileProgress> > > MonitorRequest;

/** Equal operator to run equality comparison on @a MonitorRequest entity */
extern bool operator==(MonitorRequest const & request1, MonitorRequest const & request2);

/** hash function defined for @a MonitorRequest type */
extern std::size_t hash_value(MonitorRequest const& request);

/** Defines the index tag to represent composite "session-timestamp" nature of Client Request */
struct session_timestamp_tag {};
/**
 * Type for Pool of Active Async (Pending and in Progress) and Pool of Active Sync requests.
 * Have semantics of std::list while serves as a queue of requests.
 * We want this to work fast from "find" perspective ( O(1)) - as client's sync call will cover only "find";
 * and contain requests in order as they were received, so FIFO
 */
typedef boost::multi_index::multi_index_container<
		boost::shared_ptr<MonitorRequest>,
		boost::multi_index::indexed_by<
		    boost::multi_index::sequenced<>,
    		boost::multi_index::hashed_unique<
    			boost::multi_index::tag<session_timestamp_tag>,
    			boost::multi_index::composite_key<
    			    MonitorRequest,
    			    boost::multi_index::const_mem_fun<request::Task,
    			        					std::string, &request::Task::timestampstr>,
    				boost::multi_index::const_mem_fun<MonitorRequest, SessionContext, &MonitorRequest::session>
    				 > >
    >
> ClientRequests;

typedef ClientRequests::index<session_timestamp_tag>::type RequestsBySessionAndTimestampTag;

/** represents historical request available for statistics and status query.
 * Contains the summary depicting the original request */
template<typename Progress_>
struct HistoricalRequest{
	requestIdentity     identity;    /**< request identity */
	Progress_           progress;    /**< request progress */
    request_performance performance; /**< request overall performance */
    taskOverallStatus   status ;     /**< request overall status */
    bool                canceled;    /**< flag, indicates whether request was canceled */
    bool                succeed;     /**< flag indicates success completion for those who does not need details of status */

    std::string    timestamp() const { return identity.timestamp; }
    SessionContext ctx() const { return identity.ctx; }
};

typedef HistoricalRequest<std::list<boost::shared_ptr<FileProgress> > > HistoricalCacheRequest;

/** Equal operator to run equality comparison on @a HistoricalCacheRequest entity */
extern bool operator==(HistoricalCacheRequest const & request1, HistoricalCacheRequest const & request2);

/** hash function defined for @a HistoricalCacheRequest type */
extern std::size_t hash_value(HistoricalCacheRequest const& request);

/**
 * Type for Pool of Historical requests (currently, Prepare requests only are of interest).
 * Have semantics of std::list. New request reaches the top of the history as most recent
 * We want this to work fast from "find" perspective ( O(1)) - as client's sync call will cover only "find";
 * and contain requests in the order opposite as they were received, so, LIFO.
 */
typedef boost::multi_index::multi_index_container<
		boost::shared_ptr<HistoricalCacheRequest>,
		boost::multi_index::indexed_by<
		    boost::multi_index::sequenced<>,
    		boost::multi_index::hashed_unique<
    			boost::multi_index::tag<session_timestamp_tag>,
    			boost::multi_index::composite_key<
    			HistoricalCacheRequest,
    			boost::multi_index::const_mem_fun<HistoricalCacheRequest, std::string, &HistoricalCacheRequest::timestamp>,
    			boost::multi_index::const_mem_fun<HistoricalCacheRequest, SessionContext, &HistoricalCacheRequest::ctx>
    				 > >
    >
> HistoryOfRequests;

typedef HistoryOfRequests::index<session_timestamp_tag>::type HistoricalRequestsBySessionAndTimestampTag;

struct FileProgress;

/**
 * The callback to the context where the Estimate / Prepare Operation completion report is expected (cache manager).
 * @param context  - session context (client context) which requested for prepare operation
 * @param progress - file progress for file scheduled for prepare
 *
 * @return operation status
 */
typedef boost::function<void (const boost::shared_ptr<FileProgress>& progress)> SingleFileProgressCompletedCallback;

typedef boost::function<status::StatusInternal (const FileSystemDescriptor & namenode, const char* filepath,
		request::MakeProgressTask<boost::shared_ptr<FileProgress> >* const & task)> SingleFileMakeProgressFunctor;

/** Functor to run on manager when the request is completed */
typedef boost::function<void (const requestIdentity& requestIdentity, const FileSystemDescriptor & namenode,
		requestPriority priority, bool canceled, bool async)> DataSetRequestCompletionFunctor;

typedef boost::function<status::StatusInternal (bool async, request::CancellableTask* const & cancellable)> CancellationFunctor;

}

#endif /* CACHE_DEFINITIONS_H_ */
