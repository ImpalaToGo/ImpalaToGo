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
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/random_access_index.hpp>
#include <boost/multi_index/hashed_index.hpp>

#include "dfs_cache/task.hpp"
#include "dfs_cache/managed-file.hpp"

/**
 * @namespace impala
 */
namespace impala {

/**
 * Type for Registry of managed files
 */
typedef boost::intrusive::set<ManagedFile::File>  FileRegistry;

/**
 * Defines the tag representing session within Prepare Request
 */
struct session_tag {};

/**
 * Type for Pool of Context Bound Requests. We want this to work fast and contain requests in order as they were received.
 */
typedef boost::multi_index::multi_index_container<
		request::SessionBoundTask*,
		boost::multi_index::indexed_by<
    		boost::multi_index::random_access<>, // this index represents insertion order
    		boost::multi_index::hashed_unique< boost::multi_index::tag<session_tag>,
    			boost::multi_index::member<request::SessionBoundTask, SessionContext, &request::SessionBoundTask::m_session> >
    >
> ClientRequests;

struct FileProgress;

/**
 * The callback to the context where the Estimate / Prepare Operation completion report is expected (cache manager).
 * @param context  - session context (client context) which requested for prepare operation
 * @param progress - file progress for file scheduled for prepare
 *
 * @return operation status
 */
typedef boost::function<status::StatusInternal (const FileProgress & progress)> SingleFileProgressCompletedCallback;

typedef boost::function<status::StatusInternal (const NameNodeDescriptor & namenode, const char* filepath,
		request::MakeProgressTask<FileProgress>* const & task)> SingleFileMakeProgressFunctor;

typedef boost::function<status::StatusInternal (SessionContext session, const NameNodeDescriptor & namenode, std::list<const char*>& files)> PrepareDatasetFunctor;
typedef boost::function<status::StatusInternal (SessionContext session, const NameNodeDescriptor & namenode, std::list<const char*>& files)> EstimateDatasetFunctor;

typedef boost::function<status::StatusInternal (bool async, request::CancellableTask* const & cancellable)> CancellationFunctor;

}

#endif /* CACHE_DEFINITIONS_H_ */
