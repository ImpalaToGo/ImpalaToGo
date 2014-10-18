/** @file cache-tasks.h
 *  @brief Contains definitions of tasks managed by Cache layer.
 *  Additional tasks should be declared here.
 *
 *  @date   Sep 29, 2014
 *  @author elenav
 */

#ifndef TASKS_IMPL_H_
#define TASKS_IMPL_H_

#include "dfs_cache/cache-definitions.hpp"
#include "dfs_cache/task.hpp"
#include "dfs_cache/sync-module.hpp"

/**
 * @namespace impala
 */
namespace impala {

/**
 * @namespace request
 */
namespace request{

typedef RunnableTask<SingleFileProgressCompletedCallback, SingleFileMakeProgressFunctor, CancellationFunctor, FileProgress> FileProgressTaskType;
typedef ContextBoundTask<PrepareCompletedCallback, PrepareDatasetFunctor, CancellationFunctor, std::list<FileProgress> > ContextBoundPrepareTaskType;
typedef ContextBoundTask<CacheEstimationCompletedCallback, EstimateDatasetFunctor, CancellationFunctor, std::list<FileProgress> > ContextBoundEstimateTaskType;

/**
 * File download request for Sync module.
 * This task is a part of a compound @PrepareDatasetTask and therefore it does not require the client context
 */
class FileDownloadTask : public FileProgressTaskType{
protected:
	~FileDownloadTask() = default;
	taskOverallStatus run_internal();
public:
	/**
	 * Ctor. Construct the "Single file - get locally" request
	 *
	 * @param callback     - request completion callback
	 * @param functor      - execution routine
	 * @param cancellation - cancellation routine
	 * @param dfsCluster   - dfs cluster
	 * @param file         - file path within the cluster
	 */
	FileDownloadTask(SingleFileProgressCompletedCallback callback, SingleFileMakeProgressFunctor functor, CancellationFunctor cancellation,
			const NameNodeDescriptor & namenode, const char* path)
		try : FileProgressTaskType(callback, functor, cancellation){
             m_progress->namenode = namenode;
             m_progress->dfsPath = path;
		}
		catch(...)
		{}

		/**
		 * Cancel the file download operation.
		 * Sync or async way.
		 * @param async      - flag, indicates whether request cancellation should be run asynchronously.
		 * Async way means the cancellation request is sent to the handler, and no confirmation is required so far.
		 * Sync way assumes the calling thread will be synchronously wait while the file download will be interrupted and
		 * all post-actions will be completed.
		 *
		 * @return overall request status.
		 */
		taskOverallStatus cancel(bool async = false);

		/**
		 * Call back into the owner task (PrepareDatasetTask) to inform about the completion
		 */
		void callback();
};

/**
 * File estimate request for Sync module.
 * This task is a part of a compound @EstimateDatasetTask and therefore it does not require the client context
 */
class FileEstimateTask : public FileProgressTaskType{
protected:
	~FileEstimateTask() = default;
	taskOverallStatus run_internal();
public:
	/**
	 * Ctor. Construct the "Single file - get locally" request
	 *
	 * @param callback     - request completion callback
	 * @param functor      - execution routine
	 * @param cancellation - cancellation routine
	 * @param dfsCluster   - dfs cluster
	 * @param file         - file path within the cluster
	 */
	FileEstimateTask(SingleFileProgressCompletedCallback callback, SingleFileMakeProgressFunctor functor, CancellationFunctor cancellation,
			const NameNodeDescriptor & namenode, const char* path)
		try : FileProgressTaskType(callback, functor, cancellation){

             m_progress->namenode = namenode;
             m_progress->dfsPath = path;
		}
		catch(...)
		{}

		/**
		 * Cancel the file download operation.
		 * Sync or async way.
		 * @param async      - flag, indicates whether request cancellation should be run asynchronously.
		 * Async way means the cancellation request is sent to the handler, and no confirmation is required so far.
		 * Sync way assumes the calling thread will be synchronously wait while the file download will be interrupted and
		 * all post-actions will be completed.
		 *
		 * @return overall request status.
		 */
		taskOverallStatus cancel(bool async = false);

		/**
		 * Call back into the owner task (PrepareDatasetTask) to inform about the completion
		 */
		void callback();

};

/**
 * Describe Preapre Dataset Request, the calling context and bound files set along with statuses.
 * Why we do not want c++11's futures here instead of FileDownloadTasks:
 * we have no working thread per user request, as we have no much situations when parallel requests will be handled by module.
 * Thus, requests are enqueued into the queue of processing, and each request consists of "download file" subrequests,
 * that really should be run in parallel.
 * in order to have asynchronous bound client's (described by @a m_session) invocation, when all tasks are
 * finished within the PrepareDatasetTask, we need to call the client in the async way and update it that we completed the request.
 * This is impossible to do on the separate thread of thread pool.
 */
class PrepareDatasetTask : public ContextBoundPrepareTaskType{
private:
	std::list<const char*>   m_files;            /**< requested dataset */
	NameNodeDescriptor       m_namenode;         /**< namenode descriptor */

	Sync*                    m_syncModule;       /** reference to Sync module */

    std::list<FileDownloadTask*>  m_boundrequests;      /**< list of bound file requests */
    int                           m_remainedFiles;      /**< non-processed yet files */

    // Disable copy of our task and its assignment
    PrepareDatasetTask(PrepareDatasetTask const & task) = delete;
    PrepareDatasetTask& operator=(PrepareDatasetTask const & task) = delete;

protected:
    taskOverallStatus run_internal();

public:
	PrepareDatasetTask(PrepareCompletedCallback callback, PrepareDatasetFunctor functor, CancellationFunctor cancelation, SessionContext session,
			const NameNodeDescriptor& namenode, Sync* sync, std::list<const char*> files)
        try : ContextBoundPrepareTaskType(callback, functor, cancelation, session),
        m_files(files), m_namenode(namenode), m_syncModule(sync), m_remainedFiles(0){

		}
		catch(...)
		{}

	~PrepareDatasetTask() = default;

	/** report the current progress on demand */
	std::list<FileProgress> progress();

	/**
	 * Handler for "single file is ready" completion event expected from @a FileDownloadRequest.
		  * Mark the specified file accordingly in the client request associated with @a session
		  * context.
		  * If all files are done for request, should invoke the final callback to a client with overall
		  * "prepare" status report.
		  *
		  * @param progress - single file progress
		  */
		status::StatusInternal reportSingleFileIsCompletedCallback(FileProgress const & progress);

		 /**
		  *  Cancel the request's sub requests and once done, move the request along with subrequests statuses to the history.
		  *  TODO : CREATE HISTORY FOR PREPARE DATASET REQUESTS
		  * Async way means the request should be queried for status later.
		  *
		  * @param async      - flag, indicates whether request cancellation should be run asynchronously
		  *
		  * @return overall request status.
		  */
		 taskOverallStatus cancel(bool async = false);

		 /**
		  * The callback to the client identified by SessionContext
		  */
		 void callback();
};

/**
 * Describe Estimate Dataset Request, the calling context and bound files set along with statuses.
 * Why we do not want c++11's futures here instead of FileEstimateTasks:
 * we have no working thread per user request, as we have no much situations when parallel requests will be handled by module.
 * Thus, requests are enqueued into the queue of processing, and each request consists of "estimate" subrequests,
 * that really should be run in parallel.
 * in order to have asynchronous bound client's (described by @a m_session) invocation, when all tasks are
 * finished within the PrepareDatasetTask, we need to call the client in the async way and update it that we completed the request.
 * This is impossible to do on the separate thread of thread pool.
 */
class EstimateDatasetTask : public ContextBoundEstimateTaskType{
private:
	std::list<const char*>   m_files;            /**< requested dataset */
	NameNodeDescriptor       m_namenode;         /**< namenode descriptor */

	Sync*                    m_syncModule;       /** reference to Sync module */

    std::list<FileEstimateTask*>  m_boundrequests;      /**< list of bound file requests */
    int                           m_remainedFiles;      /**< non-processed yet files */

protected:
    taskOverallStatus run_internal();

public:
	EstimateDatasetTask(CacheEstimationCompletedCallback callback, EstimateDatasetFunctor functor, CancellationFunctor cancelation, SessionContext session,
			const NameNodeDescriptor& namenode, Sync* sync, std::list<const char*> files)
        try : ContextBoundEstimateTaskType(callback, functor, cancelation, session),
        m_files(files), m_namenode(namenode), m_syncModule(sync), m_remainedFiles(0){

		}
		catch(...)
		{}

	~EstimateDatasetTask() = default;

	/** report the current progress on demand */
	std::list<FileProgress> progress();

	/**
	 * Handler for "single file is estimated" completion event expected from @a FileEstimateRequest.
	 * Mark the specified file accordingly in the client request associated with @a session
	 * context.
	 * If all files are done for request, should invoke the final callback to a client with overall
	 * "estimate" status report.
	 * @param progress - single file progress
	 */
	status::StatusInternal reportSingleFileIsCompletedCallback(FileProgress const & progress);

	/**
	 * Cancel the request's sub requests and once done, move the request along with subrequests statuses to the history.
	 * TODO : CREATE HISTORY FOR ESTIMATE DATASET REQUESTS
	 * Async way means the request should be queried for status later.
	 * @param async      - flag, indicates whether request cancellation should be run asynchronously
	 *
	 * @return overall request status.
	 */
	taskOverallStatus cancel(bool async = false);

	/**
	 * The callback to the client identified by SessionContext
	 */
	void callback();
};

} // request
} // impala

#endif /* PREPARE_REQUEST_H_ */
