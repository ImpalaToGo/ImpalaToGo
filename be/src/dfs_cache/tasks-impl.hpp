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

/** Task is able to make a progress on a single file */
typedef RunnableTask<SingleFileProgressCompletedCallback, SingleFileMakeProgressFunctor, CancellationFunctor,
		boost::shared_ptr<FileProgress> > FileProgressTaskType;

/** Prepare Dataset compound task, owns the client's context */
typedef ContextBoundTask<PrepareCompletedCallback, DataSetRequestCompletionFunctor, DataSetRequestCompletionFunctor,
		std::list<boost::shared_ptr<FileProgress> > > ContextBoundPrepareTaskType;

/** Estimate Dataset compound task, owns the client's context */
typedef ContextBoundTask<CacheEstimationCompletedCallback, DataSetRequestCompletionFunctor, DataSetRequestCompletionFunctor,
		std::list<boost::shared_ptr<FileProgress> > > ContextBoundEstimateTaskType;

/**
 * File download request for Sync module.
 * This task is a part of a compound @PrepareDatasetTask and therefore it does not require the client context
 */
class FileDownloadTask : public FileProgressTaskType{
protected:

	/** "do work" predicate */
	void run_internal();

	/** Call back into the owner task (PrepareDatasetTask) to inform about the completion */
	void callback();

	/** "finalize" predicate */
	void finalize();

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
			this->m_progress.reset(new FileProgress());
			this->m_progress->namenode = namenode;
			this->m_progress->dfsPath = path;
		}
		catch(...)
		{}


	~FileDownloadTask() = default;

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

};

/**
 * File estimate request for Sync module.
 * This task is a part of a compound @EstimateDatasetTask and therefore it does not require the client context
 */
class FileEstimateTask : public FileProgressTaskType{
protected:

	/** "do work" predicate */
	void run_internal();

	/** Call back into the owner task (PrepareDatasetTask) to inform about the completion */
	void callback();

	/** "finalize" predicate */
	void finalize();
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
             this->m_progress.reset(new FileProgress());
             this->m_progress->namenode = namenode;
             this->m_progress->dfsPath = path;
		}
		catch(...)
		{}


	~FileEstimateTask() = default;

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
	DataSet                  m_files;            /**< requested dataset */
	NameNodeDescriptor       m_namenode;         /**< namenode descriptor */

	boost::shared_ptr<Sync>       m_syncModule;       /** reference to Sync module */
	int                           m_remainedFiles;    /**< non-processed yet files */

    std::list<boost::shared_ptr<FileDownloadTask> >  m_boundrequests;      /**< list of bound file requests */

    // Disable copy of our task and its assignment
    PrepareDatasetTask(PrepareDatasetTask const & task) = delete;
    PrepareDatasetTask& operator=(PrepareDatasetTask const & task) = delete;

protected:
    /** "do work" predicate */
    void run_internal();

    /** The callback to the client identified by SessionContext */
    void callback();

    /** "finalize" predicate */
	void finalize();

public:
	PrepareDatasetTask(PrepareCompletedCallback callback, DataSetRequestCompletionFunctor functor, DataSetRequestCompletionFunctor cancelation, const SessionContext& session,
			const NameNodeDescriptor& namenode, boost::shared_ptr<Sync> sync, dfsThreadPool* pool, const DataSet& files, bool async = true)
        try : ContextBoundPrepareTaskType(callback, functor, cancelation, session, pool, async),
        			m_files(files), m_namenode(namenode){

				m_syncModule = sync;
				// note remained files to check - set size
				m_remainedFiles = files.size();
				this->m_priority = requestPriority::LOW;
		}
		catch(...)
		{}

	~PrepareDatasetTask() = default;

	/** report the current progress on demand */
	std::list<boost::shared_ptr<FileProgress> > progress();

	/**
	 * Handler for "single file is ready" completion event expected from @a FileDownloadRequest.
		  * Mark the specified file accordingly in the client request associated with @a session
		  * context.
		  * If all files are done for request, should invoke the final callback to a client with overall
		  * "prepare" status report.
		  */
	void reportSingleFileIsCompletedCallback(const boost::shared_ptr<FileProgress>& progress);

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
	DataSet                  m_files;            /**< requested dataset */
	NameNodeDescriptor       m_namenode;         /**< namenode descriptor */

	boost::shared_ptr<Sync>  m_syncModule;       /** reference to Sync module */
	int                      m_remainedFiles;    /**< non-processed yet files */

    std::list<boost::shared_ptr<FileEstimateTask> >  m_boundrequests;      /**< list of bound file requests */

protected:

    /** "do work" predicate */
    void run_internal();

	/** The callback to the client identified by SessionContext */
	void callback();

    /** "finalize" predicate */
	void finalize();

public:
	EstimateDatasetTask(CacheEstimationCompletedCallback callback, DataSetRequestCompletionFunctor functor, DataSetRequestCompletionFunctor cancelation,
			const SessionContext& session, const NameNodeDescriptor& namenode, const boost::shared_ptr<Sync> & sync, dfsThreadPool* pool, const DataSet& files, bool async = true)
        try : ContextBoundEstimateTaskType(callback, functor, cancelation, session, pool, async),
        			m_files(files), m_namenode(namenode){

				m_syncModule = sync;
				// note remained files to check - set size
				m_remainedFiles = files.size();
				this->m_priority = requestPriority::HIGH;
		}
		catch(...)
		{}

	~EstimateDatasetTask() = default;

	/** report the current progress on demand */
	std::list<boost::shared_ptr<FileProgress> > progress();

	/**
	 * Handler for "single file is estimated" completion event expected from @a FileEstimateRequest.
	 * Mark the specified file accordingly in the client request associated with @a session
	 * context.
	 * If all files are done for request, should invoke the final callback to a client with overall
	 * "estimate" status report.
	 * @param progress - single file progress
	 */
	void reportSingleFileIsCompletedCallback(const boost::shared_ptr<FileProgress>& progress);

	/**
	 * Cancel the request's sub requests and once done, move the request along with subrequests statuses to the history.
	 * TODO : CREATE HISTORY FOR ESTIMATE DATASET REQUESTS
	 * Async way means the request should be queried for status later.
	 * @param async      - flag, indicates whether request cancellation should be run asynchronously
	 *
	 * @return overall request status.
	 */
	taskOverallStatus cancel(bool async = false);
};

} // request
} // impala

#endif /* PREPARE_REQUEST_H_ */
