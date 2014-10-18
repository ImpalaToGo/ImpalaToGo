/** @file cache-mgr.h
 *  @brief Definitions of cache entities managed and relevant for Cache layer.
 *
 *  @date   Sep 26, 2014
 *  @author elenav
 */

#ifndef CACHE_MGR_H_
#define CACHE_MGR_H_

#include <string>
#include <deque>
#include <utility>

#include <boost/intrusive/set.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include <boost/bind.hpp>

#include "util/hash-util.h"
#include "dfs_cache/cache-definitions.hpp"
#include "dfs_cache/tasks-impl.hpp"
#include "dfs_cache/sync-module.hpp"

/**
 * @namespace impala
 */
namespace impala {

/**
 * Represent Cache Manager.
 * - list of currently managed by Cache files along with their own states (mapped as cache persistence)
 * - list of currently handled by Cache Prepare Requests, therefore no concurrency to access it
 *
 * Cache Manager is the only who works with Cache metadata registry
 */
class CacheManager {
private:
	// Singleton instance. Instantiated in Init().
	static boost::scoped_ptr<CacheManager> instance_;

	boost::shared_ptr<CacheLayerRegistry>   m_registry;         /**< reference to metadata registry instance */
	ClientRequests                          m_clientRequests;   /**< Set of client requests currently managed by module. */
	Sync                                    m_syncModule;       /**< sync module reference. */

	boost::mutex                            m_requestsMux;      /**< mutex to protect client requests */

	/**
	 * Ctor. Subscribe to Sync's completion routines and pass the credentials mapping to Sync module.
	 */
    CacheManager(){
 	   //m_syncModule.init(config, boost::bind(&CacheManager::reportSingleFileIsCompletedCallback, this, _1));
    }

	CacheManager(CacheManager const& l);            // disable copy constructor
	CacheManager& operator=(CacheManager const& l); // disable assignment operator

public:

       ~CacheManager() {
    	   // unsubscribe from owners of the registry:
    	   m_registry.reset();
       }

       static CacheManager* instance() { return CacheManager::instance_.get(); }

       /** Initialize Cache Manager. Call this before any Cache Manager usage */
       static void init();

       /**
        * Subscribe to cache registry as one of owners.
        *
        * @param registry - cache registry
        */
       status::StatusInternal configure(const boost::shared_ptr<CacheLayerRegistry>& registry) {
    	   // become one of owners of the arrived registry:
    	   m_registry = registry;
    	   // pass the registry reference to sync module:
    	   return m_syncModule.init(registry);
       }

       /**
        * Shutdown cache manager.
        *
        * @param force - flag, indicates whether all work in progress should be forcelly interrupted.
        * If false, all work in progress will be completed.
        * If true, all work will be cancelled.
        *
        * @param updateClients - flag, indicates whether completion callbacks should be invoked on
        * pending clients
        */
       status::StatusInternal shutdown(bool force = true, bool updateClients = true);

        /**
        * @fn Status cacheEstimate(clusterId cluster, std::list<const char*> files, time_t* time)
        * @brief For files from the list @a files, check whether all files can be accessed locally,
        *        estimate the time required to get locally all files from the list if any specified
        *        file is not available locally yet.
        *
        *        Internally, this call is divided to phases:
        *        - check the cache persistence which files we already have.
        *        - for files that are not locally, invoke sync to estimate the time to get them -
        *        per file
        *        - aggregate estimations reported by Sync for each file and reply to client.
        *
        *        TODO: whether this operation async? If so - need to add the SessionContext here and the callback.
        *
        * @param[In]  namenode    - namenode connection details
        * @param[In]  files       - List of files required to be locally.
        * @param[Out] time        - time required to get all requested files locally (if any).
        * Zero time means all data is in place
        *
        * @param[In]  async       - if true, the callback should be passed as well in order to be called on the operation completion.
        * @param[In]  callback    - callback that should be invoked on completion in case if async mode is selected
        *
        * @return Operation status. If either file is not available in specified @a cluster
        * the status will be "Canceled"
        */
       status::StatusInternal cacheEstimate(SessionContext session, const NameNodeDescriptor & namenode, const std::list<const char*>& files, time_t& time,
    		   CacheEstimationCompletedCallback callback, bool async = true);

       /**
        * @fn Status cachePrepareData(SessionContext session, clusterId cluster, std::list<const char*> files)
        * @brief Run load scenario from specified files list @a files from the @a cluster.
        *
        *       Internally, this call is divided to phases:
        *       - create Prepare Request. Filter out files that are already locally.
        *       - for each file which is not marked as "local"  or "in progress" in persistence,
        *       mark file with "in progress" and run Sync to download it.
        *       - in callback from Sync, decrement number of remained files to prepare - only in case if
        *       consequential download finished successfully. Update persistence.
        *       - if any file download operation got failure report from Sync, Prepare Request should be marked as failed
        *       and failure immediately reported to caller (coordinator) with detail per file.
        *       - Once number of remained files in Prepare Request becomes 0, report the final callback
        *       to the caller (coordinator) with the overall status.
        * @param[In]  session     - Request session id.
        * @param[In]  namenode    - namenode connection details
        * @param[In]  files       - List of files required to be locally.
        * @param[Out] callback    - callback to invoke when prepare is finished (whatever the status).
        *
        * @return Operation status
        */
       status::StatusInternal cachePrepareData(SessionContext session, const NameNodeDescriptor & namenode,
    		   const std::list<const char*>& files,
    		   PrepareCompletedCallback callback);

       /**
        * @fn Status cacheCancelPrepareData(SessionContext session)
        * @brief cancel prepare data request
        * param[In] session - client's session, prepare request caller
        *
        * @return Operation status
        */
       status::StatusInternal cacheCancelPrepareData(SessionContext session);

       /**
        * @fn Status cachePrepareData(SessionContext session, clusterId cluster, std::list<const char*> files)
        * @brief Run load scenario from specified files list @a files from the @a cluster.
        *
        * @param[In]   session     - Request session id.
        * @param[Out]  progress    - Detailed prepare progress. Can be used to present it to the user.
        * @param[Out]  performance - to hold request current performance statistic
        *
        * @return Operation status
        */
       status::StatusInternal cacheCheckPrepareStatus(SessionContext session, std::list<FileProgress*>& progress, request_performance& performance );

       /**
        * Utility function to free list of @a FileProgress entities
        *
        * @param[In] progress - list of @a FileProgress entities to cleanup
        *
        * @return Operation status
        */
       status::StatusInternal freeFileProgressList(std::list<FileProgress*>& progress);

       /**
        * Get the file.
        *
        * @param[in]     namenode   - namenode the file belongs to
        * @param[in]     path       - file path
        * @param[in/out] file       - managed file metadata
        *
        * @return true if the file exists.
        */
        bool getFile(const NameNodeDescriptor & namenode, const char* path, ManagedFile::File*& file);

};
} /** namespace impala */


#endif /* CACHE_MGR_H_ */
