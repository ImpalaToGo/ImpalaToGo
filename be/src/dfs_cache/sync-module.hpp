/** @file sync-module.h
 *  @brief Definition of Sync module within Cache layer
 *
 *  @date   Sep 26, 2014
 *  @author elenav
 */
#ifndef SYNC_MODULE_H_
#define SYNC_MODULE_H_

#include <boost/shared_ptr.hpp>

#include "dfs_cache/cache-layer-registry.hpp"

/**
 * @namespace impala
 */
namespace impala {

/**
 * Sync module is responsible to sync the local fs with remote dfs in regards to requested files.
 * Sync is also responsible to maintain local cache validation if requested.
 *
 * Sync module in general may be the mediator to remote dfs operations as it work with dfs plugins.
 * All Sync module API module are reentrant which allows it to be used the layer for any task and control it here.
 */
class Sync{
private:
	CacheLayerRegistry*   m_registry;            /**< reference to metadata registry instance */

public:
	Sync() : m_registry(nullptr) {}

	/**
	* init - Init the Sync module with an access to shared registry.
	*
	* @param registry - the reference to the registry
	* @return operation status
	*/
	status::StatusInternal init() {
		m_registry = CacheLayerRegistry::instance();
		return status::StatusInternal::OK;
	}

	/**
	 * estimateTimeToGetFileLocally - estimates how much time will take to get the file with specified @a path
	 * locally (within the file system @a fsDescriptor)
	 *
	 * @param[in] fsDescriptor - fs connection details
	 * @param[in] path         - file path
	 * @param[in] task         - task to run in operation
	 *
	 * @return operation status
	 */
	status::StatusInternal estimateTimeToGetFileLocally(const FileSystemDescriptor & fsDescriptor, const char* path,
			request::MakeProgressTask<boost::shared_ptr<FileProgress> >* const & task);

	/**
	 * prepareFile - download file locally and update the registry.
	 * Reentrant as only rely on its parameters
	 *
	 * @param[in] fsDescriptor - fs connection details
	 * @param[in] path         - file path
	 *
	 * @param[in] task         - task to run in operation
	 *
	 * @return operation status
	 */
	status::StatusInternal prepareFile(const FileSystemDescriptor & fsDescriptor, const char* path,
			request::MakeProgressTask<boost::shared_ptr<FileProgress> >* const & task);

	/**
	 * cancel active "make progress" file request (prepare / estimate) if any, described by its synchronization  context (for re-entrancy)
	 * All these sync context is handled here so in the same class
	 *
	 * @param async        - flag, indicates whether the "make progress" file operation should be interrupted immediately
	 * @param cancellable  - cancellable task with cancellation context
	 */
	status::StatusInternal cancelFileMakeProgress(bool async, request::CancellableTask* const& cancellable);

	/**
	 * validateLocalCache - run validation of local cache (data and metadata) according to configured cluster credentials
	 *
	 * @param[in/out] valid -flag, indicates that validation operation confirmed cache integrity completely.
	 * In context of this operation, local cache may change.
	 *
	 * This operation may be user-driven
	 */
	status::StatusInternal validateLocalCache(bool& valid);
};

}



#endif /* SYNC_MODULE_H_ */
