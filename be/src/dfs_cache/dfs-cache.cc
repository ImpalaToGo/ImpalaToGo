/** @file dfs-cache.cc
 *  @brief Impala Cache layer API implementation.
 *  Make the forwarding of APIs to corresponding responsible modules.
 *
 *  Internal modules:
 *  0. CacheLayerRegistry - is responsible to hold and share in the safe way data that is required to:
 *                          > track the local cache (dfs mapping -> catalogs system -> files) from filesystem perspective
 *                          > track the configured remote DFS fsDescriptors to access the data from
 *                          > hold the DFS Plugins Factory which provides Plugins to work with remote DFSs.
 *
 *  1. CacheManager       - is responsible for cache-related operations (estimation, handling, report for progress by polling or by callback to subscribers)
 *  2. FileManager        - is responsible for file-system related operations (any kind of work with local file system).
 *
 *  All mentioned in 0-2 entities are represented as singletons.
 *
 *  @date   Oct 3, 2014
 *  @author elenav
 */

#include <string.h>

#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.

#include <boost/thread/condition_variable.hpp>

#include "dfs_cache/dfs-cache.h"
#include "dfs_cache/cache-layer-registry.hpp"
#include "dfs_cache/cache-mgr.hpp"
#include "dfs_cache/filesystem-mgr.hpp"
#include "dfs_cache/uri-util.hpp"

namespace impala {

namespace constants
{
    /** default location for cache */
     const std::string DEFAULT_CACHE_ROOT = " /var/cache/impalatogo";

     /** default filesystem configuration is requested.
      * See core-site.xml
      *
      * <property>
  	  *	  <name>fs.defaultFS</name>
  	  *	  <value>file:///</value>
	  * </property>
      */
     const std::string DEFAULT_FS = "default";
}

namespace ph = std::placeholders;

/** *********************************************************************************************
 * ***********************   APIs published by Cache Management.  *******************************
 * **********************************************************************************************
 */

status::StatusInternal cacheInit() {
	// Initialize singletons.
	CacheLayerRegistry::init();
    CacheManager::init();
    filemgmt::FileSystemManager::init();

	CacheManager::instance()->configure();
	filemgmt::FileSystemManager::instance()->configure();
	return status::StatusInternal::OK;
}

status::StatusInternal cacheConfigureLocalStorage(const std::string& localpath){
	CacheLayerRegistry::instance()->localstorage(localpath);
	return status::StatusInternal::OK;
}

status::StatusInternal cacheConfigureFileSystem(FileSystemDescriptor& fsDescriptor){
	CacheLayerRegistry::instance()->setupFileSystem(fsDescriptor);
	return status::StatusInternal::OK;
}

status::StatusInternal cacheShutdown(bool force, bool updateClients) {
	CacheManager::instance()->shutdown(force, updateClients);
	return status::StatusInternal::OK;
}

status::StatusInternal cacheEstimate(SessionContext session, const FileSystemDescriptor & fsDescriptor,
		const DataSet& files, time_t & time, CacheEstimationCompletedCallback callback,
		requestIdentity & requestIdentity, bool async) {
	return CacheManager::instance()->cacheEstimate(session, fsDescriptor, files, time, callback,
			requestIdentity, async);
}

status::StatusInternal cachePrepareData(SessionContext session, const FileSystemDescriptor & fsDescriptor,
		const DataSet& files, PrepareCompletedCallback callback, requestIdentity & requestIdentity) {
	return CacheManager::instance()->cachePrepareData(session, fsDescriptor, files, callback, requestIdentity);
}

status::StatusInternal cacheCancelPrepareData(const requestIdentity & requestIdentity) {
	return CacheManager::instance()->cacheCancelPrepareData(requestIdentity);
}

status::StatusInternal cacheCheckPrepareStatus(const requestIdentity & requestIdentity,
		std::list<boost::shared_ptr<FileProgress> >& progress, request_performance& performance) {
	return CacheManager::instance()->cacheCheckPrepareStatus(requestIdentity, progress, performance);
}

/** *********************************************************************************************
 * ***********************   APIs to work with files. Inherited from libhdfs  *******************
 * **********************************************************************************************
 */
dfsFile dfsOpenFile(const FileSystemDescriptor & fsDescriptor, const char* path, int flags,
		int bufferSize, short replication, tSize blocksize, bool& available) {
	// TODO:Check in the registry, whether the file is available. if not, report an error.
	// Otherwise, create the file descriptor, add it to the registry and share with the caller.
    //managed_file::File* file;

	// try open it locally first (from cache):

	dfsFile handle = filemgmt::FileSystemManager::instance()->dfsOpenFile(fsDescriptor, path, flags,
				bufferSize, replication, blocksize, available);
    if(handle != nullptr && available){
    	//file->open(handle);
    	// file is available locally, just reply it back:
    	return handle;
    }

    // file is not available in the cache, run the auto-load scenario:

    DataSet data;
    data.push_back(path);

    bool condition = false;
    boost::condition_variable condition_var;
    boost::mutex completion_mux;

    status::StatusInternal cbStatus;

	PrepareCompletedCallback cb = [&] (SessionContext context,
			const std::list<boost::shared_ptr<FileProgress> > & progress,
			request_performance const & performance, bool overall,
			bool canceled, taskOverallStatus status) -> void {

		boost::lock_guard<boost::mutex> lock(completion_mux);
		cbStatus = (status == taskOverallStatus::COMPLETED_OK ? status::StatusInternal::OK : status::StatusInternal::REQUEST_FAILED);
		if(status != taskOverallStatus::COMPLETED_OK)
			LOG (ERROR) << "Failed to load file \"" << path << "\"" << ". Status : "
					<< status << ".\n";

		if(context == NULL)
			LOG (ERROR) << "NULL context received while loading the file \"" << path << "\"" << ".Status : "
								<< status << ".\n";

		if(progress.size() != data.size())
			LOG (ERROR) << "Expected amount of progress is not equal to received for file \"" << path << "\"" << ".Status : "
								<< status << ".\n";

		if(!overall)
			LOG (ERROR) << "Expected amount of progress is not equal to received for file \"" << path << "\"" << ".Status : "
											<< status << ".\n";
		condition = true;
		condition_var.notify_all();
		};

	time_t time_ = 0;
	requestIdentity identity;
	// execute request in async way:

	using namespace std::placeholders;

	auto f1 = std::bind(cachePrepareData, ph::_1, ph::_2, ph::_3, ph::_4, ph::_5);

	boost::uuids::uuid uuid = boost::uuids::random_generator()();

	std::string local_client = boost::lexical_cast<std::string>(uuid);
	SessionContext ctx = static_cast<void*>(&local_client);

	status::StatusInternal status;

	// run the prepare routine
	status = f1(ctx, std::cref(fsDescriptor), std::cref(data), cb, std::ref(identity));

	// check operation scheduling status:
	if(status != status::StatusInternal::OPERATION_ASYNC_SCHEDULED){
		LOG (ERROR) << "Prepare request - failed to schedule - for \"" << path << "\"" << ". Status : "
													<< status << ".\n";
		// no need to wait for callback to fire, operation was not scheduled
		return nullptr;
	}

	// wait when completion callback will be fired by Prepare scenario:
	boost::unique_lock<boost::mutex> lock(completion_mux);
	condition_var.wait(lock, [&] { return condition;});

	lock.unlock();

	// check callback status:
	if(cbStatus != status::StatusInternal::OK){
		LOG (ERROR) << "Prepare request failed for \"" << path << "\"" << ". Status : "
															<< status << ".\n";
		return nullptr;
	}

    // Now need just recall this back
	handle = filemgmt::FileSystemManager::instance()->dfsOpenFile(fsDescriptor, path, flags,
					bufferSize, replication, blocksize, available);
	    if(handle != nullptr && available){
	    	//file->open(handle);
	    	// file is available locally already:
	    	return handle;
	    }
	LOG (ERROR) << "File \"" << path << "\" is not available" << "\n";

	return handle;
}

status::StatusInternal dfsCloseFile(const FileSystemDescriptor & fsDescriptor, dfsFile file) {
	return filemgmt::FileSystemManager::instance()->dfsCloseFile(fsDescriptor, file);
}

status::StatusInternal dfsExists(const FileSystemDescriptor & fsDescriptor, const char *path) {
	return filemgmt::FileSystemManager::instance()->dfsExists(fsDescriptor, path);
}

status::StatusInternal dfsSeek(const FileSystemDescriptor & fsDescriptor, dfsFile file, tOffset desiredPos) {
	return filemgmt::FileSystemManager::instance()->dfsSeek(fsDescriptor, file, desiredPos);
}

tOffset dfsTell(const FileSystemDescriptor & fsDescriptor, dfsFile file) {
	return filemgmt::FileSystemManager::instance()->dfsTell(fsDescriptor, file);
}

tSize dfsRead(const FileSystemDescriptor & fsDescriptor, dfsFile file, void* buffer, tSize length) {
	return filemgmt::FileSystemManager::instance()->dfsRead(fsDescriptor, file, buffer, length);
}

tSize dfsPread(const FileSystemDescriptor & fsDescriptor, dfsFile file, tOffset position, void* buffer, tSize length) {
	return filemgmt::FileSystemManager::instance()->dfsPread(fsDescriptor, file, position, buffer, length);
}

tSize dfsWrite(const FileSystemDescriptor & fsDescriptor, dfsFile file, const void* buffer, tSize length) {
	return filemgmt::FileSystemManager::instance()->dfsWrite(fsDescriptor, file, buffer, length);
}

status::StatusInternal dfsFlush(const FileSystemDescriptor & fsDescriptor, dfsFile file) {
	return filemgmt::FileSystemManager::instance()->dfsFlush(fsDescriptor, file);
}

tOffset dfsAvailable(const FileSystemDescriptor & fsDescriptor, dfsFile file) {
	return filemgmt::FileSystemManager::instance()->dfsAvailable(fsDescriptor, file);
}

status::StatusInternal dfsCopy(const FileSystemDescriptor & fsDescriptor1, const char* src,
		const FileSystemDescriptor & fsDescriptor2, const char* dst) {
	return filemgmt::FileSystemManager::instance()->dfsCopy(fsDescriptor1, src, fsDescriptor2, dst);
}

status::StatusInternal dfsMove(const FileSystemDescriptor & fsDescriptor, const char* src, const char* dst) {
	return filemgmt::FileSystemManager::instance()->dfsMove(fsDescriptor, src, dst);
}

status::StatusInternal dfsDelete(const FileSystemDescriptor & fsDescriptor, const char* path, int recursive = 1) {
	return filemgmt::FileSystemManager::instance()->dfsDelete(fsDescriptor, path, recursive);
}

status::StatusInternal dfsRename(const FileSystemDescriptor & fsDescriptor, const char* oldPath,
		const char* newPath) {
	return filemgmt::FileSystemManager::instance()->dfsRename(fsDescriptor, oldPath, newPath);
}

status::StatusInternal dfsCreateDirectory(const FileSystemDescriptor & fsDescriptor, const char* path) {
	return filemgmt::FileSystemManager::instance()->dfsCreateDirectory(fsDescriptor, path);
}

status::StatusInternal dfsSetReplication(const FileSystemDescriptor & fsDescriptor, const char* path, int16_t replication) {
	return filemgmt::FileSystemManager::instance()->dfsSetReplication(fsDescriptor, path, replication);
}

dfsFileInfo *dfsListDirectory(const FileSystemDescriptor & fsDescriptor, const char* path,
		int *numEntries) {
	return filemgmt::FileSystemManager::instance()->dfsListDirectory(fsDescriptor, path, numEntries);
}

dfsFileInfo *dfsGetPathInfo(const FileSystemDescriptor & fsDescriptor, const char* path) {
	return filemgmt::FileSystemManager::instance()->dfsGetPathInfo(fsDescriptor, path);
}

void dfsFreeFileInfo(const FileSystemDescriptor & fsDescriptor, dfsFileInfo *dfsFileInfo, int numEntries) {
	return filemgmt::FileSystemManager::instance()->dfsFreeFileInfo(fsDescriptor, dfsFileInfo, numEntries);
}

tOffset dfsGetCapacity(const FileSystemDescriptor & fsDescriptor, const char* host) {
	return filemgmt::FileSystemManager::instance()->dfsGetCapacity(fsDescriptor, host);
}

tOffset dfsGetUsed(const FileSystemDescriptor & fsDescriptor, const char* host) {
	return filemgmt::FileSystemManager::instance()->dfsGetUsed(fsDescriptor, host);
}

status::StatusInternal dfsChown(const FileSystemDescriptor & fsDescriptor, const char* path,
		const char *owner, const char *group) {
	return filemgmt::FileSystemManager::instance()->dfsChown(fsDescriptor, path, owner, group);
}

status::StatusInternal dfsChmod(const FileSystemDescriptor & fsDescriptor, const char* path, short mode) {
	return filemgmt::FileSystemManager::instance()->dfsChmod(fsDescriptor, path, mode);
}

int dfsFileGetReadStatistics(const FileSystemDescriptor & fsDescriptor, dfsFile file,
		struct dfsReadStatistics **stats){
	return -1;
}

int64_t dfsReadStatisticsGetRemoteBytesRead(const struct dfsReadStatistics *stats){
	return -1;
}

void dfsFileFreeReadStatistics(const FileSystemDescriptor & fsDescriptor, struct dfsReadStatistics *stats){
}

}
