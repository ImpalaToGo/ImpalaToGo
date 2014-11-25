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
#include "dfs_cache/utilities.hpp"

namespace impala {

namespace constants
{
    /** default location for cache */
     const std::string DEFAULT_CACHE_ROOT = "/var/cache/impalatogo/";

     /** default cache capacity  */
     const long long DEFAULT_CACHE_CAPACITY = 50000000000;       // 50 Gb in bytes

     /** default filesystem configuration is requested.
      * See core-site.xml
      *
      * <property>
  	  *	  <name>fs.defaultFS</name>
  	  *	  <value>file:///</value>
	  * </property>
      */
     const std::string DEFAULT_FS = "default";

     /** HDFS scheme name */
     const std::string HDFS_SCHEME = "hdfs";

     /** S3N scheme name */
     const std::string S3N_SCHEME = "s3n";

     /** separator we use to divide the source host and the port in the file path */
     const char HOST_PORT_SEPARATOR = '_';
}

namespace ph = std::placeholders;

/** *********************************************************************************************
 * ***********************   APIs published by Cache Management.  *******************************
 * **********************************************************************************************
 */

status::StatusInternal cacheInit(const std::string& root) {
	// Initialize singletons.
	CacheLayerRegistry::init(root);
    CacheManager::init();
    filemgmt::FileSystemManager::init();

	CacheManager::instance()->configure();
	filemgmt::FileSystemManager::instance()->configure();
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

	Uri uri = Uri::Parse(path);
    managed_file::File* managed_file;
	// first check whether the file is already in the registry.
	// for now, when the autoload is the default behavior, we return immediately if we found that there's no file exist in the registry
	if(!CacheLayerRegistry::instance()->findFile(uri.FilePath.c_str(), fsDescriptor, managed_file) || managed_file == nullptr){
		LOG (ERROR) << "File \"/" << fsDescriptor.host << ":" << std::to_string(fsDescriptor.port) <<
				"/" << path << "\" is not available either on target or locally." << "\n";
		return NULL; // return plain NULL to support past-c++0x
	}

	// so as the file is in the registry, just open it:
	if(managed_file->exists())
		managed_file->open(); // mark the file with the one more usage
	else{
		LOG (ERROR) << "File \"" << path << "\" is not available in LRU." << "\n";
		return NULL;
	}

	dfsFile handle = filemgmt::FileSystemManager::instance()->dfsOpenFile(fsDescriptor, uri.FilePath.c_str(), flags,
				bufferSize, replication, blocksize, available);
    if(handle != nullptr && available){
    	// file is available locally, just reply it back:
    	return handle;
    }
    LOG (ERROR) << "File \"" << path << "\" is not available" << "\n";
    return handle;
}

status::StatusInternal dfsCloseFile(const FileSystemDescriptor & fsDescriptor, dfsFile file) {

	status::StatusInternal status;
	std::string path = filemgmt::FileSystemManager::filePathByDescriptor(file);
    if(path.empty()){
    	status = status::StatusInternal::DFS_OBJECT_DOES_NOT_EXIST;
    	LOG (WARNING) << "File descriptor is not resolved within the system!" << "\n";
    }
    // anyway try close the file
    status = filemgmt::FileSystemManager::instance()->dfsCloseFile(fsDescriptor, file);

	managed_file::File* managed_file;
	// if no file path resolved from the file descriptor, no chance to find it in the cache.
	if(path.empty())
		return status;

	if(!CacheLayerRegistry::instance()->findFile(path.c_str(), managed_file) || managed_file == nullptr){
		status = status::StatusInternal::CACHE_OBJECT_NOT_FOUND;
	}
	managed_file->close();

	return status;
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
	status::StatusInternal status;
	// Remove the file from registry if it is there:
	bool result = CacheLayerRegistry::instance()->deleteFile(fsDescriptor, path);
	status = result ? status::StatusInternal::OK : status::FILE_OBJECT_OPERATION_FAILURE;

	// delete the file from the file system:
	//status::StatusInternal status = filemgmt::FileSystemManager::instance()->dfsDelete(fsDescriptor, path, recursive);
	return status;
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
