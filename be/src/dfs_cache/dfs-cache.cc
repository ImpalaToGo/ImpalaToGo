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

#include "dfs_cache/dfs-cache.h"
#include "dfs_cache/cache-layer-registry.hpp"
#include "dfs_cache/cache-mgr.hpp"
#include "dfs_cache/filesystem-mgr.hpp"
#include "dfs_cache/uri-util.hpp"

namespace impala {

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

status::StatusInternal cacheConfigureClusterFocalPoint(const FileSystemDescriptor& fsDescriptor){
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
	// cut the file path only without a host to get the file name:
	Uri uri = Uri::Parse(path);
	// Check in the registry, whether the file is available. if not, report an error.
	// Otherwise, create the file descriptor, add it to the registry and share with the caller.
	ManagedFile::File* file;

	// file is not available

	/*
	 if(!cacheMgr.getFile(cluster, path, file)){
	 available = false;
	 return nullptr;
	 }

	 // seems file is available locally.
	 available = true;

	 // check that we have already an open handle to the file:
	 if(file->m_handle != nullptr)
	 return file->m_handle;

	 // there's no opened handle exist for the file yet, so open it and update:
	 file->m_handle = filemgmt::FileSystemManager::instance()->dfsOpenFile(cluster, uri.FilePath.c_str(), flags, bufferSize, replication, blocksize, available);
	 file->m_owners++;
	 return file->m_handle;
	 */
	file = new ManagedFile::File(path, path);
	dfsFile handle = filemgmt::FileSystemManager::instance()->dfsOpenFile(fsDescriptor, uri.FilePath.c_str(), flags,
			bufferSize, replication, blocksize, available);
	file->open(handle);
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
