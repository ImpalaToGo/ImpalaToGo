/** @file dfs-cache.cc
 *  @brief Impala Cache layer API implementation.
 *  Make the forwarding of APIs to corresponding responsible modules.
 *
 *  Internal modules:
 *  0. CacheLayerRegistry - is responsible to hold and share in the safe way data that is required to:
 *                          > track the local cache (dfs mapping -> catalogs system -> files) from filesystem perspective
 *                          > track the configured remote DFS namenodes to access the data from
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

	// and pin its singleton reference
	boost::shared_ptr<CacheLayerRegistry>  cacheRegistry(CacheLayerRegistry::instance());

	// now share the cache registry between Cache Manager and File Manager
	CacheManager::instance()->configure(cacheRegistry);
	filemgmt::FileSystemManager::instance()->configure(cacheRegistry);
	return status::StatusInternal::OK;
}

status::StatusInternal cacheConfigureLocalStorage(const std::string& localpath){
	CacheLayerRegistry::instance()->localstorage(localpath);
	return status::StatusInternal::OK;
}

status::StatusInternal cacheConfigureDFSPluginFactory(const boost::shared_ptr<dfsAdaptorFactory>& factory){
	CacheLayerRegistry::instance()->setupDFSPluginFactory(factory);
	return status::StatusInternal::OK;
}

status::StatusInternal cacheConfigureNameNode(const NameNodeDescriptor& namenode){
	CacheLayerRegistry::instance()->setupNamenode(namenode);
	return status::StatusInternal::OK;
}

status::StatusInternal cacheShutdown(bool force, bool updateClients) {
	CacheManager::instance()->shutdown(force, updateClients);
	return status::StatusInternal::OK;
}

status::StatusInternal cacheEstimate(SessionContext session, const NameNodeDescriptor & namenode,
		const std::list<const char*>& files, time_t & time,
		CacheEstimationCompletedCallback callback, bool async) {
	return CacheManager::instance()->cacheEstimate(session, namenode, files, time, callback,
			async);
}

status::StatusInternal cachePrepareData(SessionContext session, const NameNodeDescriptor & namenode,
		const std::list<const char*>& files,
		PrepareCompletedCallback callback) {
	return CacheManager::instance()->cachePrepareData(session, namenode, files, callback);
}

status::StatusInternal cacheCancelPrepareData(SessionContext session) {
	return CacheManager::instance()->cacheCancelPrepareData(session);
}

status::StatusInternal cacheCheckPrepareStatus(SessionContext session,
		std::list<FileProgress*>& progress, request_performance& performance) {
	return CacheManager::instance()->cacheCheckPrepareStatus(session, progress, performance);
}

status::StatusInternal freeFileProgressList(std::list<FileProgress*>& progress) {
	return CacheManager::instance()->freeFileProgressList(progress);
}

/** *********************************************************************************************
 * ***********************   APIs to work with files. Inherited from libhdfs  *******************
 * **********************************************************************************************
 */
dfsFile dfsOpenFile(const NameNodeDescriptor & namenode, const char* path, int flags,
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
	dfsFile handle = filemgmt::FileSystemManager::instance()->dfsOpenFile(namenode, uri.FilePath.c_str(), flags,
			bufferSize, replication, blocksize, available);
	file->open(handle);
	return handle;
}

status::StatusInternal dfsCloseFile(const NameNodeDescriptor & namenode, dfsFile file) {
	return filemgmt::FileSystemManager::instance()->dfsCloseFile(namenode, file);
}

status::StatusInternal dfsExists(const NameNodeDescriptor & namenode, const char *path) {
	return filemgmt::FileSystemManager::instance()->dfsExists(namenode, path);
}

status::StatusInternal dfsSeek(const NameNodeDescriptor & namenode, dfsFile file, tOffset desiredPos) {
	return filemgmt::FileSystemManager::instance()->dfsSeek(namenode, file, desiredPos);
}

tOffset dfsTell(const NameNodeDescriptor & namenode, dfsFile file) {
	return filemgmt::FileSystemManager::instance()->dfsTell(namenode, file);
}

tSize dfsRead(const NameNodeDescriptor & namenode, dfsFile file, void* buffer, tSize length) {
	return filemgmt::FileSystemManager::instance()->dfsRead(namenode, file, buffer, length);
}

tSize dfsPread(const NameNodeDescriptor & namenode, dfsFile file, tOffset position, void* buffer, tSize length) {
	return filemgmt::FileSystemManager::instance()->dfsPread(namenode, file, position, buffer, length);
}

tSize dfsWrite(const NameNodeDescriptor & namenode, dfsFile file, const void* buffer, tSize length) {
	return filemgmt::FileSystemManager::instance()->dfsWrite(namenode, file, buffer, length);
}

status::StatusInternal dfsFlush(const NameNodeDescriptor & namenode, dfsFile file) {
	return filemgmt::FileSystemManager::instance()->dfsFlush(namenode, file);
}

tOffset dfsAvailable(const NameNodeDescriptor & namenode, dfsFile file) {
	return filemgmt::FileSystemManager::instance()->dfsAvailable(namenode, file);
}

status::StatusInternal dfsCopy(const NameNodeDescriptor & namenode1, const char* src,
		const NameNodeDescriptor & namenode2, const char* dst) {
	return filemgmt::FileSystemManager::instance()->dfsCopy(namenode1, src, namenode2, dst);
}

status::StatusInternal dfsMove(const NameNodeDescriptor & namenode, const char* src, const char* dst) {
	return filemgmt::FileSystemManager::instance()->dfsMove(namenode, src, dst);
}

status::StatusInternal dfsDelete(const NameNodeDescriptor & namenode, const char* path, int recursive = 1) {
	return filemgmt::FileSystemManager::instance()->dfsDelete(namenode, path, recursive);
}

status::StatusInternal dfsRename(const NameNodeDescriptor & namenode, const char* oldPath,
		const char* newPath) {
	return filemgmt::FileSystemManager::instance()->dfsRename(namenode, oldPath, newPath);
}

status::StatusInternal dfsCreateDirectory(const NameNodeDescriptor & namenode, const char* path) {
	return filemgmt::FileSystemManager::instance()->dfsCreateDirectory(namenode, path);
}

status::StatusInternal dfsSetReplication(const NameNodeDescriptor & namenode, const char* path, int16_t replication) {
	return filemgmt::FileSystemManager::instance()->dfsSetReplication(namenode, path, replication);
}

dfsFileInfo *dfsListDirectory(const NameNodeDescriptor & namenode, const char* path,
		int *numEntries) {
	return filemgmt::FileSystemManager::instance()->dfsListDirectory(namenode, path, numEntries);
}

dfsFileInfo *dfsGetPathInfo(const NameNodeDescriptor & namenode, const char* path) {
	return filemgmt::FileSystemManager::instance()->dfsGetPathInfo(namenode, path);
}

void dfsFreeFileInfo(const NameNodeDescriptor & namenode, dfsFileInfo *dfsFileInfo, int numEntries) {
	return filemgmt::FileSystemManager::instance()->dfsFreeFileInfo(namenode, dfsFileInfo, numEntries);
}

tOffset dfsGetCapacity(const NameNodeDescriptor & namenode, const char* host) {
	return filemgmt::FileSystemManager::instance()->dfsGetCapacity(namenode, host);
}

tOffset dfsGetUsed(const NameNodeDescriptor & namenode, const char* host) {
	return filemgmt::FileSystemManager::instance()->dfsGetUsed(namenode, host);
}

status::StatusInternal dfsChown(const NameNodeDescriptor & namenode, const char* path,
		const char *owner, const char *group) {
	return filemgmt::FileSystemManager::instance()->dfsChown(namenode, path, owner, group);
}

status::StatusInternal dfsChmod(const NameNodeDescriptor & namenode, const char* path, short mode) {
	return filemgmt::FileSystemManager::instance()->dfsChmod(namenode, path, mode);
}

int dfsFileGetReadStatistics(const NameNodeDescriptor & namenode, dfsFile file,
		struct dfsReadStatistics **stats){
	return -1;
}

int64_t dfsReadStatisticsGetRemoteBytesRead(const struct dfsReadStatistics *stats){
	return -1;
}

void dfsFileFreeReadStatistics(const NameNodeDescriptor & namenode, struct dfsReadStatistics *stats){
}

}
