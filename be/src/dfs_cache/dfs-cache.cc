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
#include <fcntl.h>

#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.

#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>

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

     /** default filesystem configuration is requested.
      * See core-site.xml
      *
      * <property>
  	  *	  <name>fs.defaultFS</name>
  	  *	  <value>file:///</value>
	  * </property>
      */
     const std::string DEFAULT_FS = "default";

     /** hdfs scheme name */
     const std::string HDFS_SCHEME = "hdfs";

     /** s3n scheme name */
     const std::string S3N_SCHEME = "s3n";

     /** separator we use to divide the source host and the port in the file path */
     const char HOST_PORT_SEPARATOR = '_';
}

namespace ph = std::placeholders;

/** *********************************************************************************************
 * ***********************   APIs published by Cache Management.  *******************************
 * **********************************************************************************************
 */

status::StatusInternal cacheInit(int mem_limit_percent, const std::string& root) {
	// Initialize singletons.
	CacheLayerRegistry::init(mem_limit_percent, root);
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

/**
 * Handle "open file for write" scenario
 *
 * @param [in]  fsDescriptor - filesystem descriptor
 * @param [in]  path         - file path
 * @param [in]  bufferSize   - buffer size
 * @param [in]  replication  - replication (hdfs-only relevant)
 * @param [in]  blockSize    - block size
 * @param [out] available    - flag, indicates whether the file is available after all
 *
 * @return local (cached) file handle
 */
static dfsFile openForWrite(const FileSystemDescriptor & fsDescriptor, const char* path,
		int bufferSize, short replication, tSize blocksize, bool& available){

	Uri uri = Uri::Parse(path);

	// locate the remote filesystem adapter:
	boost::shared_ptr<FileSystemDescriptorBound> fsAdaptor = (*CacheLayerRegistry::instance()->getFileSystemDescriptor(fsDescriptor));
	if(fsAdaptor == nullptr){
		LOG (ERROR) << "No filesystem adaptor configured for FileSystem \"" << fsDescriptor.dfs_type << ":" <<
				fsDescriptor.host << "\"" << "\n";
		// no namenode adaptor configured
		return NULL;
	}

    raiiDfsConnection connection(fsAdaptor->getFreeConnection());
    if(!connection.valid()) {
    	LOG (ERROR) << "No connection to dfs available, unable to create file for write on FileSystem \"" << fsDescriptor.dfs_type << ":" <<
    			fsDescriptor.host << "\"" << "\n";
    	return NULL;
    }

    // Add the reference to new file into LRU registry
    managed_file::File* managed_file;
    bool ret = CacheLayerRegistry::instance()->addFile(uri.FilePath.c_str(), fsDescriptor, managed_file);
    if(!ret){
    	LOG (ERROR) << "Unable to add the file to the LRU registry for FileSystem \"" << fsDescriptor.dfs_type << ":" <<
    			fsDescriptor.host << "\"" << "\n";
    	return NULL;
    }
    managed_file->open(); // create one more reference as a client who will be back for this file
    // say file is busy with "write" operation:
    managed_file->state(managed_file::State::FILE_IS_UNDER_WRITE);

	// open remote file:
	dfsFile hfile = fsAdaptor->fileOpen(connection, managed_file->relative_name().c_str(), O_WRONLY, 0, 0, 0);

	if(hfile == NULL){
		LOG (ERROR) << "Failed to open remote file \"" << path << "\" for write on FileSystem \"" << fsDescriptor.dfs_type << "//:" <<
				fsDescriptor.host << "\"" << "\n";
		// update file status:
		managed_file->state(managed_file::State::FILE_IS_FORBIDDEN);
		managed_file->close(); // close the reference to file as a "client"
		return NULL;
	}

	int retstatus = -1;

	// create local file:
	dfsFile handle = filemgmt::FileSystemManager::instance()->dfsOpenFile(
			fsDescriptor, uri.FilePath.c_str(), O_CREAT, bufferSize, replication,
			blocksize, available);
	// check we have it opened
	if(handle == nullptr || !available){
		// file was not opened for write, cleanup opened dfs file handle
		LOG (ERROR) << "Failed to open local file for write : \"" <<
						path << "\"." << "\n";

		// cleanup:
		// remotely (file close):
		retstatus = fsAdaptor->fileClose(connection, hfile);
		if(retstatus != 0){
			LOG (ERROR) << "Failed to close remote file : \"" << path << "\"." << "\n";
		}
		managed_file->state(managed_file::State::FILE_IS_FORBIDDEN);
		managed_file->close(); // close the reference to file as a "client"
		// from registry:
		ret = CacheLayerRegistry::instance()->deleteFile(fsDescriptor, path);
		if(!ret){
			LOG (ERROR) << "Failed to clean the file : \"" << path << "\" from LRU registry." << "\n";
		}
		return NULL;
	}

	// file is available locally:
	LOG (INFO) << "Successfully opened both local and remote files for write : \"" <<
			path << "\"." << "\n";

	// pin opened handles in the registry:
	ret = CacheLayerRegistry::instance()->registerCreateFromSelectScenario(handle, hfile);
	if(ret)
		return handle;

    LOG (ERROR)<< "Failed to register CREATE ON SELECT scenario within the registry for file : \"" << path << "\"." << "\n";
	// cleanup:
	// remote file close:
	retstatus = fsAdaptor->fileClose(connection, hfile);
	if (retstatus != 0) {
		LOG (ERROR)<< "Failed to close remote file : \"" << path << "\"." << "\n";
	}
	// local file close:
	status::StatusInternal status = filemgmt::FileSystemManager::instance()->dfsCloseFile(fsDescriptor, handle);
	if(status != status::StatusInternal::OK){
		LOG (ERROR)<< "Failed to close local file : \"" << path << "\"; operation status : " << status << "\n";
	}
	managed_file->close(); // close the reference to file as a "client"
	// from registry:
	ret = CacheLayerRegistry::instance()->deleteFile(fsDescriptor, path);
	if (!ret) {
		LOG (ERROR)<< "Failed to clean the file : \"" << path << "\" from LRU registry." << "\n";
	}
	return NULL;
}

/**
 * Handle "open file for read/create file" scenario
 *
 * @param [in]  fsDescriptor - filesystem descriptor
 * @param [in]  path         - file path
 * @param [in]  bufferSize   - buffer size
 * @param [in]  replication  - replication (hdfs-only relevant)
 * @param [in]  blockSize    - block size
 * @param [out] available    - flag, indicates whether the file is available after all
 *
 * @return local (cached) file handle
 */
static dfsFile openForReadOrCreate(const FileSystemDescriptor & fsDescriptor, const char* path,
		int flags, int bufferSize, short replication, tSize blocksize, bool& available){

	Uri uri = Uri::Parse(path);

	managed_file::File* managed_file;
	// first check whether the file is already in the registry.
	// for now, when the autoload is the default behavior, we return immediately if we found that there's no file exist
	// in the registry or it happens to be retrieved in a forbidden/near-to-be-deleted state:
	if (!CacheLayerRegistry::instance()->findFile(uri.FilePath.c_str(),
			fsDescriptor, managed_file) || managed_file == nullptr
			|| !managed_file->valid()) {
		LOG (ERROR)<< "File \"/" << "/" << path << "\" is not available either on target or locally." << "\n";
		return NULL; // return plain NULL to support past-c++0x
	}

	boost::condition_variable* condition;
	boost::mutex* mux;

	// subscribe for file status updates if file is under sync just now:
	if ((managed_file->state() == managed_file::State::FILE_IS_IN_USE_BY_SYNC)){
		if(!managed_file->subscribe_for_updates(condition, mux)){
			LOG (ERROR) << "Failed to subscribe for file \"" << path << "\" status updates, unable to proceed." << "\n";
			managed_file->close();
			return NULL;
		}
		// wait for sync is completed:
		boost::unique_lock<boost::mutex> lock(*mux);
		(*condition).wait(lock,
				[&] {return managed_file->state() != managed_file::State::FILE_IS_IN_USE_BY_SYNC;});
		lock.unlock();

		// un-subscribe from updates (and further file usage), safe here as the file is "opened" or will not be used more
		managed_file->unsubscribe_from_updates();
    }

	// so as the file is available locally, just open it:
	if (!managed_file->exists()) {
		// and reply no data available otherwise
		LOG (ERROR)<< "File \"" << path << "\" is not available locally." << "\n";
		managed_file->close();
		return NULL;
	}

	dfsFile handle = filemgmt::FileSystemManager::instance()->dfsOpenFile(
			fsDescriptor, uri.FilePath.c_str(), flags, bufferSize, replication,
			blocksize, available);
	if (handle != nullptr && available) {
		// file is available locally, just reply it back:
		LOG (INFO) << "dfsOpenFile() : \"" << path << "\" is opened successfully.";
		return handle;
	}
	LOG (ERROR)<< "File \"" << path << "\" is not available" << "\n";
	// no close will be performed on non-successful open
	managed_file->close();
	return handle;
}

dfsFile dfsOpenFile(const FileSystemDescriptor & fsDescriptor, const char* path, int flags,
		int bufferSize, short replication, tSize blocksize, bool& available) {
	LOG (INFO) << "dfsOpenFile() begin : file path \"" << path << "\"." << "\n";

	// check whether the file is opened for write:
    if(flags == O_WRONLY){
       return openForWrite(fsDescriptor, path, bufferSize, replication, blocksize, available);
    }
    return openForReadOrCreate(fsDescriptor, path, flags, bufferSize, replication, blocksize, available);
}

static status::StatusInternal handleCloseFileInWriteMode(const FileSystemDescriptor & fsDescriptor, dfsFile file,
		managed_file::File* managed_file){
	// First check the scenario, if one is "CREATE FROM SELECT", extra actions are required:
	bool available;

	dfsFile hfile = CacheLayerRegistry::instance()->getCreateFromSelectScenario(file, available);
	if(hfile == NULL || !available){
       return status::StatusInternal::NO_STATUS;
	}

	// assign the estimated size as the local file size.
	// TODO : more efficiently this may be done on each file write operation.
	// To avoid retrieving managed file on each write from registry, managed file reference may be
	// preserved in "create from select scenario" along with both file handles, local and remote
	managed_file->estimated_size(managed_file->size());

	LOG (INFO) << "dfsCloseFile() is requested for file write operation." << "\n";

	// locate the remote filesystem adaptor:
	boost::shared_ptr<FileSystemDescriptorBound> fsAdaptor = (*CacheLayerRegistry::instance()->getFileSystemDescriptor(fsDescriptor));
	if (fsAdaptor == nullptr) {
		LOG (ERROR)<< "No filesystem adaptor configured for FileSystem \"" << fsDescriptor.dfs_type << ":" <<
		fsDescriptor.host << "\"" << "\n";
		// no namenode adaptor configured
		return status::StatusInternal::DFS_ADAPTOR_IS_NOT_CONFIGURED;
	}

	raiiDfsConnection connection(fsAdaptor->getFreeConnection());
	if (!connection.valid()) {
		LOG (ERROR)<< "No connection to dfs available, unable to close file for write on FileSystem \"" << fsDescriptor.dfs_type << ":" <<
				fsDescriptor.host << "\"" << "\n";
		return status::StatusInternal::DFS_NAMENODE_IS_NOT_REACHABLE;
	}
    int ret = fsAdaptor->fileClose(connection, hfile);
    if(ret != 0){
    	LOG (ERROR) << "Failed to close file for write on FileSystem \"" << fsDescriptor.dfs_type << ":" <<
    			fsDescriptor.host << "\"" << "\n";
    	return status::StatusInternal::DFS_OBJECT_OPERATION_FAILURE;
    }
	ret = CacheLayerRegistry::instance()->unregisterCreateFromSelectScenario(file);

	return status::StatusInternal::OK;
}

status::StatusInternal dfsCloseFile(const FileSystemDescriptor & fsDescriptor, dfsFile file) {
	LOG (INFO) << "dfsCloseFile()" << "\n";

	managed_file::File* managed_file;
	status::StatusInternal status = status::StatusInternal::NO_STATUS;

	std::string path = filemgmt::FileSystemManager::filePathByDescriptor(file);
	if(!CacheLayerRegistry::instance()->findFile(path.c_str(), managed_file) || managed_file == nullptr){
		status = status::StatusInternal::CACHE_OBJECT_NOT_FOUND;
	}

	status = handleCloseFileInWriteMode(fsDescriptor, file, managed_file);

    if(path.empty()){
    	status = status::StatusInternal::DFS_OBJECT_DOES_NOT_EXIST;
    	LOG (WARNING) << "File descriptor is not resolved within the system!" << "\n";
    }
    LOG (INFO) << "dfsCloseFile() is going to close file \"" << path << "\"." << "\n";

    // anyway try close the file
    status = filemgmt::FileSystemManager::instance()->dfsCloseFile(fsDescriptor, file);

	// if no file path resolved from the file descriptor, no chance to find it in the cache.
	if(path.empty())
		return status;

	if( managed_file != nullptr)
		// unbind reference as a client - from preceding "file open()" scenario ("i hold reference as a client who opened the file")
		// and one is from local "find file" scenario which, on success, auto-open the file to save it from deletion
		managed_file->close(2); // this will still leave 1 internal reference on the file as a underlying LRU collection item

	return status;
}

status::StatusInternal dfsExists(const FileSystemDescriptor & fsDescriptor, const char *path, bool* exists) {
	*exists = false;

	// try look for file remotely:
	// locate the remote filesystem adapter:
	boost::shared_ptr<FileSystemDescriptorBound> fsAdaptor = (*CacheLayerRegistry::instance()->getFileSystemDescriptor(fsDescriptor));
	if(fsAdaptor == nullptr){
		LOG (ERROR) << "No filesystem adaptor configured for FileSystem \"" << fsDescriptor.dfs_type << ":" <<
				fsDescriptor.host << "\"" << "\n";
		// no namenode adaptor configured
		return status::StatusInternal::DFS_ADAPTOR_IS_NOT_CONFIGURED;
	}

    raiiDfsConnection connection(fsAdaptor->getFreeConnection());
    if(!connection.valid()) {
    	LOG (ERROR) << "No connection to dfs available, unable to create file for write on FileSystem \"" << fsDescriptor.dfs_type << ":" <<
    			fsDescriptor.host << "\"" << "\n";
    	return status::StatusInternal::DFS_NAMENODE_IS_NOT_REACHABLE;
    }

    bool ret = fsAdaptor->pathExists(connection, path);
	if(ret){
		LOG (INFO) << "Path \"" << path << "\" exists on FileSystem \""
				<< fsDescriptor.dfs_type << "://" << fsDescriptor.host << "\"" << "\n";
		*exists = true;
	}
	return status::StatusInternal::OK;
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
	// First check the scenario, if one is "CREATE FROM SELECT", extra actions are required:
	bool available;

	dfsFile hfile = CacheLayerRegistry::instance()->getCreateFromSelectScenario(file, available);
	if(hfile == NULL || !available){
		LOG (ERROR) << "File write is invoked for non-existing WRITE scenario." << "\n";
		return -1;
	}

	// locate the remote filesystem adapter:
	boost::shared_ptr<FileSystemDescriptorBound> fsAdaptor = (*CacheLayerRegistry::instance()->getFileSystemDescriptor(fsDescriptor));
	if(fsAdaptor == nullptr){
		LOG (ERROR) << "No filesystem adaptor configured for FileSystem \"" << fsDescriptor.dfs_type << ":" <<
				fsDescriptor.host << "\"" << "\n";
		// no namenode adaptor configured
		return -1;
	}

    raiiDfsConnection connection(fsAdaptor->getFreeConnection());
    if(!connection.valid()) {
    	LOG (ERROR) << "No connection to dfs available, unable to create file for write on FileSystem \"" << fsDescriptor.dfs_type << ":" <<
    			fsDescriptor.host << "\"" << "\n";
    	return -1;
    }

    tSize remotebytes_written = fsAdaptor->fileWrite(connection, hfile, buffer, length);
    if(remotebytes_written == -1){
    	LOG (ERROR) << "Failed to write into remote file. " << "\n";
    }

    tSize localbytes_written = filemgmt::FileSystemManager::instance()->dfsWrite(fsDescriptor, file, buffer, length);
    if(localbytes_written == -1){
    	LOG (ERROR) << "Failed to write into local file. " << "\n";
    }
    return localbytes_written;
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
	LOG (INFO) << "dfsDelete() : path = \"" << path << "\"\n";

	// Remove the file from registry if it is there:
	Uri uri = Uri::Parse(path);

	if (!CacheLayerRegistry::instance()->deletePath(fsDescriptor, uri.FilePath.c_str())){
		LOG (ERROR) << "Path \"" << path << "\" was not deleted from registry." << "\n";
		return status::StatusInternal::CACHE_OBJECT_OPERATION_FAILURE;
	}

	LOG (INFO) << "Path \"" << path << "\" successfully deleted from registry." << "\n";

	// locate the remote filesystem adapter:
	boost::shared_ptr<FileSystemDescriptorBound> fsAdaptor = (*CacheLayerRegistry::instance()->getFileSystemDescriptor(fsDescriptor));
	if(fsAdaptor == nullptr){
		LOG (ERROR) << "No filesystem adaptor configured for FileSystem \"" << fsDescriptor.dfs_type << ":" <<
				fsDescriptor.host << "\"" << "\n";
		// no namenode adaptor configured
		return status::StatusInternal::DFS_ADAPTOR_IS_NOT_CONFIGURED;
	}

    raiiDfsConnection connection(fsAdaptor->getFreeConnection());
    if(!connection.valid()) {
    	LOG (ERROR) << "No connection to dfs available, unable to delete file from FileSystem \"" << fsDescriptor.dfs_type << "://" <<
    			fsDescriptor.host << "\"" << "\n";
    	return status::StatusInternal::DFS_NAMENODE_IS_NOT_REACHABLE;
    }
    int ret = fsAdaptor->pathDelete(connection, path, recursive);
    if(ret != 0){
    	LOG (WARNING) << "Negative server reply received when trying to delete remote path \"" << path << "\" from FileSystem \""
    			<< fsDescriptor.dfs_type << "://" << fsDescriptor.host << "\"" << "\n";

    	ret = fsAdaptor->pathExists(connection, path);
    	if(ret == 0){
    		LOG (WARNING) << "Path assigned for removal still exists on remote part : \"" << path << "\" on FileSystem \""
    				<< fsDescriptor.dfs_type << "://" << fsDescriptor.host << "\"" << "\n";

    	}
    	// as discussed, don't stuck here if remote part respond with negative
    	return status::StatusInternal::OK;
    }

    LOG (INFO) << "dfsDelete() : succeed for path = \"" << path << "\"\n";
    return status::StatusInternal::OK;
}

status::StatusInternal dfsRename(const FileSystemDescriptor & fsDescriptor, const char* oldPath,
		const char* newPath) {
	LOG (INFO) << "dfsRename() : \"" << oldPath << "\" to \"" << newPath << "\".\n";

	Uri uriOld = Uri::Parse(oldPath);
	Uri uriNew = Uri::Parse(newPath);

	// drop old file from registry. Instruction below just clean the file reference from registry, without physical affect
	if(!CacheLayerRegistry::instance()->deleteFile(fsDescriptor, uriOld.FilePath.c_str(), false)){
		LOG (WARNING) << "Failed to delete old temp file \"" << oldPath << "\" from cache.\n";
	}

	// locate the remote filesystem adaptor:
	boost::shared_ptr<FileSystemDescriptorBound> fsAdaptor = (*CacheLayerRegistry::instance()->getFileSystemDescriptor(fsDescriptor));
	if (fsAdaptor == nullptr) {
		LOG (ERROR)<< "No filesystem adaptor configured for FileSystem \"" << fsDescriptor.dfs_type << ":" <<
		fsDescriptor.host << "\"" << "\n";
		// no namenode adaptor configured
		return status::StatusInternal::DFS_ADAPTOR_IS_NOT_CONFIGURED;
	}

	raiiDfsConnection connection(fsAdaptor->getFreeConnection());
	if (!connection.valid()) {
		LOG (ERROR)<< "No connection to dfs available, unable to rename the file \"" << oldPath << "\" on FileSystem \""
				<< fsDescriptor.dfs_type << ":" << fsDescriptor.host << "\"" << "\n";
		return status::StatusInternal::DFS_NAMENODE_IS_NOT_REACHABLE;
	}

	// rename remote file:
    int ret = fsAdaptor->fileRename(connection, oldPath, newPath);

    if(ret != 0){
    	LOG (ERROR) << "Failed to rename file \"" << oldPath << "\" on FileSystem \"" << fsDescriptor.dfs_type << ":" <<
    			fsDescriptor.host << "\"" << "\n";
    	return status::StatusInternal::DFS_OBJECT_OPERATION_FAILURE;
    }

    // rename local file:
    status::StatusInternal status = filemgmt::FileSystemManager::instance()->dfsRename(fsDescriptor,
    		uriOld.FilePath.c_str(), uriNew.FilePath.c_str());
    if(status != status::StatusInternal::OK){
    	LOG (ERROR) << "Failed to rename \"" << oldPath << "\" to \"" << newPath << "\" on local filesystem." << "\n";
    	return status;
    }

    // create new one file in the registry, renamed.
	managed_file::File* managed_file;
	if(!CacheLayerRegistry::instance()->addFile(uriNew.FilePath.c_str(), fsDescriptor, managed_file)){
		LOG (ERROR) << "Unable to add the file to the LRU registry for FileSystem \"" << fsDescriptor.dfs_type << ":" <<
				fsDescriptor.host << "\"" << "\n";
		return status::StatusInternal::CACHE_OBJECT_OPERATION_FAILURE;
	}
    return status;
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
	LOG (INFO) << "getPathInfo() for \"" << path << "\".\n";
	// We always get statistics from remote side:
	boost::shared_ptr<FileSystemDescriptorBound> fsAdaptor = (*CacheLayerRegistry::instance()->getFileSystemDescriptor(fsDescriptor));
	if (fsAdaptor == nullptr) {
		LOG (ERROR)<< "No filesystem adaptor configured for FileSystem \"" << fsDescriptor.dfs_type << ":" <<
		fsDescriptor.host << "\"" << "\n";
		// no namenode adaptor configured
		return NULL;
	}

	raiiDfsConnection connection(fsAdaptor->getFreeConnection());
	if (!connection.valid()) {
		LOG (ERROR)<< "No connection to dfs available, unable get path info for file \"" << path << "\" on FileSystem \""
				<< fsDescriptor.dfs_type << ":" << fsDescriptor.host << "\"" << "\n";
		return NULL;
	}

	// get file statistics:
	dfsFileInfo* info = fsAdaptor->fileInfo(connection, path);

    if(info == NULL){
    	LOG (ERROR) << "Failed to retrieve file info for file \"" << path << "\" on FileSystem \"" << fsDescriptor.dfs_type << ":" <<
    			fsDescriptor.host << "\"" << "\n";
    }
    return info;
}

void dfsFreeFileInfo(const FileSystemDescriptor & fsDescriptor, dfsFileInfo *dfsFileInfo, int numEntries) {
	if(dfsFileInfo == NULL)
		return;

	// We delegate statistics cleanup to jni layer. Free file statistics:
	FileSystemDescriptorBound::freeFileInfo(dfsFileInfo, numEntries);
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
