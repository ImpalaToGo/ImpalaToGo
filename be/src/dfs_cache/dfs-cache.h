/** @file dfs-cache.h
 *  @brief Impala Cache layer API.
 *
 *  This module inherits the facilities of libhdfs (regarding file system operations, the only difference is that all file system operations
 *  are running locally now, not on remote DFS like with libhdfs) and accompanies them with with cache facilities (for details, see further)
 *
 *  Facilities:
 *  1. Publish the cache management APIs (prefixed with "cache").
 *  These APIs serve to give the ability to estimate and schedule caching operations, to subscribe / poll for such operations completion and status.
 *  Underlying access the requested DFS is implemented via configured DFS adaptors (Plugins).
 *
 *  2. Publish FileSystem API to work with files (prefixed with "dfs").
 *  TODO: prefix dfs is inherited from similar "hdfs" - from libhdfs.
 *  This prefix should be changed to "fs" to show that the requested API is running locally.
 *
 *  @date   Sep 29, 2014
 *  @author elenav
 */

#ifndef LIBDFS_CACHE_H_
#define LIBDFS_CACHE_H_

#include <boost/function.hpp>
#include <boost/bind.hpp>

#include "dfs_cache/common-include.hpp"
#include "dfs_cache/dfs-adaptor-factory.hpp"

/** @namespace impala */
namespace impala {

/** *********************************************************************************************
 * ***********************   APIs published by Cache Management.  *******************************
 * **********************************************************************************************
 */

/**
 * @fn StatusInternal cacheInit()
 * @brief Initialize the module and underlying mechanisms.
 *
 * @return Operation status.
 */
status::StatusInternal cacheInit();

/**
 * @fn StatusInternal cacheConfigureLocalStorage(const std::string& localpath)
 * @brief Configure local storage root path (cache location on the file system)
 *
 * @param [In] localpath - local filesystem path to the storage root
 * @return Operation status.
 */
status::StatusInternal cacheConfigureLocalStorage(const std::string& localpath);

/**
 * @fn StatusInternal StatusInternal cacheConfigureDFSPluginFactory(const boost::shared_ptr<dfsAdaptorFactory>& dfsPluginFactory)
 * @brief Configure DFS adaptors (Plugins) factory
 *
 * @param [In] factory  - DFS plugins(adaptors) factory, the source of DFS Plugins, one plugin per dfs type.
 *
 * @return operation status
 */
status::StatusInternal cacheConfigureDFSPluginFactory(const boost::shared_ptr<dfsAdaptorFactory>& factory);

/**
 * @fn StatusInternal cacheConfigureNameNode(const NameNodeDescriptor & adaptor)
 * @brief Configure DFS NameNode (with connection details)
 *
 * @param [In] adaptor  - DFS Namenode connection details
 *
 * @return operation status
 */
status::StatusInternal cacheConfigureNameNode(const NameNodeDescriptor & adaptor);

/**
 * @fn Status cacheShutdown(bool force = true)
 * @brief Shutdown the cache management layer and all its underlying workers.
 *
 * Possible clients will be able to finish their work with the cache only if
 * @a force flag is specified as "false" (which is non-default)
 *
 * @param [In] force         - flag, indicates whether all operations in progress should be force aborted.
 * @param [In] updateClients - flag, indicates whether clients should be force updated
 *
 * @return Operation status
 */
status::StatusInternal cacheShutdown(bool force = true, bool updateClients = true);

/**
 * @fn status::StatusInternal cacheEstimate(SessionContext session, const NameNodeDescriptor & namenode,
		const std::list<const char*>& files, time_t& time,
		CacheEstimationCompletedCallback callback, bool async = true)

 * @brief For files from the list @a files, check whether all files can be accessed locally,
 *        estimate the time required to get them all locally - if any specified
 *        file is not available locally yet.
 *
 * @param [In]  session     - Request session id.
 * @param [In]  namenode    - namenode descriptor, connection details
 * @param [In]  files       - List of files required to be locally.
 * @param [Out] time        - time required to get all requested files locally (if any).
 * Zero time means all data is in place (if the operation status is "OK")
 *
 * @param [In]  callback        - callback that should be invoked on completion in case if async mode is selected.
 * @param [Out] requestIdentity - request identity assigned to this request, should be used to poll it for progress later.
 * @param [In]  async           - if true, the callback should be passed as well in order to be called on the operation completion.
 * @return Operation status.
 * In async call, in case of successful operation scheduling, the status should be "OPERATION_ASYNC_SCHEDULED".
 *
 * In sync call. If either file is not available in specified @a namenode, the status will be "DFS_OBJECT_DOES_NOT_EXIST".
 * The only accepted result of sync call here is "OK", all other statuses should be treated as failure.
 */
status::StatusInternal cacheEstimate(SessionContext session, const NameNodeDescriptor & namenode,
		const DataSet& files, time_t& time,
		CacheEstimationCompletedCallback callback, requestIdentity & requestIdentity, bool async = true);

/**
 * @fn status::StatusInternal cachePrepareData(SessionContext session, const NameNodeDescriptor & namenode,
		const std::list<const char*>& files, PrepareCompletedCallback callback)

 * @brief Run load scenario for specified files list @a files from the @a namenode.
 * This is async operation.
 *
 * @param [In]  session     - Request session id.
 * @param [In]  namenode    - namenode connection details
 * @param [In]  files       - List of files required to be locally.
 * @param [Out] callback    - callback to invoke when prepare is finished (whatever the status).
 *
 * @param [Out] requestIdentity - request identity assigned to this request, should be used to poll it for progress later.
 *
 * @return Operation status
 */
status::StatusInternal cachePrepareData(SessionContext session, const NameNodeDescriptor & namenode,
		const DataSet& files, PrepareCompletedCallback callback, requestIdentity & requestIdentity);

/**
 * @fn Status cacheCancelPrepareData(SessionContext session) *
 * @brief cancel prepare data request
 *
 * @param [In] requestIdentity - request identity assigned to this request
 *
 * @return Operation status
 */
status::StatusInternal cacheCancelPrepareData(const requestIdentity & requestIdentity);

/**
 * @fn status::StatusInternal cacheCheckPrepareStatus(SessionContext session,
		std::list<FileProgress*>& progress, request_performance& performance)

 * @brief Check the previously scheduled "Prepare" operation status.
 * This is sync operation.
 *
 * @param [In] requestIdentity - request identity assigned to this request
 * @param [Out]  progress    - Detailed prepare progress. Can be used to present it to the user.
 * @param [Out]  performance - to hold request current performance statistic
 *
 * @return Operation status
 */
status::StatusInternal cacheCheckPrepareStatus(const requestIdentity & requestIdentity,
		std::list<boost::shared_ptr<FileProgress> >& progress, request_performance& performance);


/** *********************************************************************************************
 * ***********************   APIs to work with files. Inherited from libhdfs  *******************
 * **********************************************************************************************
 */

/**
 * @fn  dfsOpenFile(const NameNodeDescriptor & namenode, const char* path, int flags,
		int bufferSize, short replication, tSize blocksize, bool& available)

 * @brief Open the file in given mode.This will be done locally but @a namenode is required
 * for path resolution.
 *
 * @param [In] namenode  - The configured filesystem handle.
 * @param [In] path      - The full path to the file.
 * @param [In] flags     - an | of bits/fcntl.h file flags - supported flags are O_RDONLY, O_WRONLY (meaning create or overwrite i.e., implies O_TRUNCAT),
 * O_WRONLY|O_APPEND. Other flags are generally ignored other than (O_RDWR || (O_EXCL & O_CREAT)) which return NULL and set errno equal ENOTSUP.
 *
 * @param [In] bufferSize  - Size of buffer for read/write - pass 0 if you want to use the default configured values.
 * @param [In] replication - Block replication - pass 0 if you want to use the default configured values.
 * @param [In] blocksize   - Size of block - pass 0 if you want to use the default configured values.
 *
 * @param [In/Out] available - flag, indicates whether the requested file is available.
 *
 * @return Returns the handle to the open file or NULL on error.
 */
dfsFile dfsOpenFile(const NameNodeDescriptor & namenode, const char* path, int flags,
		int bufferSize, short replication, tSize blocksize, bool& available);

/**
 * @fn status::StatusInternal dfsCloseFile(const NameNodeDescriptor & namenode, dfsFile file)
 *
 * @brief Close an opened file. File is always local. Namenode parameter is not needed and should be removed.
 * Cuurently it is just ignored.
 * TODO: remove namenode parameter.
 *
 * @param namenode  - The configured filesystem handle.
 * @param file      - The file handle.
 *
 * @return Operation status.
 */
status::StatusInternal dfsCloseFile(const NameNodeDescriptor & namenode, dfsFile file);

/**
 * @fn status::StatusInternal dfsExists(const NameNodeDescriptor & namenode, const char *path)
 * @brief Checks if a given path exists. In past, this check was done on remote dfs,
 * now all file-related operations are performed locally. Therefore, for clients of this API the
 * usage semantic should be checked.
 *
 * @param namenode  - Namenode connection details, may be need to locate the file locally.
 *                    Check what we receive in "path" here.
 * @param path      - The path to look for
 *
 * @return Operation status
 */
status::StatusInternal dfsExists(const NameNodeDescriptor & namenode, const char *path);

/**
 * @fn status::StatusInternal dfsSeek(const NameNodeDescriptor & namenode, dfsFile file, tOffset desiredPos)
 * @brief Seek to given offset in file. This works only for files opened in read-only mode.
 *
 * @param namenode   - namenode descriptor
 * @param file       - The file handle.
 * @param desiredPos - Offset into the file to seek into.
 *
 * @return Operation status
 */
status::StatusInternal dfsSeek(const NameNodeDescriptor & namenode, dfsFile file, tOffset desiredPos);

/**
 * @fn ttOffset dfsTell(const NameNodeDescriptor & namenode, dfsFile file)
 * @brief Get the current offset in the file, in bytes.
 *
 * @param namenode - original dfs namenode
 * @param file     - The file handle.
 *
 * @return Current offset, -1 on error.
 */
tOffset dfsTell(const NameNodeDescriptor & namenode, dfsFile file);

/**
 * @fn tSize dfsRead(const NameNodeDescriptor & namenode, dfsFile file, void* buffer, tSize length)
 * @brief Read data from an open file.
 *
 * @param namenode - file's namenode
 * @param file     - The file handle.
 * @param buffer   - The buffer to copy read bytes into.
 * @param length   - The length of the buffer.
 *
 * @return Returns the number of bytes actually read, possibly less than than length;
 * -1 on error.
 */
tSize dfsRead(const NameNodeDescriptor & namenode, dfsFile file, void* buffer, tSize length);

/**
 * @fn tSize dfsPread(const NameNodeDescriptor & namenode, tOffset position, void* buffer, tSize length)
 * @brief Positional read of data from an open file.
 *
 * @param namenode - file's original namenode
 * @param file     - The file handle.
 * @param position - Position from which to read
 * @param buffer   - The buffer to copy read bytes into.
 * @param length   - The length of the buffer.
 *
 * @return Returns the number of bytes actually read, possibly less than
 * than length;-1 on error.
 */
tSize dfsPread(const NameNodeDescriptor & namenode, tOffset position, void* buffer, tSize length);

/**
 * @fn tSize dfsWrite(const NameNodeDescriptor & namenode, dfsFile file, const void* buffer, tSize length)
 * @brief Write data into an open file.
 *
 * @param namenode - namenode
 * @param file     - The file handle.
 * @param buffer   - The data.
 * @param length   - The no. of bytes to write.
 *
 * @return Returns the number of bytes written, -1 on error.
 */
tSize dfsWrite(const NameNodeDescriptor & namenode, dfsFile file, const void* buffer, tSize length);

/**
 * @fn status::StatusInternal dfsFlush(const NameNodeDescriptor & namenode, dfsFile file)
 * @brief Flush the data.
 *
 * @param namenode - namenode file belongs to
 * @param file     - The file handle.
 *
 * @return Operation status
 */
status::StatusInternal dfsFlush(const NameNodeDescriptor & namenode, dfsFile file);

/**
 * @fn status::StatusInternal dfsHFlush(const NameNodeDescriptor & namenode, dfsFile file)
 * @brief Flush out the data in client's user buffer. After the
 * return of this call, new readers will see the data.
 *
 * @param namenode - namenode file belongs to
 * @param file - file handle
 *
 * @return Operation status
 */
status::StatusInternal dfsHFlush(const NameNodeDescriptor & namenode, dfsFile file);

/**
 * @fn int dfsAvailable(dfsFile file)
 * @brief Number of bytes that can be read from this input stream without blocking.
 * TODO: remove this comment when DFS adaptors are designed.
 * Comment: Useful function to estimate file readiness and progress in "Prepare"
 *
 * @param namenode - namenode file belongs to
 * @param file - The file handle.
 *
 * @return Returns available bytes; -1 on error.
 */
tOffset dfsAvailable(const NameNodeDescriptor & namenode, dfsFile file);

/**
 * @fn status::StatusInternal dfsCopy(const NameNodeDescriptor & namenode1, const char* src, const NameNodeDescriptor & namenode2,
		const char* dst)
 * @brief Copy file from one filesystem to another.
 * Is available inside single cluster (because of credentials only)
 *
 * @param namenode - namenode file belongs to
 * @param src      - The path of source file.
 * @param dst      - The path of destination file.
 *
 * @return Operation status
 */
status::StatusInternal dfsCopy(const NameNodeDescriptor & namenode1, const char* src, const NameNodeDescriptor & namenode2,
		const char* dst);

/**
 * @fn status::StatusInternal dfsCopy(const NameNodeDescriptor & namenode, const char* src, const char* dst)
 * @brief Copy file within filesystem
 * Is available inside single cluster (because of credentials only)
 *
 * @param namenode - namenode file belongs to
 * @param src      - The path of source file.
 * @param dst      - The path of destination file.
 *
 * @return Returns 0 on success, -1 on error.
 */
status::StatusInternal dfsCopy(const NameNodeDescriptor & namenode, const char* src, const char* dst);

/**
 * @fn status::StatusInternal dfsMove(const NameNodeDescriptor & namenode, const char* src, const char* dst)
 * @brief Move file from one filesystem to another.
 * Is available inside single cluster (because of credentials only)
 *
 * @param namenode - namenode file belongs to
 * @param src      - The path of source file.
 * @param dst      - The path of destination file.
 *
 * @return Operation status
 */
status::StatusInternal dfsMove(const NameNodeDescriptor & namenode, const char* src, const char* dst);

/**
 * @fn status::StatusInternal dfsDelete(const NameNodeDescriptor & namenode, const char* path, int recursive)
 * @brief Delete file.
 *
 * @param namenode  - namenode file belongs to
 * @param path      - The path of the file/folder.
 * @param recursive - if path is a directory and set to
 * non-zero, the directory is deleted else throws an exception. In
 * case of a file the recursive argument is irrelevant.
 *
 * @return Operation status
 */
status::StatusInternal dfsDelete(const NameNodeDescriptor & namenode, const char* path, int recursive);

/**
 * @fn status::StatusInternal dfsRename(const NameNodeDescriptor & namenode, const char* oldPath,
		const char* newPath)
 * @brief Rename the file.
 *
 * @param namenode  - namenode file belongs to
 * @param oldPath   - The path of the source file.
 * @param newPath   - The path of the destination file.
 *
 * @return Operation status
 */
status::StatusInternal dfsRename(const NameNodeDescriptor & namenode, const char* oldPath,
		const char* newPath);

/**
 * @fn status::StatusInternal dfsCreateDirectory(const NameNodeDescriptor & namenode, const char* path)
 * @brief Make the given file and all non-existent
 * parents into directories.
 *
 * @param namenode  - namenode file belongs to
 * @param path      - The path of the directory.
 *
 * @return Returns 0 on success, -1 on error.
 */
status::StatusInternal dfsCreateDirectory(const NameNodeDescriptor & namenode, const char* path);

/**
 * @fn status::StatusInternal dfsSetReplication(const NameNodeDescriptor & namenode, const char* path, int16_t replication)
 * @brief Set the replication of the specified
 * file to the supplied value
 *
 * @param namenode  - namenode file belongs to
 * @param path - The path of the file.
 *
 * @return Operation status
 */
status::StatusInternal dfsSetReplication(const NameNodeDescriptor & namenode, const char* path, int16_t replication);

/**
 * @fn dfsFileInfo *dfsListDirectory(const NameNodeDescriptor & namenode, const char* path,
		int *numEntries)
 * @brief Get list of files/directories for a given
 * directory-path. dfsFreeFileInfo should be called to deallocate memory.
 *
 * @param namenode   - namenode file belongs to
 * @param path       - The path of the directory.
 * @param numEntries -  Set to the number of files/directories in path.
 *
 * @return Returns a dynamically-allocated array of dfsFileInfo
 * objects; NULL on error.
 */
dfsFileInfo *dfsListDirectory(const NameNodeDescriptor & namenode, const char* path,
		int *numEntries);

/**
 * @fn dfsFileInfo *dfsGetPathInfo(const NameNodeDescriptor & namenode, const char* path)
 * @brief Get information about a path as a (dynamically
 * allocated) single dfsFileInfo struct. dfsFreeFileInfo should be
 * called when the pointer is no longer needed.
 *
 * @param namenode - namenode file belongs to
 * @param path     - The path of the file.
 *
 * @return Returns a dynamically-allocated dfsFileInfo object;
 * NULL on error.
 */
dfsFileInfo *dfsGetPathInfo(const NameNodeDescriptor & namenode, const char* path);

/**
 * @fn void dfsFreeFileInfo(const NameNodeDescriptor & namenode, dfsFileInfo *dfsFileInfo, int numEntries)
 * @brief Free up the dfsFileInfo array (including fields)
 *
 * @param namenode    - namenode file belongs to
 * @param dfsFileInfo - The array of dynamically-allocated dfsFileInfo
 * objects.
 *
 * @param numEntries The size of the array.
 */
void dfsFreeFileInfo(const NameNodeDescriptor & namenode, dfsFileInfo *dfsFileInfo, int numEntries);

/**
 * @fn tOffset dfsGetCapacity(const NameNodeDescriptor & namenode, const char* host)
 * @brief Return the raw capacity of the local filesystem.
 *
 * @param namenode - namenode file belongs to
 * @param host     - hostname
 *
 * @return Returns the raw-capacity; -1 on error.
 */
tOffset dfsGetCapacity(const NameNodeDescriptor & namenode, const char* host);

/**
 * @fn tOffset dfsGetUsed(const NameNodeDescriptor & namenode, const char* host)
 * Return the total raw size of all files in the filesystem.
 *
 * @param namenode - namenode file belongs to
 * @param host     - hostname
 *
 * @return Returns the total-size; -1 on error.
 */
tOffset dfsGetUsed(const NameNodeDescriptor & namenode, const char* host);

/**
 * @fn status::StatusInternal dfsChown(const NameNodeDescriptor & namenode, const char* path,
		const char *owner, const char *group)
 * @brief Change owner of the specified path
 *
 * @param namenode - configured namenode.
 * @param path     - the path to the file or directory
 * @param owner    - Set to null or "" if only setting group
 * @param group    - Set to null or "" if only setting user
 * @return Operation status
 */
status::StatusInternal dfsChown(const NameNodeDescriptor & namenode, const char* path,
		const char *owner, const char *group);

/**
 * @fn status::StatusInternal dfsChmod(const NameNodeDescriptor & namenode, const char* path, short mode)
 * @brief Change mode of specified path @a path within the specified @a cluster
 *
 * @param namenode - configured namenode
 * @param path     - the path to the file or directory
 * @param mode     - the bitmask to set it to
 *
 * @return Operation status
 */
status::StatusInternal dfsChmod(const NameNodeDescriptor & namenode, const char* path, short mode);

/**
 * @fn status::StatusInternal dfsChown(const NameNodeDescriptor & namenode, const char* path,
		const char *owner, const char *group)
 * @brief Get read statistics about a file.  This is only applicable to files
 * opened for reading.
 *
 * @param namenode - configured namenode
 * @param file     - The HDFS file
 * @param stats    - (out parameter) on a successful return, the read
 *                 statistics.  Unchanged otherwise.  You must free the
 *                 returned statistics with dfsFileFreeReadStatistics.
 *
 * @return         0 if the statistics were successfully returned,
 *                 -1 otherwise.  On a failure, please check errno against
 *                 ENOTSUP.  webhdfs, LocalFilesystem, and so forth may
 *                 not support read statistics.
 */
int dfsFileGetReadStatistics(const NameNodeDescriptor & namenode,
		dfsFile file,
		struct dfsReadStatistics **stats);

/**
 * @fn status::StatusInternal dfsChown(const NameNodeDescriptor & namenode, const char* path,
		const char *owner, const char *group)
 * @brief Get read statistics about a file.  This is only applicable to files
 * @param stats    HDFS read statistics for a file.
 *
 * @return the number of remote bytes read.
 */
int64_t dfsReadStatisticsGetRemoteBytesRead(const struct dfsReadStatistics *stats);

/**
 * Free some HDFS read statistics.
 *
 * @param namenode - configured namenode
 * @param stats    - the HDFS read statistics to free.
 */
void dfsFileFreeReadStatistics(const NameNodeDescriptor & namenode, struct dfsReadStatistics *stats);
}

#endif /* LIBDFS_CACHE_H_ */
