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
 * @param root - local cache root - absoulte filesystem path.
 * @return Operation status.
 */
status::StatusInternal cacheInit(const std::string& root = "");

/**
 * @fn StatusInternal cacheConfigureNameNode(const FileSystemDescriptor & adaptor)
 * @brief Configure DFS NameNode (with connection details)
 *
 * @param [In] fs - FS connection details
 *
 * @return operation status
 */
status::StatusInternal cacheConfigureFileSystem(FileSystemDescriptor & fs);

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
 * @fn status::StatusInternal cacheEstimate(SessionContext session, const FileSystemDescriptor & fs,
		const std::list<const char*>& files, time_t& time,
		CacheEstimationCompletedCallback callback, bool async = true)

 * @brief For files from the list @a files, check whether all files can be accessed locally,
 *        estimate the time required to get them all locally - if any specified
 *        file is not available locally yet.
 *
 * @param [In]  session      - Request session id.
 * @param [In]  fsDescriptor - file system descriptor, connection details
 * @param [In]  files        - List of files required to be locally.
 * @param [Out] time         - time required to get all requested files locally (if any).
 * 							   Zero time means all data is in place (if the operation status is "OK")
 *
 * @param [In]  callback        - callback that should be invoked on completion in case if async mode is selected.
 * @param [Out] requestIdentity - request identity assigned to this request, should be used to poll it for progress later.
 * @param [In]  async           - if true, the callback should be passed as well in order to be called on the operation completion.
 * @return Operation status.
 * In async call, in case of successful operation scheduling, the status should be "OPERATION_ASYNC_SCHEDULED".
 *
 * In sync call. If either file is not available in specified @a fsDescriptor, the status will be "DFS_OBJECT_DOES_NOT_EXIST".
 * The only accepted result of sync call here is "OK", all other statuses should be treated as failure.
 */
status::StatusInternal cacheEstimate(SessionContext session, const FileSystemDescriptor & fsDescriptor,
		const DataSet& files, time_t& time,
		CacheEstimationCompletedCallback callback, requestIdentity & requestIdentity, bool async = true);

/**
 * @fn status::StatusInternal cachePrepareData(SessionContext session, const FileSystemDescriptor & fsDescriptor,
		const std::list<const char*>& files, PrepareCompletedCallback callback)

 * @brief Run load scenario for specified files list @a files from the @a fsDescriptor.
 * This is async operation.
 *
 * @param [In]  session      - Request session id.
 * @param [In]  fsDescriptor - file system connection details
 * @param [In]  files        - List of files required to be locally.
 * @param [Out] callback     - callback to invoke when prepare is finished (whatever the status).
 *
 * @param [Out] requestIdentity - request identity assigned to this request, should be used to poll it for progress later.
 *
 * @return Operation status
 */
status::StatusInternal cachePrepareData(SessionContext session, const FileSystemDescriptor & fsDescriptor,
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
 * @fn  dfsOpenFile(const FileSystemDescriptor & fsDescriptor, const char* path, int flags,
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
dfsFile dfsOpenFile(const FileSystemDescriptor & fsDesciptor, const char* path, int flags,
		int bufferSize, short replication, tSize blocksize, bool& available);

/**
 * @fn status::StatusInternal dfsCloseFile(const FileSystemDescriptor & fsDescriptor, dfsFile file)
 *
 * @brief Close an opened file. File is always local. Namenode parameter is not needed and should be removed.
 * Cuurently it is just ignored.
 *
 * @param fsDescriptor  - The configured filesystem.
 * @param file          - The file handle.
 *
 * @return Operation status.
 */
status::StatusInternal dfsCloseFile(const FileSystemDescriptor & fsDescriptor, dfsFile file);

/**
 * @fn status::StatusInternal dfsExists(const FileSystemDescriptor & fsDescriptor, const char *path)
 * @brief Checks if a given path exists. In past, this check was done on remote dfs,
 * now all file-related operations are performed locally. Therefore, for clients of this API the
 * usage semantic should be checked.
 *
 * @param fsDescriptor  - FS connection details, may be need to locate the file locally.
 *                        Check what we receive in "path" here.
 * @param path          - The path to look for
 *
 * @return Operation status
 */
status::StatusInternal dfsExists(const FileSystemDescriptor & fsDescriptor, const char *path);

/**
 * @fn status::StatusInternal dfsSeek(const FileSystemDescriptor & namenode, dfsFile file, tOffset desiredPos)
 * @brief Seek to given offset in file. This works only for files opened in read-only mode.
 *
 * @param fsDescriptor - FS descriptor
 * @param file         - The file handle.
 * @param desiredPos   - Offset into the file to seek into.
 *
 * @return Operation status
 */
status::StatusInternal dfsSeek(const FileSystemDescriptor & fsDescriptor, dfsFile file, tOffset desiredPos);

/**
 * @fn ttOffset dfsTell(const FileSystemDescriptor & fsDescriptor, dfsFile file)
 * @brief Get the current offset in the file, in bytes.
 *
 * @param fsDescriptor - original dfs namenode
 * @param file         - The file handle.
 *
 * @return Current offset, -1 on error.
 */
tOffset dfsTell(const FileSystemDescriptor & fsDescriptor, dfsFile file);

/**
 * @fn tSize dfsRead(const FileSystemDescriptor & fsDescriptor, dfsFile file, void* buffer, tSize length)
 * @brief Read data from an open file.
 *
 * @param fsDescriptor - file's namenode
 * @param file         - The file handle.
 * @param buffer       - The buffer to copy read bytes into.
 * @param length       - The length of the buffer.
 *
 * @return Returns the number of bytes actually read, possibly less than than length;
 * -1 on error.
 */
tSize dfsRead(const FileSystemDescriptor & fsDescriptor, dfsFile file, void* buffer, tSize length);

/**
 * @fn tSize dfsPread(const FileSystemDescriptor & fsDescriptor, tOffset position, void* buffer, tSize length)
 * @brief Positional read of data from an open file.
 *
 * @param fsDescriptor - file's original fsDescriptor
 * @param file         - The file handle.
 * @param position     - Position from which to read
 * @param buffer       - The buffer to copy read bytes into.
 * @param length       - The length of the buffer.
 *
 * @return Returns the number of bytes actually read, possibly less than
 * than length;-1 on error.
 */
tSize dfsPread(const FileSystemDescriptor & fsDescriptor, tOffset position, void* buffer, tSize length);

/**
 * Write data into an open file.
 *
 * @param fsDescriptor - fs
 * @param file         - The file handle.
 * @param buffer       - The data.
 * @param length       - The no. of bytes to write.
 *
 * @return Returns the number of bytes written, -1 on error.
 */
tSize dfsWrite(const FileSystemDescriptor & fsDescriptor, dfsFile file, const void* buffer, tSize length);

/**
 * @fn status::StatusInternal dfsFlush(const FileSystemDescriptor & namenode, dfsFile file)
 * @brief Flush the data.
 *
 * @param fsDescriptor - fs file belongs to
 * @param file         - The file handle.
 *
 * @return Operation status
 */
status::StatusInternal dfsFlush(const FileSystemDescriptor & fsDescriptor, dfsFile file);

/**
 * Flush out the data in client's user buffer. After the
 * return of this call, new readers will see the data.
 *
 * @param fsDescriptor - fs file belongs to
 * @param file         - file handle
 *
 * @return Operation status
 */
status::StatusInternal dfsHFlush(const FileSystemDescriptor & fsDescriptor, dfsFile file);

/**
 * Number of bytes that can be read from this input stream without blocking.
 * TODO: remove this comment when DFS adaptors are designed.
 * Comment: Useful function to estimate file readiness and progress in "Prepare"
 *
 * @param fsDescriptor - fs file belongs to
 * @param file         - The file handle.
 *
 * @return Returns available bytes; -1 on error.
 */
tOffset dfsAvailable(const FileSystemDescriptor & fsDescriptor, dfsFile file);

/**
 * Copy file from one filesystem to another.
 * Is available inside single cluster (because of credentials only)
 *
 * @param fsDescriptor1 - fs src belongs to
 * @param src           - The path of source file.
 * @param fsDescriptor2 - fs destimation belongs to
 * @param dst           - The path of destination file.
 *
 * @return Operation status
 */
status::StatusInternal dfsCopy(const FileSystemDescriptor & fsDescriptor1, const char* src, const FileSystemDescriptor & fsDescriptor2,
		const char* dst);

/**
 * Copy file within filesystem
 * Is available inside single cluster (because of credentials only)
 *
 * @param fsDescriptor - fs file belongs to
 * @param src          - The path of source file.
 * @param dst          - The path of destination file.
 *
 * @return Returns 0 on success, -1 on error.
 */
status::StatusInternal dfsCopy(const FileSystemDescriptor & fsDescriptor, const char* src, const char* dst);

/**
 * Move file from one filesystem to another.
 * Is available inside single cluster (because of credentials only)
 *
 * @param fsDescriptor - fs file belongs to
 * @param src          - The path of source file.
 * @param dst          - The path of destination file.
 *
 * @return Operation status
 */
status::StatusInternal dfsMove(const FileSystemDescriptor & namenode, const char* src, const char* dst);

/**
 * Delete file.
 *
 * @param fsDescriptor  - fs file belongs to
 * @param path          - The path of the file/folder.
 * @param recursive     - if path is a directory and set to
 * 						  non-zero, the directory is deleted else throws an exception. In
 * 						  case of a file the recursive argument is irrelevant.
 *
 * @return Operation status
 */
status::StatusInternal dfsDelete(const FileSystemDescriptor & fsDescriptor, const char* path, int recursive);

/**
 * Rename the file.
 *
 * @param fsDescriptor  - fs file belongs to
 * @param oldPath   - The path of the source file.
 * @param newPath   - The path of the destination file.
 *
 * @return Operation status
 */
status::StatusInternal dfsRename(const FileSystemDescriptor & fsDescriptor, const char* oldPath,
		const char* newPath);

/**
 * Make the given file and all non-existent
 * parents into directories.
 *
 * @param fsDescriptor  - fs file belongs to
 * @param path          - The path of the directory.
 *
 * @return Returns 0 on success, -1 on error.
 */
status::StatusInternal dfsCreateDirectory(const FileSystemDescriptor & fsDescriptor, const char* path);

/**
 * Set the replication of the specified
 * file to the supplied value
 *
 * @param fsDescriptor  - fs file belongs to
 * @param path          - The path of the file.
 *
 * @return Operation status
 */
status::StatusInternal dfsSetReplication(const FileSystemDescriptor & fsDescriptor, const char* path, int16_t replication);

/**
 * Get list of files/directories for a given
 * directory-path. dfsFreeFileInfo should be called to deallocate memory.
 *
 * @param fsDescriptor - fs file belongs to
 * @param path         - The path of the directory.
 * @param numEntries   - Set to the number of files/directories in path.
 *
 * @return Returns a dynamically-allocated array of dfsFileInfo
 * objects; NULL on error.
 */
dfsFileInfo *dfsListDirectory(const FileSystemDescriptor & fsDescriptor, const char* path,
		int *numEntries);

/**
 * Get information about a path as a (dynamically
 * allocated) single dfsFileInfo struct. dfsFreeFileInfo should be
 * called when the pointer is no longer needed.
 *
 * @param fsDescriptor - fs file belongs to
 * @param path         - The path of the file.
 *
 * @return Returns a dynamically-allocated dfsFileInfo object;
 * NULL on error.
 */
dfsFileInfo *dfsGetPathInfo(const FileSystemDescriptor & fsDescriptor, const char* path);

/**
 * Free up the dfsFileInfo array (including fields)
 *
 * @param fsDescriptor - fs file belongs to
 * @param dfsFileInfo  - The array of dynamically-allocated dfsFileInfo
 * objects.
 *
 * @param numEntries The size of the array.
 */
void dfsFreeFileInfo(const FileSystemDescriptor & fsDescriptor, dfsFileInfo *dfsFileInfo, int numEntries);

/**
 * @fn tOffset dfsGetCapacity(const FileSystemDescriptor & namenode, const char* host)
 * @brief Return the raw capacity of the local filesystem.
 *
 * @param fsDescriptor - fs file belongs to
 * @param host         - hostname
 *
 * @return Returns the raw-capacity; -1 on error.
 */
tOffset dfsGetCapacity(const FileSystemDescriptor & fsDescriptor, const char* host);

/**
 * Return the total raw size of all files in the filesystem.
 *
 * @param fsDescriptor - fs file belongs to
 * @param host         - hostname
 *
 * @return Returns the total-size; -1 on error.
 */
tOffset dfsGetUsed(const FileSystemDescriptor & fsDescriptor, const char* host);

/**
 * Change owner of the specified path
 *
 * @param fsDescriptor - configured fs.
 * @param path         - the path to the file or directory
 * @param owner        - Set to null or "" if only setting group
 * @param group        - Set to null or "" if only setting user
 *
 * @return Operation status
 */
status::StatusInternal dfsChown(const FileSystemDescriptor & fsDescriptor, const char* path,
		const char *owner, const char *group);

/**
 * Change mode of specified path @a path within the specified @a cluster
 *
 * @param fsDescriptor - configured fs
 * @param path         - the path to the file or directory
 * @param mode         - the bitmask to set it to
 *
 * @return Operation status
 */
status::StatusInternal dfsChmod(const FileSystemDescriptor & fsDescriptor, const char* path, short mode);

/**
 * Get read statistics about a file.  This is only applicable to files
 * opened for reading.
 *
 * @param fsDescriptor - configured fs
 * @param file         - The HDFS file
 * @param stats        - (out parameter) on a successful return, the read
 *                		 statistics.  Unchanged otherwise.  You must free the
 *                		 returned statistics with dfsFileFreeReadStatistics.
 *
 * @return         0 if the statistics were successfully returned,
 *                 -1 otherwise.  On a failure, please check errno against
 *                 ENOTSUP.  webhdfs, LocalFilesystem, and so forth may
 *                 not support read statistics.
 */
int dfsFileGetReadStatistics(const FileSystemDescriptor & fsDescriptor,
		dfsFile file,
		struct dfsReadStatistics **stats);

/**
 * Get read statistics about a file.  This is only applicable to files
 * @param stats    HDFS read statistics for a file.
 *
 * @return the number of remote bytes read.
 *
 * TODO: this method is specific for HDFS FileSystem. Should be replaced to other with similar meaning
 */
int64_t dfsReadStatisticsGetRemoteBytesRead(const struct dfsReadStatistics *stats);

/**
 * Free some HDFS read statistics.
 *
 * @param fsDescriptor - configured fs
 * @param stats        - the HDFS read statistics to free.
 */
void dfsFileFreeReadStatistics(const FileSystemDescriptor & fsDescriptor, struct dfsReadStatistics *stats);
}

#endif /* LIBDFS_CACHE_H_ */
