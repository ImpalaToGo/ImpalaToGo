/*
 * @file  hadoopfs-adaptive.hpp
 * @brief wraps the org.apache.hadoop.fs.FileSystem in order to access its file operations and
 * statistics on them
 *
 * Hadoop sources used for this file to born:
 * - libhdfs
 * https://svn.apache.org/repos/asf/hadoop/common/tags/release-2.3.0/hadoop-hdfs-project/hadoop-hdfs/src/main/native/libhdfs/hdfs.h
 * https://svn.apache.org/repos/asf/hadoop/common/tags/release-2.3.0/hadoop-hdfs-project/hadoop-hdfs/src/main/native/libhdfs/hdfs.c
 *
 *
 * @date   Nov 1, 2014
 * @author elenav
 */

#ifndef HADOOPFS_ADAPTIVE_HPP_
#define HADOOPFS_ADAPTIVE_HPP_

#include "dfs_cache/common-include.hpp"
#include "dfs_cache/hadoop-fs-definitions.h"

#ifdef __cplusplus
extern "C" {
#endif

/****************************  Initialize and shutdown  ********************************/
/**
 * Connect to the filesystem based on the uri, the passed
 * configuration and the user
 *
 * @param host     - filesystem host
 * @param port     - filesystem port
 * @param dfs_type - dfs type to be able to assemble corresponding configuration for FileSystem class resolver
 * @param user     - user to perform the get as
 *
 * @return filesystem handle
 */
fsBridge _dfsConnect(const char* host, int port, dfs::DFS_TYPE dfs_type, const char* user);

/**
 * Disconnect from specified file system.
 *
 * @param filesystem  - file system handle
 *
 * @return Returns 0 on success, -1 on error.
 *         Even if there is an error, the resources associated with the
 *         hdfsFS will be freed.
 */
int _dfsDisconnect(fsBridge filesystem);

/****************************  Filesystem operations  ********************************/

/**
 * Checks if a given path exists on the filesystem
 *
 * @param filesystem - filesystem handle
 * @param path       - The path to look for
 *
 * @return Returns 0 on success, -1 on error.
 */
int _dfsPathExists(fsBridge filesystem, const char *path);

/**
 * Return an array containing hostnames, offset and size of
 * portions of the given file.  For a nonexistent
 * file or regions, null will be returned.
 *
 * This call is most helpful with DFS, where it returns
 * hostnames of machines that contain the given file.
 *
 * The FileSystem will simply return an elt containing 'localhost'.
 *
 * @param filesystem - filesystem connection
 * @param file       - FilesStatus to get data from
 * @param start      - offset into the given file
 * @param len        - length for which to get locations for
 */
fsBlockLocation* _dfsGetFileBlockLocations(fsBridge filesystem,
		fileStatus file, long start, long len);

/**
 * Return an array containing hostnames, offset and size of
 * portions of the given file.  For a nonexistent
 * file or regions, null will be returned.
 *
 * This call is most helpful with DFS, where it returns
 * hostnames of machines that contain the given file.
 *
 * The FileSystem will simply return an elt containing 'localhost'.
 *
 * @param filesystem - filesystem connection
 * @param path       - path is used to identify an FS since an FS could have
 *          	       another FS that it could be delegating the call to
 *
 * @param start - offset into the given file
 * @param len   - length for which to get locations for
 */
fsBlockLocation* _dfsGetFileBlockLocations(fsBridge filesystem,
		const char* path, long start, long len);

/**
 * Append to an existing file (optional operation).
 * Same as append(f, getConf().getInt("io.file.buffer.size", 4096), null)
 *
 * @param filesystem - filesystem handle
 * @param f          - the existing file to be appended.
 *
 * @return an opened stream to a file which was appended (org.apache.hadoop.fs.FSDataOutputStream)
 * TODO: check where stream pointer is located for returned stream and document this
 */
dfsFileInfo _dfsAppend(fsBridge filesystem, const char* f, int bufferSize);

/**
 * Concat existing files together.
 *
 * @param filesystem - filesystem handle
 * @param trg        - the path to the target destination.
 * @param psrcs      - the paths to the sources to use for the concatenation.
 */
void _dfsConcat(fsBridge filesystem, const char* trg, char** psrcs);

/**
 * Mark a path to be deleted when FileSystem is closed.
 * When the JVM shuts down,
 * all FileSystem objects will be closed automatically.
 * Then,
 * the marked path will be deleted as a result of closing the FileSystem.
 *
 * The path has to exist in the file system.
 *
 * @param filesystem - filesystem handle
 * @param path       - the path to delete.
 *
 * @return  true if deleteOnExit is successful, otherwise false.
 */
bool _dfsDeleteOnExit(fsBridge filesystem, const char* path);

/**
 * Cancel the deletion of the path when the FileSystem is closed
 *
 * @param filesystem - filesystem handle
 * @param path       - the path to cancel deletion
 *
 * @return true if cancellation was successful
 */
bool _dfsCancelDeleteOnExit(fsBridge filesystem, const char* path);

/** True if the named path is a directory.
 * Note: Avoid using this method. Instead reuse the FileStatus
 * returned by getFileStatus() or listStatus() methods.
 *
 * @param filesystem  - filesystem handle
 * @param path        - path to check
 */
bool _dfsIsDirectory(fsBridge filesystem, const char* path);

/** True if the named path is a regular file.
 * Note: Avoid using this method. Instead reuse the FileStatus
 * returned by getFileStatus() or listStatus() methods.
 *
 * @param filesystem - filesystem handle
 * @param path       - path to check
 */
bool _dfsIsFile(fsBridge filesystem, const char* path);

/** Return the ContentSummary of a given path.
 *
 * @param filesystem - filesystem handle
 * @param path       - path to use
 */
fsContentSummary _dfsGetContentSummary(fsBridge filesystem, const char* path);

/**
 * @return an iterator over the corrupt files under the given path
 * (may contain duplicates if a file has more than one corrupt block)
 *
 * @param filesystem - filesystem handle
 * @param path       - path to check
 *
 * @return vector of corrupted files under given path. Note that in original org.apache.hadoop.fs.FileSystem API
 * return value of wrapped method which is public RemoteIterator<Path> listCorruptFileBlocks(Path path)
 * is the iterator, therefore, the implementation in order to avoid complexities should fetch all iterator
 * to the vector of paths.
 */
char** _dfsListCorruptFileBlocks(fsBridge filesystem,
		const char* path);

/**
 * Filter files/directories in the given list of paths using default
 * path filter.
 *
 * @param filesystem - filesystem handle
 * @param files      - a list of paths
 *
 * @return a list of statuses for the files under the given paths after
 *         applying the filter default Path filter
 */
fileStatus* _dfsListStatus(fsBridge filesystem,	char** files);

/** Return the current user's home directory in this filesystem.
 * The default implementation returns "/user/$USER/".
 *
 * @param filesystem - filesystem handle
 */
char* _dfsGetHomeDirectory(fsBridge filesystem);

/**
 * Return the raw capacity of the filesystem.
 *
 * @param filesystem - filesystem handle
 *
 * @return Returns the raw-capacity; -1 on error.
 */
tOffset _dfsGetCapacity(fsBridge filesystem);

/**
 * Return the total raw size of all files in the filesystem.
 *
 * @param filesystem - filesystem handle
 *
 * @return Returns the total-size; -1 on error.
 */
tOffset _dfsGetUsed(fsBridge filesystem);

/**
 * The src file is on the local disk.  Add it to FS at
 * the given dst name and the source is kept intact afterwards
 *
 * @param fsBridge  - filesystem handle
 * @param src       - local file path
 * @param dst       - remote file path
 * @param overwrite - whether to overwrite an existing file
 */
void _dfsCopyFromLocalFile(fsBridge filesystem, const char* src,
		const char* dst, bool overwrite);

/**
 * The src file is under FS, and the dst is on the local disk.
 * Copy it from FS control to the local dst name.
 *
 * @param filesystem - filesystem handle
 * @param src        - remote (FS) file path
 * @param dst path   - local path
 */
void _dfsCopyToLocalFile(fsBridge filesystem, const char* src,
		const char* dst);

/**
 * Returns a local File that the user can write output to.  The caller
 * provides both the eventual FS target name and the local working
 * file.  If the FS is local, we write directly into the target.  If
 * the FS is remote, we write into the tmp local area.
 *
 * @param filesystem   - filesystem handle
 * @param fsOutputFile - path of output file
 * @param tmpLocalFile - path of local tmp file
 */
const char* _dfsStartLocalOutput(fsBridge filesystem,
		const char* fsOutputFile, const char* tmpLocalFile);

/**
 * Called when we're all done writing to the target.  A local FS will
 * do nothing, because we've written to exactly the right place.  A remote
 * FS will copy the contents of tmpLocalFile to the correct target at
 * fsOutputFile.
 *
 * @param filesystem   - filesystem handle
 * @param fsOutputFile - path of output file
 * @param tmpLocalFile - path to local tmp file
 */
void _dfsCompleteLocalOutput(fsBridge filesystem, const char* fsOutputFile,
		const char* tmpLocalFile);

/**
 * Return a file status object that represents the path.
 *
 * @param filesystem - filesystem handle
 * @param path       - the path we want information from
 *
 * @return a FileStatus object
 */
fileStatus _dfsGetFileStatus(fsBridge filesystem, const char* path);

/**
 * Get the checksum of a file.
 *
 * @param filesystem - filesystem handle
 * @param path       - The file path
 *
 * @return The file checksum.  The default return value is null,
 *  which indicates that no checksum algorithm is implemented
 *  in the corresponding FileSystem.
 */
fsChecksum _dfsGetFileChecksum(fsBridge filesystem, const char* path);

/****************************  FileSystem/File statistics API *********************************/
/**
 * Determine if a file is open for read.
 *
 * @param file - file stream
 *
 * @return         1 if the file is open for read; 0 otherwise
 */
int _dfsFileIsOpenForRead(dfsFile file);

/**
 * Determine if a file is open for write.
 *
 * @param file - file stream
 *
 * @return         1 if the file is open for write; 0 otherwise
 */
int _dfsFileIsOpenForWrite(dfsFile file);

/**
 * Get read statistics about a file.  This is only applicable to files
 * opened for reading.
 *
 * @param [in]  file  - file stream
 * @param [out] stats - (out parameter) on a successful return, the read
 *                 	 statistics.  Unchanged otherwise.  You must free the
 *                 	 returned statistics with hdfsFileFreeReadStatistics.
 *
 * @return  0 if the statistics were successfully returned,
 *          -1 otherwise.
 *          On a failure, please check errno against ENOTSUP.
 *          webhdfs, LocalFilesystem, and so forth may
 *                 not support read statistics.
 */
int _dfsFileGetReadStatistics(dfsFile file, struct dfsReadStatistics **stats);

/**
 * Free some DFS read statistics.
 *
 * @param stats - DFS read statistics to free.
 */
void _dfsFileFreeReadStatistics(struct dfsReadStatistics *stats);

/**
 * @param stats    DFS read statistics for a file.
 *
 * @return the number of remote bytes read.
 */
int64_t _dfsReadStatisticsGetRemoteBytesRead(const struct dfsReadStatistics *stats);

/**********************************  Operations with org.apache.hadoop.fs.FSData(Output|Input)Stream and file objects **/
/** Partially got from https://svn.apache.org/repos/asf/hadoop/common/tags/release-2.3.0/hadoop-hdfs-project/hadoop-hdfs/src/main/native/libhdfs/hdfs.h **/

/** create a file with the provided permission
 * The permission of the file is set to be the provided permission as in
 * setPermission, not permission&~umask
 *
 * It is implemented using two RPCs. It is understood that it is inefficient,
 * but the implementation is thread-safe. The other option is to change the
 * value of umask in configuration to be 0, but it is not thread-safe.
 *
 * @param filesystem  - file system handle
 * @param file        - the name of the file to be created
 * @param bufferSize  - the size of the buffer to be used.
 * @param createFlags - flags to use for this stream
 * @param replication - required block replication for the file.
 * @param blockSize   - the size of the block to be used.
 * @param overwrite   - if a file with this name already exists, then if true,
 *        the file will be overwritten, and if false an error will be thrown. *
 * @param permission  - the permission of the file
 *
 * @return an output stream - org.apache.hadoop.fs.FSDataOutputStream
 */
dfsFile _dfsCreate(fsBridge filesystem, const char* file, int bufferSize,
		boost::filesystem::createStreamFlag* createFlags,
		short replication, long blockSize, bool overwrite,
		boost::filesystem::perms permission);

/**
 * Opens an fSDataInputStream at the indicated Path in a given mode.
 *
 * @param fsBridge   - filesystem handle
 * @param f          - the file path to open
 * @param bufferSize - the size of the buffer to be used.
 *
 * @param flags       - an | of bits/fcntl.h file flags - supported flags are O_RDONLY, O_WRONLY (meaning create or overwrite i.e., implies O_TRUNCAT),
 *                      O_WRONLY|O_APPEND. Other flags are generally ignored other than (O_RDWR || (O_EXCL & O_CREAT)) which return NULL and set errno equal ENOTSUP.
 * @param bufferSize  - size of buffer for read/write - pass 0 if you want
 *                      to use the default configured values.
 * @param replication - block replication - pass 0 if default configured values are enough.
 * @param blocksize   - size of block - pass 0 if you default configured values are enough.
 *
 * @return Returns the opened file stream.
 */
dfsFile _dfsOpen(fsBridge fsBridge, const char* path, int flags, int bufferSize,
		short replication, tSize blocksize);

/**
 * Close an opened filestream.
 *
 * @param filesystem - filesystem handle
 * @param file       - file stream (org.apache.hadoop.fs.FSDataInputStream or org.apache.hadoop.fs.FSDataOutputStream )
 *
 * @return Returns 0 on success, -1 on error.
 * 		   On error, errno will be set appropriately.
 *         If the requested file was valid, the memory associated with it will be freed at the end of this call,
 *         even if there was an I/O error.
 */
int _dfsClose(fsBridge fsBridge, dfsFile file);

/**
 * Get the current offset in the file, in bytes.
 * @param filesystem - filesystem handle
 * @param file       - file stream
 *
 * @return Current offset, -1 on error.
 */
tOffset _dfsTell(fsBridge filesystem, dfsFile file);

/**
 * Seek to given offset in file stream.
 * This works only for files opened in read-only mode (so that, for fSDataInputStream)
 *
 * @param fsBridge   - filesystem handle
 * @param file       - file stream (org.apache.hadoop.fs.FSDataInputStream or org.apache.hadoop.fs.FSDataOutputStream )
 * @param desiredPos - offset into the file to seek into.
 *
 * @return Returns 0 on success, -1 on error.
 */
int _dfsSeek(fsBridge fsBridge, dfsFile file, tOffset desiredPos);

/**
 * Read data from an open file.
 *
 * @param filesystem - filesystem handle
 * @param file       - file handle.
 * @param buffer     - buffer to copy read bytes into.
 * @param length     - length of the buffer.
 * @return      On success, a positive number indicating how many bytes
 *              were read.
 *              On end-of-file, 0.
 *              On error, -1.  Errno will be set to the error code.
 *              Just like the POSIX read function, hdfsRead will return -1
 *              and set errno to EINTR if data is temporarily unavailable,
 *              but we are not yet at the end of the file.
 */
tSize _dfsRead(fsBridge filesystem, dfsFile file, void* buffer, tSize length);

/**
 * Positional read of data from an opened stream.
 *
 * @param filesystem - filesystem handle
 * @param file       - file handle.
 * @param position   - position from which to read
 * @param buffer     - buffer to copy read bytes into.
 * @param length     - length of the buffer.
 *
 * @return      See dfsRead
 */
tSize _dfsPread(fsBridge filesystem, dfsFile file, tOffset position, void* buffer,
		tSize length);

/**
 * Write data into an open file.
 *
 * @param filesystem - filesystem handle
 * @param file       - file handle.
 * @param buffer     - data.
 * @param length     - no. of bytes to write.
 *
 * @return Returns the number of bytes written, -1 on error.
 */
tSize _dfsWrite(fsBridge filesystem, dfsFile file, const void* buffer, tSize length);

/**
 * Flush the data.
 *
 * @param filesystem - filesystem handle
 * @param file       - file handle.
 *
 * @return Returns 0 on success, -1 on error.
 */
int _dfsFlush(fsBridge filesystem, dfsFile file);

/**
 * Flush out the data in client's user buffer. After the return of this call,
 * new readers will see the data.
 *
 * @param filesystem - filesystem handle
 * @param file       - file handle.
 *
 * @return 0 on success, -1 on error and sets errno
 */
int _dfsHFlush(fsBridge filesystem, dfsFile file);

/**
 * Similar to posix fsync, Flush out the data in client's
 * user buffer. all the way to the disk device (but the disk may have
 * it in its cache).
 *
 * @param filesystem - filesystem handle
 * @param file       - file handle.
 *
 * @return 0 on success, -1 on error and sets errno
 */
int _dfsHSync(fsBridge filesystem, dfsFile file);

/**
 * Return number of bytes that can be read from this input stream without blocking.
 *
 * @param filesystem - filesystem handle
 * @param file       - file handle.
 *
 * @return Returns available bytes; -1 on error.
 */
int _dfsAvailable(fsBridge filesystem, dfsFile file);

/**
 * Copy file from one filesystem to another.
 *
 * @param srcFS - handle to source filesystem.
 * @param src   - path of source file.
 * @param dstFS - handle to destination filesystem.
 * @param dst   - path of destination file.
 *
 * @return Returns 0 on success, -1 on error.
 */
int _dfsCopy(fsBridge srcFS, const char* src, fsBridge dstFS, const char* dst);

/**
 * Move file from one filesystem to another.
 *
 * @param srcFS - handle to source filesystem.
 * @param src   - path of source file.
 * @param dstFS - handle to destination filesystem.
 * @param dst   - path of destination file.
 *
 * @return Returns 0 on success, -1 on error.
 */
int _dfsMove(fsBridge srcFS, const char* src, fsBridge dstFS, const char* dst);

/**
 * Delete file.
 *
 * @param filesystem - filesystem handle
 * @param path       - path of the file.
 * @param recursive  - if path is a directory and set to non-zero, the directory is
 * 					   deleted else throws an exception. In case of a file the recursive
 * 					   argument is irrelevant.
 *
 * @return Returns 0 on success, -1 on error.
 */
int _dfsDelete(fsBridge filesystem, const char* path, int recursive);

/**
 * Rename file.
 *
 * @param filesystem - filesystem handle
 * @param oldPath    - path of the source file.
 * @param newPath    - path of the destination file.
 *
 * @return Returns 0 on success, -1 on error.
 */
int _dfsRename(fsBridge filesystem, const char* oldPath, const char* newPath);

/**
 * hdfsGetWorkingDirectory - Get the current working directory for the given filesystem.
 *
 * @param filesystem - filesystem handle
 * @param buffer     - user-buffer to copy path of cwd into.
 * @param bufferSize - length of user-buffer.
 *
 * @return Returns buffer, NULL on error.
 */
char* _dfsGetWorkingDirectory(fsBridge filesystem, char *buffer, size_t bufferSize);

/**
 * hdfsSetWorkingDirectory - Set the working directory. All relative paths will be resolved relative to it.
 *
 * @param filesystem - filesystem handle
 * @param path       - path of the new 'cwd'.
 *
 * @return Returns 0 on success, -1 on error.
 */
int _dfsSetWorkingDirectory(fsBridge filesystem, const char* path);

/**
 * Create a directory with the provided permission.
 * The permission of the directory is set to be the provided permission as in
 * setPermission, not permission&~umask
 *
 * @param filesystem - file system handle
 * @param dir        - the name of the directory to be created
 * @param permission - the permission of the directory
 *
 * @return Returns 0 on success, -1 on error.
 */
int _dfsCreateDirectory(fsBridge filesystem, const char* path, boost::filesystem::perms permission);

/**
 * Set the replication of the specified file to the supplied value
 *
 * @param filesystem - file system handle
 * @param path       - file path.
 *
 * @return Returns 0 on success, -1 on error.
 */
int _dfsSetReplication(fsBridge filesystem, const char* path, int16_t replication);

/**
 * Get list of files/directories for a given
 * directory-path. hdfsFreeFileInfo should be called to deallocate memory.
 *
 * @param filesystem - file system handle
 * @param path       - path of the directory.
 * @param numEntries - set to the number of files/directories in path.
 *
 * @return Returns a dynamically-allocated array of hdfsFileInfo
 * objects; NULL on error.
 */
dfsFileInfo * _dfsListDirectory(fsBridge filesystem, const char* path, int *numEntries);

/**
 * Get information about a path as a (dynamically
 * allocated) single hdfsFileInfo struct. hdfsFreeFileInfo should be
 * called when the pointer is no longer needed.
 *
 * @param filesystem - file system handle
 * @param path       - path of the directory.
 *
 * @return Returns a dynamically-allocated hdfsFileInfo object;
 * NULL on error.
 */
dfsFileInfo * _dfsGetPathInfo(fsBridge filesystem, const char* path);

/**
 * Free up the hdfsFileInfo array (including fields)
 *
 * @param dfsFileInfo - array of dynamically-allocated hdfsFileInfo objects.
 * @param numEntries The size of the array.
 */
void _dfsFreeFileInfo(dfsFileInfo *hdfsFileInfo, int numEntries);

/**
 * Get hostnames where a particular block (determined by
 * pos & blocksize) of a file is stored. The last element in the array
 * is NULL. Due to replication, a single block could be present on
 * multiple hosts.
 *
 * @param filesystem - file system handle
 * @param path       - file path
 * @param start      - start of the block.
 * @param length     - length of the block.
 *
 * @return Returns a dynamically-allocated 2-d array of blocks-hosts;
 * NULL on error.
 */
char*** _dfsGetHosts(fsBridge filesystem, const char* path, tOffset start,
		tOffset length);

/**
 * Free up the structure returned by dfsGetHosts
 *
 * @param blockHosts - array of dynamically allocated  2-d array of blocks-hosts.
 * @param numEntries - size of the array.
 */
void _dfsFreeHosts(char ***blockHosts, int numEntries);

/**
 * Get the default blocksize at the filesystem indicated by a given path.
 *
 * @param filesystem - file system handle
 * @param path       - given path will be used to locate the actual filesystem.
 * 					   The full path does not have to exist.
 *
 * @return              Returns the default blocksize, or -1 on error.
 */
tOffset _dfsGetDefaultBlockSizeAtPath(fsBridge filesystem, const char *path);

/**
 * Change the user and/or group of a file or directory.
 *
 * @param filesystem - file system handle
 * @param path       - the path to the file or directory
 * @param owner      - user string.  Set to NULL for 'no change'
 * @param group      - group string.  Set to NULL for 'no change'
 *
 * @return 0 on success else -1
 */
int _dfsChown(fsBridge filesystem, const char* path, const char *owner,
		const char *group);

/**
 * change mode of file system object described by @a path to
 * required by @a mode
 * @param filesystem - file system handle
 * @param path       - path to the file or directory
 * @param mode       - bitmask to set it to
 *
 * @return 0 on success else -1
 */
int _dfsChmod(fsBridge filesystem, const char* path, short mode);

/**
 * change modification AND|OR access time of file system object described by @path
 * @param fsBridge - filesystem handle
 * @param path     - the path to the file or directory
 * @param mtime    - new modification time or -1 for no change
 * @param atime    - new access time or -1 for no change
 *
 * @return 0 on success else -1
 */
int _dfsUtime(fsBridge fs, const char* path, tTime mtime, tTime atime);

#ifdef __cplusplus
}
#endif

#endif /* HADOOPFS_ADAPTIVE_HPP_ */
