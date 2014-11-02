/*
 * @file  hadoopfs-adaptive.hpp
 * @brief wraps the org.apache.hadoop.fs.FileSystem in order to access its file operations and
 * statistics on them
 *
 * @date   Nov 1, 2014
 * @author elenav
 */

#ifndef HADOOPFS_ADAPTIVE_HPP_
#define HADOOPFS_ADAPTIVE_HPP_

#include "dfs_cache/common-include.hpp"
#include "dfs_cache/hadoopfs-definitions.hpp"

namespace impala {

/****************************  Initialize and shutdown  ********************************/
/**
 * Connect to the filesystem based on the uri, the passed
 * configuration and the user
 *
 * @param uri  - uri of the filesystem
 * @param conf - the configuration to use
 * @param user - to perform the get as
 *
 * @return filesystem handle
 */
fSBridge connect(const Uri& uri, const fsConfiguration& conf, std::string user);

/**
 * Connect to configured filesystem implementation.
 *
 * @param conf - the configuration to use
 *
 * @return filesystem handle
 */
fSBridge connect(const fsConfiguration& conf);

/** Connect to the FileSystem for this URI's scheme and authority.  The scheme
 * of the URI determines a configuration property name,
 * <tt>fs.<i>scheme</i>.class</tt> whose value names the FileSystem class.
 * The entire URI is passed to the FileSystem instance's initialize method.
 */
fSBridge connect(const Uri& uri, const fsConfiguration& conf);

/**
 * Connect to local file system.
 * @param conf - the configuration to configure the file system with
 *
 * @return filesystem handle
 */
fSBridge connectLocal(const fsConfiguration& conf);

/****************************  File operations  ********************************/

/** create a file with the provided permission
 * The permission of the file is set to be the provided permission as in
 * setPermission, not permission&~umask
 *
 * It is implemented using two RPCs. It is understood that it is inefficient,
 * but the implementation is thread-safe. The other option is to change the
 * value of umask in configuration to be 0, but it is not thread-safe.
 *
 * @param fs          - file system handle
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
 dfsFile create(fSBridge fs, const std::string& file, int bufferSize, std::list<boost::filesystem::createStreamFlag> createFlags,
		 short replication, long blockSize, bool overwrite, boost::filesystem::perms permission);

/** create a directory with the provided permission
 * The permission of the directory is set to be the provided permission as in
 * setPermission, not permission&~umask
 *
 * @see #create(FileSystem, Path, FsPermission)
 *
 * @param fsBridge   - file system handle
 * @param dir        - the name of the directory to be created
 * @param permission - the permission of the directory
 *
 * @return true if the directory creation succeeds; false otherwise
 */
 bool mkdir(fSBridge fsBridge, const std::string& dir, boost::filesystem::perms permission);

/**
 * Check that a Path belongs to this FileSystem.
 *
 * @param fsBridge - filesystem connection
 * @param path     - path to check
 */
void checkPath(fSBridge fs, const std::string& path);

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
 * @param fsBridge - filesystem connection
 * @param file     - FilesStatus to get data from
 * @param start    - offset into the given file
 * @param len      - length for which to get locations for
 */
std::vector<fsBlockLocation> getFileBlockLocations(fSBridge fs, fileStatus file, long start, long len);

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
 * @param fsBridge - filesystem connection
 * @param p        - path is used to identify an FS since an FS could have
 *          	   another FS that it could be delegating the call to
 *
 * @param start - offset into the given file
 * @param len   - length for which to get locations for
 */
std::vector<fsBlockLocation> getFileBlockLocations(fSBridge fsBridge, const std::string&  p, long start, long len);

/**
 * Append to an existing file (optional operation).
 * Same as append(f, getConf().getInt("io.file.buffer.size", 4096), null)
 *
 * @param fsBridge - filesystem handle
 * @param f        - the existing file to be appended.
 *
 * @return an opened stream to a file which was appended (org.apache.hadoop.fs.FSDataOutputStream)
 * TODO: check where stream pointer is located for returned stream and document this
 */
dfsFileInfo append(fSBridge fsBridge, const std::string& f, int bufferSize);

/**
 * Concat existing files together.
 *
 * @param fsBridge - filesystem handle
 * @param trg      - the path to the target destination.
 * @param psrcs    - the paths to the sources to use for the concatenation.
 */
void concat(fSBridge fsBridge, const std::string& trg, std::vector<std::string> & psrcs);

/**
 * Renames Path src to Path dst.  Can take place on local fs
 * or remote DFS.
 *
 * @param fsBridge - filesystem handle
 * @param src      - path to be renamed
 * @param dst      - new path after rename
 *
 * @return true if rename is successful
 */
bool rename(fSBridge fsBridge, const std::string& src, const std::string& dst);

/** Delete a file.
 *
 * @param fsBridge  - filesystem handle
 * @param f         - the path to delete.
 * @param recursive - if path is a directory and set to
 * true, the directory is deleted else throws an exception. In
 * case of a file the recursive can be set to either true or false.
 *
 * @return  true if delete is successful else false.
 */
 bool del(fSBridge fsBridge, const std::string& f, bool recursive);

/**
 * Mark a path to be deleted when FileSystem is closed.
 * When the JVM shuts down,
 * all FileSystem objects will be closed automatically.
 * Then,
 * the marked path will be deleted as a result of closing the FileSystem.
 *
 * The path has to exist in the file system.
 *
 * @param fsBridge  - filesystem handle
 * @param f         - the path to delete.
 *
 * @return  true if deleteOnExit is successful, otherwise false.
 */
bool deleteOnExit(fSBridge fsBridge, const std::string& f);

/**
 * Cancel the deletion of the path when the FileSystem is closed
 *
 * @param fsBridge  - filesystem handle
 * @param f         - the path to cancel deletion
 *
 * @return true if cancellation was successful
 */
bool cancelDeleteOnExit(fSBridge fsBridge, const std::string& f);


/** Check if specified path exists.
 *
 * @param fsBridge  - filesystem handle
 * @param f         - source file
 */
bool exists(fSBridge fsBridge, const std::string& f);

/** True if the named path is a directory.
 * Note: Avoid using this method. Instead reuse the FileStatus
 * returned by getFileStatus() or listStatus() methods.
 *
 * @param fsBridge  - filesystem handle
 * @param f         - path to check
 */
bool isDirectory(fSBridge fsBridge, const std::string& f);

/** True if the named path is a regular file.
 * Note: Avoid using this method. Instead reuse the FileStatus
 * returned by getFileStatus() or listStatus() methods.
 *
 * @param fsBridge  - filesystem handle
 * @param f         - path to check
 */
bool isFile(fSBridge fsBridge, const std::string& f);

/** Return the {@link ContentSummary} of a given {@link Path}.
 *
 * @param fsBridge  - filesystem handle
 * @param f         - path to use
 */
fsContentSummary getContentSummary(fSBridge fsBridge, const std::string& f);

/**
 * List the statuses of the files/directories in the given path if the path is
 * a directory.
 *
 * @param fsBridge  - filesystem handle
 * @param f         - given path
 *
 * @return the statuses of the files/directories in the given patch
 */
std::vector<fileStatus> listStatus(fSBridge fsBridge, const std::string& f);

/**
 * @return an iterator over the corrupt files under the given path
 * (may contain duplicates if a file has more than one corrupt block)
 *
 * @param fsBridge  - filesystem handle
 * @param path      - path to check
 *
 * @return vector of corrupted files under given path. Note that in original org.apache.hadoop.fs.FileSystem API
 * return value of wrapped method which is public RemoteIterator<Path> listCorruptFileBlocks(Path path)
 * is the iterator, therefore, the implementation in order to avoid complexities should fetch all iterator
 * to the vector of paths.
 */
std::vector<std::string> listCorruptFileBlocks(fSBridge fsBridge, const std::string& path);

/**
 * Filter files/directories in the given list of paths using default
 * path filter.
 *
 * @param fsBridge  - filesystem handle
 * @param files     - a list of paths
 *
 * @return a list of statuses for the files under the given paths after
 *         applying the filter default Path filter
 */
std::vector<fileStatus> listStatus(fSBridge fsBridge, std::vector<std::string> & files);

/** Return the current user's home directory in this filesystem.
 * The default implementation returns "/user/$USER/".
 *
 * @param fsBridge - filesystem handle
 */
std::string getHomeDirectory(fSBridge fsBridge);
/**
 * Set the current working directory for the given file system. All relative
 * paths will be resolved relative to it.
 *
 * @param fsBridge - filesystem handle
 * @param new_dir
 */
void setWorkingDirectory(fSBridge fsBridge, const std::string& new_dir);

/**
 * Get the current working directory for the given file system
 * @return the directory pathname
 *
 * @param fsBridge - filesystem handle
 */
std::string getWorkingDirectory(fSBridge fsBridge);

/**
 * The src file is on the local disk.  Add it to FS at
 * the given dst name and the source is kept intact afterwards
 *
 * @param fsBridge  - filesystem handle
 * @param src       - local file path
 * @param dst       - remote file path
 * @param overwrite - whether to overwrite an existing file
 */
void copyFromLocalFile(fSBridge fsBridge, std::vector<std::string>& src, const std::string& dst, bool overwrite);

/**
 * The src file is under FS, and the dst is on the local disk.
 * Copy it from FS control to the local dst name.
 *
 * @param fsBridge  - filesystem handle
 * @param src       - remote (FS) file path
 * @param dst path  - local path
 */
void copyToLocalFile(fSBridge fsBridge, const std::string& src, const std::string& dst);

/**
 * Returns a local File that the user can write output to.  The caller
 * provides both the eventual FS target name and the local working
 * file.  If the FS is local, we write directly into the target.  If
 * the FS is remote, we write into the tmp local area.
 *
 * @param fsBridge     - filesystem handle
 * @param fsOutputFile - path of output file
 * @param tmpLocalFile - path of local tmp file
 */
std::string startLocalOutput(fSBridge fsBridge, const std::string& fsOutputFile, const std::string& tmpLocalFile);

/**
 * Called when we're all done writing to the target.  A local FS will
 * do nothing, because we've written to exactly the right place.  A remote
 * FS will copy the contents of tmpLocalFile to the correct target at
 * fsOutputFile.
 *
 * @param fsBridge     - filesystem handle
 * @param fsOutputFile - path of output file
 * @param tmpLocalFile - path to local tmp file
 */
void completeLocalOutput(fSBridge fsBridge, const std::string& fsOutputFile, const std::string& tmpLocalFile);

/** Return the total size of all files in the filesystem.
 *
 * @param fsBridge - filesystem handle
 */
long getUsed(fSBridge fsBridge);

/**
 * Return a file status object that represents the path.
 *
 * @param fsBridge - filesystem handle
 * @param f        - the path we want information from
 *
 * @return a FileStatus object
 */
fileStatus getFileStatus(fSBridge fsBridge, const std::string& f);

/**
 * Get the checksum of a file.
 *
 * @param fsBridge - filesystem handle
 * @param f        - The file path
 *
 * @return The file checksum.  The default return value is null,
 *  which indicates that no checksum algorithm is implemented
 *  in the corresponding FileSystem.
 */
fsChecksum getFileChecksum(fSBridge fsBridge, const std::string& f);

/**********************************  Operations with  org.apache.hadoop.fs.FSDataInputStream **/

/**
 * Opens an fSDataInputStream at the indicated Path.
 *
 * @param fsBridge   - filesystem handle
 * @param f          - the file name to open
 * @param bufferSize - the size of the buffer to be used.
 *
 * @return opened input stream
 */
dfsFile fopen(fSBridge fsBridge, const std::string& f, int flags, int bufferSize, short replication, tSize blockSize);

/**
 * Close an opened filestream.
 *
 * @param fsBridge - filesystem handle
 * @param file     - file stream (org.apache.hadoop.fs.FSDataInputStream or org.apache.hadoop.fs.FSDataOutputStream )
 *
 * @return Returns 0 on success, -1 on error.
 */
int fclose(fSBridge fsBridge, dfsFile file);

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
int fseek(fSBridge fsBridge, dfsFile file, tOffset desiredPos);

/**
 * Get the current offset in the file, in bytes
 *
 * @param fsBridge   - filesystem handle
 * @param file       - file stream.
 *
 * @return Current offset, -1 on error.
 */
tOffset hdfsTell(fSBridge fsBridge, dfsFile file);

/**
 * Read data from an opened stream
 *
 * @param fsBridge   - filesystem handle
 * @param file       - file stream (org.apache.hadoop.fs.FSDataInputStream or org.apache.hadoop.fs.FSDataOutputStream )
 * @param buffer     - the buffer to copy read bytes into.
 * @param length     - the length of the buffer.
 *
 * @return Returns the number of bytes actually read, possibly less than than length;-1 on error.
 */
tSize hdfsRead(fSBridge fsBridge, dfsFile file, void* buffer, tSize length);

/**
 * hdfsWrite - Write data into an open file.
 *
 * @param fsBridge   - filesystem handle
 * @param file       - file stream (org.apache.hadoop.fs.FSDataInputStream or org.apache.hadoop.fs.FSDataOutputStream )
 * @param buffer     - the data.
 * @param length     - the no. of bytes to write.
 *
 * @return Returns the number of bytes written, -1 on error.
 */
tSize hdfsWrite(fSBridge fsBridge, dfsFile file, const void* buffer, tSize length);

/**
 * Flush the data.
 *
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @return Returns 0 on success, -1 on error.
 */
int hdfsFlush(fSBridge fsBridge, dfsFile file);
}



#endif /* HADOOPFS_ADAPTIVE_HPP_ */
