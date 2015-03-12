/*
 * @file filesystem-descriptor-bound.hpp
 * @brief definition of hadoop FileSystem mediator (mainly types translator and connections handler)
 *
 * @date   Oct 9, 2014
 * @author elenav
 */

#ifndef FILESYSTEM_DESCRIPTOR_BOUND_HPP_
#define FILESYSTEM_DESCRIPTOR_BOUND_HPP_

#include <utility>
#include "dfs_cache/common-include.hpp"
#include "dfs_cache/dfs-connection.hpp"

namespace impala{


/**
 * FileSystemDescriptor bound to hadoop FileSystem
 * Holds and manages connections to this file system.
 * Connections are hold in the list.
 * Lists have the important property that insertion
 * and splicing do not invalidate iterators to list elements, and that even removal
 * invalidates only the iterators that point to the elements that are removed
 */
class FileSystemDescriptorBound{
private:
	boost::mutex                                      m_mux;
	std::list<boost::shared_ptr<dfsConnection> >      m_connections;     /**< cached connections to this File System */
	FileSystemDescriptor                              m_fsDescriptor;    /**< File System connection details as configured */

	/** helper predicate to find free non-error connections. */
	struct freeConnectionPredicate
	{
		bool operator() ( const boost::shared_ptr<dfsConnection> & connection )
	    {
	        return connection->state == dfsConnection::FREE_INITIALIZED;
	    }
	};

	struct anyNonInitializedConnectionPredicate{
		bool operator() ( const boost::shared_ptr<dfsConnection> & connection )
	    {
	        return connection->state != dfsConnection::BUSY_OK && connection->state != dfsConnection::FREE_INITIALIZED;
	    }
	};

	/** Encapsulates File System connection logic */
	fsBridge connect();

public:
	~FileSystemDescriptorBound();

	inline FileSystemDescriptorBound(const FileSystemDescriptor & fsDescriptor) {
		// copy the FileSystem configuration
		m_fsDescriptor = fsDescriptor;
	}

	/** Resolve the address of file system using Hadoop File System class.
	 * Should be used when default file system is requested
	 *
	 * @param fsDescriptor - file system descriptor to resolve address for
	 *
	 * @return operation status, only 0 is ok
	 */
	static int resolveFsAddress(FileSystemDescriptor& fsDescriptor);

	inline const FileSystemDescriptor& descriptor() { return m_fsDescriptor; }
	/**
	 * get free FileSystem connection
	 */
	raiiDfsConnection getFreeConnection();

	/**
	 * Open file with given path and flags
	 *
	 * @param conn        - wrapped managed connection
	 * @param path        - file path
	 * @param flags       - flags
	 * @param bufferSize  - buffer size
	 * @param replication - replication
	 * @param blockSize   - block size
	 *
	 * @return file handle on success, NULL otherwise
	 */
	dfsFile fileOpen(raiiDfsConnection& conn, const char* path, int flags, int bufferSize,
			short replication, tSize blocksize);

	/**
	 * Close an opened file handle.
	 *
	 * @param conn - wrapped managed connection
	 * @param file - file stream (org.apache.hadoop.fs.FSDataInputStream or org.apache.hadoop.fs.FSDataOutputStream )
	 *
	 * @return Returns 0 on success, -1 on error.
	 * 		   On error, errno will be set appropriately.
	 *         If the requested file was valid, the memory associated with it will be freed at the end of this call,
	 *         even if there was an I/O error.
	 */
	int fileClose(raiiDfsConnection& conn, dfsFile file);

	/**
	 * Get the current offset in the specified file, in bytes.
	 *
	 * @param conn - wrapped managed connection
	 * @param file - file stream
	 *
	 * @return Current offset, -1 on error.
	 */
	tOffset fileTell(raiiDfsConnection& conn, dfsFile file);

	/**
	 * Seek to given offset in file stream.
	 * This works only for files opened in read-only mode (so that, for fSDataInputStream)
	 *
	 * @param conn       - wrapped managed connection
	 * @param file       - file stream (org.apache.hadoop.fs.FSDataInputStream or org.apache.hadoop.fs.FSDataOutputStream )
	 * @param desiredPos - offset into the file to seek into.
	 *
	 * @return Returns 0 on success, -1 on error.
	 */
	int fileSeek(raiiDfsConnection& conn, dfsFile file, tOffset desiredPos);

	/**
	 * Read data from an open file.
	 *
	 * @param conn   - wrapped managed connection
	 * @param file   - file handle.
	 * @param buffer - buffer to copy read bytes into.
	 * @param length - length of the buffer.
	 *
	 * @return      On success, a positive number indicating how many bytes
	 *              were read.
	 *              On end-of-file, 0.
	 *              On error, -1.  Errno will be set to the error code.
	 *              Just like the POSIX read function, hdfsRead will return -1
	 *              and set errno to EINTR if data is temporarily unavailable,
	 *              but we are not yet at the end of the file.
	 */
	tSize fileRead(raiiDfsConnection& conn, dfsFile file, void* buffer, tSize length);

	/**
	 * Positional read of data from an opened stream.
	 *
	 * @param conn     - wrapped managed connection
	 * @param file     - file handle.
	 * @param position - position from which to read
	 * @param buffer   - buffer to copy read bytes into.
	 * @param length   - length of the buffer.
	 *
	 * @return      See fileRead
	 */
	tSize filePread(raiiDfsConnection& conn, dfsFile file, tOffset position,
			void* buffer, tSize length);

	/**
	 * Positional read of data from an opened stream.
	 *
	 * @param conn     - wrapped managed connection
	 * @param file     - file handle.
	 * @param buffer   - buffer to get bytes to write from.
	 * @param length   - length of the buffer.
	 *
	 * @return      See fileWrte
	 */
	tSize fileWrite(raiiDfsConnection& conn, dfsFile file, const void* buffer, tSize length);

	/**
	 * Rename the file, specified by @a oldPath, to @a newPath
	 *
	 * @param conn    - wrapped managed connection
	 * @param oldPath - old file path
	 * @param newPath - new file path
	 *
	 * @return operation status, 0 is "success"
	 */
	int fileRename(raiiDfsConnection& conn, const char* oldPath, const char* newPath);

	/**
	 * Copy file described by path @a src within the source file system described by @a conn_src connection
	 * to path @a dst within the target file system @a conn_dest
	 *
	 * @param conn_src  - wrapped managed connection to source fs
	 * @param src       - source file name within source fs
	 * @param conn_dest - wrapped managed connection to target fs
	 * @param dst       - destination file name within destination fs
	 *
	 * @return operation status, 0 is "success"
	 */
	static int fileCopy(raiiDfsConnection& conn_src, const char* src, raiiDfsConnection& conn_dest, const char* dst);

	/**
	 * Delete specified path.
	 *
	 * @param conn - wrapped managed connection
	 * @param path      - path to delete
	 * @param recursive - flag, indicates whether recursive removal is required
	 *
	 * @return operation status, 0 is "success"
	 */
	int pathDelete(raiiDfsConnection& conn, const char* path, int recursive);

	/**
	 * Get the specified path info
	 *
	 * @param conn - wrapped managed connection
	 * @param path - path to get info(s) for
	 *
	 * @return path info(s)
	 */
	dfsFileInfo* fileInfo(raiiDfsConnection& conn, const char* path);

	/**
	 * Get list of files/directories for a given
	 * directory-path. freeFileInfo should be called to deallocate memory.
	 *
	 * @param [in]  conn       - wrapped managed connection
	 * @param [in]  path       - path of the directory.
	 * @param [out] numEntries - set to the number of files/directories in path.
	 *
	 * @return Returns a dynamically-allocated array of dfsFileInfo
	 * objects; NULL on error.
	 */
	dfsFileInfo* listDirectory(raiiDfsConnection& conn, const char* path, int *numEntries);

	/**
	 * Create directory with a given path.
	 *
	 * @param [in]  conn       - wrapped managed connection
	 * @param [in]  path       - path of the directory to create.
	 *
	 * @return operation status, 0 on success.
	 */
	int createDirectory(raiiDfsConnection& conn, const char* path);

	/**
	 * Free file info
	 *
	 * @param fileInfo     - file info set to free
	 * @param numOfEntries - number of entries in file info set
	 */
	static void freeFileInfo(dfsFileInfo* fileInfo, int numOfEntries);

	/*
	 * Check that specified @a path exists on the specified fs
	 *
	 * @param conn - wrapped managed connection
	 * @param path - path to check for existence
	 *
	 * @return true if path exists, false otherwise
	 */
	bool pathExists(raiiDfsConnection& conn, const char* path);

	/**
	 * Retrieve default block size within the filesystem
	 *
	 * @return default block size
	 */
	int64_t getDefaultBlockSize(raiiDfsConnection& conn);

	/**
	 * Allocate a zero-copy options structure.
	 *
	 * You must free all options structures allocated with this function using
	 * hadoopRzOptionsFree.
	 *
	 * @return            A zero-copy options structure, or NULL if one could
	 *                    not be allocated.  If NULL is returned, errno will
	 *                    contain the error number.
	 */
	static struct hadoopRzOptions* _hadoopRzOptionsAlloc(void);

	/**
	 * Determine whether we should skip checksums in read0.
	 *
	 * @param opts        The options structure.
	 * @param skip        Nonzero to skip checksums sometimes; zero to always
	 *                    check them.
	 *
	 * @return            0 on success; -1 plus errno on failure.
	 */
	static int _hadoopRzOptionsSetSkipChecksum(
	        struct hadoopRzOptions* opts, int skip);
	/**
	 * Set the ByteBufferPool to use with read0.
	 *
	 * @param opts        The options structure.
	 * @param className   If this is NULL, we will not use any
	 *                    ByteBufferPool.  If this is non-NULL, it will be
	 *                    treated as the name of the pool class to use.
	 *                    For example, you can use
	 *                    ELASTIC_BYTE_BUFFER_POOL_CLASS.
	 *
	 * @return            0 if the ByteBufferPool class was found and
	 *                    instantiated;
	 *                    -1 plus errno otherwise.
	 */
	static int _hadoopRzOptionsSetByteBufferPool(
	        struct hadoopRzOptions* opts, const char *className);

	/**
	 * Free a hadoopRzOptionsFree structure.
	 *
	 * @param opts        The options structure to free.
	 *                    Any associated ByteBufferPool will also be freed.
	 */
	static void _hadoopRzOptionsFree(struct hadoopRzOptions* opts);

	/**
	 * Perform a byte buffer read.
	 * If possible, this will be a zero-copy (mmap) read.
	 *
	 * @param file       The file to read from.
	 * @param opts       An options structure created by hadoopRzOptionsAlloc.
	 * @param maxLength  The maximum length to read.  We may read fewer bytes
	 *                   than this length.
	 *
	 * @return           On success, returns a new hadoopRzBuffer.
	 *                   This buffer will continue to be valid and readable
	 *                   until it is released by readZeroBufferFree.  Failure to
	 *                   release a buffer will lead to a memory leak.
	 *
	 *                   NULL plus an errno code on an error.
	 *                   errno = EOPNOTSUPP indicates that we could not do a
	 *                   zero-copy read, and there was no ByteBufferPool
	 *                   supplied.
	 */
	static struct hadoopRzBuffer* _hadoopReadZero(dfsFile file,
	        struct hadoopRzOptions* opts, int32_t maxLength);

	/**
	 * Determine the length of the buffer returned from readZero.
	 *
	 * @param buffer     a buffer returned from readZero.
	 * @return           the length of the buffer.
	 */
	static int32_t _hadoopRzBufferLength(const struct hadoopRzBuffer* buffer);

	/**
	 * Get a pointer to the raw buffer returned from readZero.
	 *
	 * To find out how many bytes this buffer contains, call
	 * hadoopRzBufferLength.
	 *
	 * @param buffer     a buffer returned from readZero.
	 * @return           a pointer to the start of the buffer.  This will be
	 *                   NULL when end-of-file has been reached.
	 */
	static const void * _hadoopRzBufferGet(const struct hadoopRzBuffer* buffer);

	/**
	 * Release a buffer obtained through readZero.
	 *
	 * @param file       The dfs stream that created this buffer.  This must be
	 *                   the same stream you called hadoopReadZero on.
	 * @param buffer     The buffer to release.
	 */
	static void _hadoopRzBufferFree(dfsFile file, struct hadoopRzBuffer* buffer);
};
}

#endif /* FILESYSTEM_DESCRIPTOR_BOUND_HPP_ */
