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
};
}

#endif /* FILESYSTEM_DESCRIPTOR_BOUND_HPP_ */
