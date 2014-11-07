/*
 * @file filesystem-mgr.h
 * @brief Define Filesystem Management features
 *
 *  @date   Oct 3, 2014
 *  @author elenav
 */

#ifndef FILESYSTEM_MGR_H_
#define FILESYSTEM_MGR_H_

#include <memory>
#include <boost/shared_ptr.hpp>
#include <boost/smart_ptr/scoped_ptr.hpp>

#include "dfs_cache/cache-definitions.hpp"
#include "dfs_cache/cache-layer-registry.hpp"

/**
 * @namespace impala
 */
namespace impala {

/**
 * @namespace filemgmt
 */
namespace filemgmt {

class FileSystemManager {
private:
	// Singleton instance. Instantiated in Init().
	static boost::scoped_ptr<FileSystemManager> instance_;

	CacheLayerRegistry*                         m_registry; /**< reference to metadata registry instance */

	FileSystemManager() : m_registry(nullptr) { };
	FileSystemManager(FileSystemManager const& l);            // disable copy constructor
	FileSystemManager& operator=(FileSystemManager const& l); // disable assignment operator

	/** reply the mode basing on specified flags
	 * @param mode (<fcntl>)
	 *
	 * @return mode or empty string if no mode supported
	 */
	std::string getMode(int flags);

	/** platform-specific separator */
	static std::string fileSeparator;

public:
    static FileSystemManager* instance() { return FileSystemManager::instance_.get(); }

    /** Initialize File System Manager. Call this before any File System Manager usage */
    static void init();

	/**
	 * Subscribe to cache registry as one of owners.
	 */
	status::StatusInternal configure() {
		// become one of owners of the arrived registry:
		m_registry = CacheLayerRegistry::instance();
		return status::StatusInternal::OK;
	}

	/**
	 * Construct local fully qualified path basing on file with @path and its @fsDescriptor owner
	 *
	 * @param fsDescriptor - file system descriptor
	 * @param path         - file path
	 *
	 * @return fully qualified local path
	 */
	std::string constructLocalPath(const FileSystemDescriptor& fsDescriptor, const char* path);

	/**
	 * @fn  dfsOpenFile(const FileSystemDescriptor & fsDescriptor, const char* path, int flags,
	 *                    int bufferSize, short replication, tSize blocksize)
	 * @brief Open the file in given mode.This will be done locally but @a fsDescriptoris required
	 * for path resolution.
	 *
	 * @param fsDescriptor - fsDescriptor descriptor, to locate the file locally.
	 * @param path     - The full path to the file.
	 * @param flags    - an | of bits/fcntl.h file flags - supported flags are O_RDONLY, O_WRONLY (meaning create or overwrite i.e., implies O_TRUNCAT),
	 * O_WRONLY|O_APPEND. Other flags are generally ignored other than (O_RDWR || (O_EXCL & O_CREAT)) which return NULL and set errno equal ENOTSUP.
	 *
	 * @param bufferSize  - Size of buffer for read/write - pass 0 if you want
	 * to use the default configured values.
	 * @param replication - Block replication - pass 0 if you want to use
	 * the default configured values.
	 * @param blocksize   - Size of block - pass 0 if you want to use the
	 * default configured values.
	 * @param available - flag, indicates whether the requested file is available.
	 *
	 * @return Returns the handle to the open file or NULL on error.
	 */
	dfsFile dfsOpenFile(const FileSystemDescriptor & fsDescriptor, const char* path, int flags,
			int bufferSize, short replication, tSize blocksize,
			bool& available);

	/**
	 * @fn dfsCloseFile(dfsFile file)
	 * @brief Close an opened file. File is always local.
	 * Note that file may have several clients at a time and will be closed only in case if no clients exist more.
	 *
	 * @param file - The file handle.
	 *
	 * @return Operation status.
	 */
	status::StatusInternal dfsCloseFile(const FileSystemDescriptor & fsDescriptor, dfsFile file);

	/**
	 * @fn int dfsExists(const FileSystemDescriptor & fsDescriptor, const char *path)
	 * @brief Checks if a given path exists on the remote cluster
	 *
	 * @param fsDescriptor- data fsDescriptorid
	 * @param path    - The path to look for
	 * @return Returns 0 on success, -1 on error.
	 */
	status::StatusInternal dfsExists(const FileSystemDescriptor & fsDescriptor, const char *path);

	/**
	 * dfsSeek - Seek to given offset in file.
	 * This works only for files opened in read-only mode.
	 * @param fs         - The configured filesystem handle.
	 * @param file       - The file handle.
	 * @param desiredPos - Offset into the file to seek into.
	 *
	 * @return Returns 0 on success, -1 on error.
	 */
	status::StatusInternal dfsSeek(const FileSystemDescriptor & fsDescriptor, dfsFile file, tOffset desiredPos);

	/**
	 * dfsTell - Get the current offset in the file, in bytes.
	 * @param fs   - The configured filesystem handle.
	 * @param file - The file handle.
	 *
	 * @return Current offset, -1 on error.
	 */
	tOffset dfsTell(const FileSystemDescriptor & fsDescriptor, dfsFile file);

	/**
	 * dfsRead - Read data from an open file.
	 * @param fs     - The configured filesystem handle.
	 * @param file   -  The file handle.
	 * @param buffer -  The buffer to copy read bytes into.
	 * @param length -  The length of the buffer.
	 *
	 * @return Returns the number of bytes actually read, possibly less
	 * than than length;-1 on error.
	 */
	tSize dfsRead(const FileSystemDescriptor & fsDescriptor, dfsFile file, void* buffer, tSize length);

	/**
	 * Positional read of data from an open file.
	 *
	 * @param file     - The file handle.
	 * @param position - Position from which to read
	 * @param buffer   - The buffer to copy read bytes into.
	 * @param length   - The length of the buffer.
	 *
	 * @return Returns the number of bytes actually read, possibly less than
	 * than length;-1 on error.
	 */
	tSize dfsPread(const FileSystemDescriptor & fsDescriptor, dfsFile file, tOffset position, void* buffer, tSize length);

	/**
	 * Write data into an open file.
	 *
	 * @param file   - The file handle.
	 * @param buffer - The data.
	 * @param length - The no. of bytes to write.
	 *
	 * @return Returns the number of bytes written, -1 on error.
	 */
	tSize dfsWrite(const FileSystemDescriptor & fsDescriptor, dfsFile file, const void* buffer, tSize length);

	/**
	 * Flush the data
	 *
	 * @param file - The file handle.
	 *
	 * @return Returns 0 on success, -1 on error.
	 */
	status::StatusInternal dfsFlush(const FileSystemDescriptor & fsDescriptor, dfsFile file);

	/**
	 * Flush out the data in client's user buffer. After the
	 * return of this call, new readers will see the data.
	 *
	 * @param file - file handle
	 *
	 * @return 0 on success, -1 on error and sets errno
	 */
	status::StatusInternal dfsHFlush(const FileSystemDescriptor & fsDescriptor, dfsFile file);

	/**
	 * @fn int dfsAvailable(dfsFile file)
	 * @brief Number of bytes that can be read from this
	 * input stream without blocking.
	 *
	 * @param file - The file handle.
	 *
	 * @return Returns available bytes; -1 on error.
	 */
	tOffset dfsAvailable(const FileSystemDescriptor & fsDescriptor, dfsFile file);

	/**
	 * Copy file from one filesystem to another.
	 * Is available inside single fsDescriptor(because of credentials only)
	 *
	 * @param src - The path of source file.
	 * @param dst - The path of destination file.
	 *
	 * @return Returns 0 on success, -1 on error.
	 */
	status::StatusInternal dfsCopy(const FileSystemDescriptor & fsDescriptor, const char* src, const char* dst);

	/**
	 * Copy file from one filesystem to another.
	 * Is available inside single fsDescriptor(because of credentials only)
	 *
	 * @param src - The path of source file.
	 * @param dst - The path of destination file.
	 *
	 * @return Returns 0 on success, -1 on error.
	 */
	status::StatusInternal dfsCopy(const FileSystemDescriptor & fsDescriptor1, const char* src,
			const FileSystemDescriptor & fsDescriptor2, const char* dst);

	/**
	 * Move file from one filesystem to another.
	 * Is available inside single fsDescriptor(because of credentials only)
	 *
	 * @param src - The path of source file.
	 * @param dst - The path of destination file.
	 *
	 * @return Returns 0 on success, -1 on error.
	 */
	status::StatusInternal dfsMove(const FileSystemDescriptor & fsDescriptor, const char* src, const char* dst);

	/**
	 * Delete file.
	 *
	 * @param path The path of the file.
	 * @return Returns 0 on success, -1 on error.
	 */
	status::StatusInternal dfsDelete(const FileSystemDescriptor & fsDescriptor, const char* path, int recursive);

	/**
	 * Rename the file.
	 *
	 * @param oldPath - The path of the source file.
	 * @param newPath - The path of the destination file.
	 *
	 * @return Returns 0 on success, -1 on error.
	 */
	status::StatusInternal dfsRename(const FileSystemDescriptor & fsDescriptor, const char* oldPath, const char* newPath);

	/**
	 * Make the given file and all non-existent
	 * parents into directories.
	 *
	 * @param path - The path of the directory.
	 *
	 * @return Returns 0 on success, -1 on error.
	 */
	status::StatusInternal dfsCreateDirectory(const FileSystemDescriptor & fsDescriptor, const char* path);

	/**
	 * Set the replication of the specified file to the supplied value
	 *
	 * @param path - The path of the file.
	 *
	 * @return Returns 0 on success, -1 on error.
	 */
	status::StatusInternal dfsSetReplication(const FileSystemDescriptor & fsDescriptor, const char* path, int16_t replication);

	/**
	 * @fn dfsFileInfo *dfsListDirectory(clusterId cluster, const char* path,
	 *                                int *numEntries)
	 * @brief Get list of files/directories for a given
	 * directory-path. dfsFreeFileInfo should be called to deallocate memory.
	 *
	 * @param fsDescriptor - the fs path belongs to.
	 * @param path         - The path of the directory.
	 * @param numEntries   - Set to the number of files/directories in path.
	 *
	 * @return Returns a dynamically-allocated array of dfsFileInfo
	 * objects; NULL on error.
	 */
	dfsFileInfo *dfsListDirectory(const FileSystemDescriptor & fsDescriptor, const char* path,
			int *numEntries);

	/**
	 * @fn dfsFileInfo *dfsGetPathInfo(clusterId cluster, const char* path)
	 * @brief Get information about a path as a (dynamically
	 * allocated) single dfsFileInfo struct. dfsFreeFileInfo should be
	 * called when the pointer is no longer needed.
	 *
	 * @param fsDescriptor - the fs path belongs to.
	 * @param path         - The path of the file.
	 *
	 * @return Returns a dynamically-allocated dfsFileInfo object;
	 * NULL on error.
	 */
	dfsFileInfo *dfsGetPathInfo(const FileSystemDescriptor & fsDescriptor, const char* path);

	/**
	 * @fn void dfsFreeFileInfo(dfsFileInfo *dfsFileInfo, int numEntries
	 * @brief Free up the dfsFileInfo array (including fields)
	 *
	 * @param dfsFileInfo - The array of dynamically-allocated dfsFileInfo
	 * objects.
	 *
	 * @param numEntries The size of the array.
	 */
	void dfsFreeFileInfo(const FileSystemDescriptor & fsDescriptor, dfsFileInfo *dfsFileInfo, int numEntries);

	/**
	 * @fn tOffset dfsGetCapacity(hdfsFS fs)
	 * @brief Return the raw capacity of the local filesystem.
	 *
	 * @param fsDescriptor - fs machine belongs to
	 * @param host         - hostname
	 *
	 * @return Returns the raw-capacity; -1 on error.
	 */
	tOffset dfsGetCapacity(const FileSystemDescriptor & fsDescriptor, const char* host);

	/**
	 * @fn tOffset dfsGetUsed(clusterId cluster, const char* host)
	 * Return the total raw size of all files in the filesystem.
	 *
	 * @param fsDescriptor - fs machine belongs to
	 * @param host         - hostname
	 *
	 * @return Returns the total-size; -1 on error.
	 */
	tOffset dfsGetUsed(const FileSystemDescriptor & fsDescriptor, const char* host);

	/**
	 * @fn int dfsChown(clusterId cluster, const char* path, const char *owner, const char *group)
	 * @brief Change owner of the specified path
	 *
	 * @param fsDescriptor - fs the path belongs to
	 * @param path         - the path to the file or directory
	 * @param owner        - Set  to null or "" if only setting group
	 * @param group        - Set to null or "" if only setting user
	 * @return 0 on success else -1
	 */
	status::StatusInternal dfsChown(const FileSystemDescriptor & fsDescriptor, const char* path,
			const char *owner, const char *group);

	/**
	 * @fn int dfsChmod(clusterId Cluster, const char* path, short mode)
	 * @brief Change mode of specified path @a path within the specified @a cluster
	 * @param fsDescriptor - fs the path belongs to
	 * @param path         - the path to the file or directory
	 * @param mode         - the bitmask to set it to
	 * @return 0 on success else -1
	 */
	status::StatusInternal dfsChmod(const FileSystemDescriptor & fsDescriptor, const char* path, short mode);
};

} // filemgmt
} // impala

#endif /* FILESYSTEM_MGR_H_ */
