/*
 * @file  cache-layer-registry.h
 * @brief contains definition of cache centralized registry
 *
 * @date   Sep 29, 2014
 * @author elenav
 */

#ifndef CACHE_LAYER_REGISTRY_H_
#define CACHE_LAYER_REGISTRY_H_

#include <cstring>
#include <list>
#include <memory>
#include <boost/function.hpp>
#include <boost/bind.hpp>

#include <boost/shared_ptr.hpp>

#include "dfs_cache/cache-definitions.hpp"
#include "dfs_cache/common-include.hpp"
#include "dfs_cache/filesystem-descriptor-bound.hpp"
#include "dfs_cache/utilities.hpp"

/**
 * @namespace impala
 */
namespace impala {

/** defines the map of maps of remote FileSystem descriptors.
 *  Key   - supported filesystem type
 *  Value - map of known filesystems of this type
 *
 *  For map of known filesystems of same type:
 *  Key   - FileSystem address
 *  Value - adaptor to FileSystem described by @address
 * */
typedef std::map<DFS_TYPE, std::map<std::string, boost::shared_ptr<FileSystemDescriptorBound> > > DFSConnections;

/**
 * Represent cache data registry.
 */
class CacheLayerRegistry{
private:
	/** Singleton instance. Instantiated in init(). */
	static boost::scoped_ptr<CacheLayerRegistry> instance_;

	static std::string fileSeparator;  /**< platform-specific file separator */

	FileRegistry*       m_cache;       /**< Registry of cache-managed files */
	DFSConnections      m_filesystems; /**< Registry of file systems adaptors registered as a target for impala as a client */

	std::string m_localstorageRoot;   /**< path to local file system storage root */

	boost::mutex m_cachemux;           /**< mutex for file cache collection */
	boost::mutex m_connmux;            /**< mutex for connections collection */
	boost::mutex m_adaptorsmux;        /**< mutex for adapters collection */

	volatile bool m_valid;             /**< flag, indicates that registry is in the valid state */

	CacheLayerRegistry(const std::string& root = "") {
		m_valid = false;
		if(root.empty())
			localstorage(constants::DEFAULT_CACHE_ROOT);
		else
			localstorage(root);

		// create the autoload LRU cache, default is 50 Gb
		m_cache = new FileSystemLRUCache(constants::DEFAULT_CACHE_CAPACITY, m_localstorageRoot, true);
	}

	CacheLayerRegistry(CacheLayerRegistry const& l);            // disable copy constructor
	CacheLayerRegistry& operator=(CacheLayerRegistry const& l); // disable assignment operator

    /** Setter for Local storage root file system path */
    inline void localstorage(const std::string& localpath) {
    	m_localstorageRoot = localpath;

        // add file separator if no specified:
    	bool trailing = impala::utilities::endsWith(m_localstorageRoot, fileSeparator);
    	if(!trailing){
    		m_localstorageRoot += fileSeparator;
    	}
    }

    /** reload the cache */
    inline void reload(){
    	// reload the cache:
    	if(m_cache->reload(m_localstorageRoot))
    		m_valid = true;
    }

public:

	~CacheLayerRegistry() {
		delete m_cache;
		LOG (INFO) << "cache layer registry destructor" << "\n";
	}
    static CacheLayerRegistry* instance() { return CacheLayerRegistry::instance_.get(); }

	/** *************************** External configuration  ************************************************************/

    /** Initialize registry. Call this before any Registry usage
     * @param root - local cache root - file system absolute path
     *
     */
    static void init(const std::string& root = "");

    /** Getter for Local storage root file system path */
    inline std::string localstorage() {return m_localstorageRoot;}

	/**
	 * Setup namenode
	 *
	 * @param[In/Out] fsDescriptor - file system connection details
	 *
	 * @return Operation status
	 */
	status::StatusInternal setupFileSystem(FileSystemDescriptor & fsDescriptor);


	/** ***************************  DFS related registry API  ***********************************************************/

	/**
	 * Get connected FileSystem descriptor by its connection descriptor
	 *
	 * @param fsDescriptor - configured file system connection details
	 *
	 * @return filesystem adaptor
	 */
	const boost::shared_ptr<FileSystemDescriptorBound>* getFileSystemDescriptor(const FileSystemDescriptor & fsDescriptor);

	/** *************************** Local file system registry API *****************************************************/

	/**
	 * Get the File object by its path
	 *
	 * @param [in]  path       - file fqp
	 * @param [in]  descriptor - file system descriptor
	 * @param [out] file       - managed file instance (if any)
	 */
	bool findFile(const char* path, const FileSystemDescriptor& descriptor, managed_file::File*& file);

	/**
	 * Insert the managed file into the set.
	 * The key is file fully qualified local path
	 *
	 * @param [in]     path       - file fqp
	 * @param [in]     descriptor - file system descriptor
	 * @param [in/out] file       - managed file
	 *
	 * @return true is the file was inserted to the cache, false otherwise
	 */
	bool addFile(const char* path, const FileSystemDescriptor& descriptor, managed_file::File*& file);

	struct StrExpComp
	{
	   bool operator()(const std::string & str, const managed_file::File& file) const
	   {
		   return str.compare(file.fqp()) == 0;
	   }

	   bool operator()(const managed_file::File& file, const std::string & str) const
	   {
		   return file.fqp().compare(str) == 0;
	   }
	};
};

}

#endif /* CACHE_LAYER_REGISTRY_H_ */
