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
#include <unordered_map>
#include <memory>
#include <boost/function.hpp>
#include <boost/bind.hpp>

#include <boost/shared_ptr.hpp>

#include "dfs_cache/cache-definitions.hpp"
#include "dfs_cache/common-include.hpp"
#include "dfs_cache/filesystem-descriptor-bound.hpp"
#include "dfs_cache/utilities.hpp"

/** pointer hash utility */
template<typename Tval>
struct _PointerHash {
    size_t operator()(const Tval* val) const {
        static const size_t shift = (size_t)log2(1 + sizeof(Tval));
        return (size_t)(val) >> shift;
    }
    size_t operator()(const Tval* val, size_t size) const {
           static const size_t shift = (size_t)log2(1 + size);
           return (size_t)(val) >> shift;
       }
};

/** Hash for dfsFile */
namespace std {
    template <>
        class hash<dfsFile> {
        public :
            size_t operator()(const dfsFile &file ) const
            {
                return _PointerHash<void>()(file->file, file->size);
            }
    };
};

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
 * Set of file handles that are stored for "CREATE FROM SELECT" scenario:
 * key   - local file handle (cache)
 * value - remote file handle (bound FileSystem)
 */
typedef std::unordered_map<dfsFile, dfsFile> CreateFromSelectFiles;
typedef std::unordered_map<dfsFile, dfsFile>::iterator itCreateFromSelect;

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

	CreateFromSelectFiles m_createFromSelect;     /**< local and remote file handles pairs, created in "CREATE FROM SELECT" scenario*/
    boost::mutex          m_createfromselect_mux; /**< mutex to protect "CREATE FROM SELECT" file write scenario */

	std::string m_localstorageRoot;   /**< path to local file system storage root */

	boost::mutex m_cachemux;           /**< mutex for file cache collection */
	boost::mutex m_connmux;            /**< mutex for connections collection */
	boost::mutex m_adaptorsmux;        /**< mutex for adapters collection */

	volatile bool m_valid;             /**< flag, indicates that registry is in the valid state */

	const double m_available_capacity_ratio = 0.85; /**< ratio for setting "cache capacity", percent from available
	 	 	 	 	 	 	 	 	 	 	 	     * root storage space */

    dfsFileInfo* getFileInfo(const char* path, FileSystemDescriptor descriptor){
    	boost::shared_ptr<FileSystemDescriptorBound> fsAdaptor = (*CacheLayerRegistry::instance()->getFileSystemDescriptor(descriptor));

    	LOG (INFO) << "Get file path for \"" << path << "\"\n";
    	if (!fsAdaptor) {
    		LOG (ERROR) << "Unable to create new file from path \"" << path <<
    				"\". No filesystem adaptor configured for FileSystem \"" << descriptor.dfs_type << ":" <<
    				descriptor.host << "\"" << "\n";
    		// no namenode adaptor configured
    		return NULL;
    	}

    	raiiDfsConnection connection(fsAdaptor->getFreeConnection());
    	if (!connection.valid()) {
    		LOG (ERROR)<< "Unable to create new file from path \"" << path <<
    				"\". No connection to dfs available \" on FileSystem \"" <<
    				descriptor.dfs_type << ":" << descriptor.host << "\"" << "\n";
    		return NULL;
    	}

    	// ask remote part about file info:
    	dfsFileInfo* info = fsAdaptor->fileInfo(connection, path);
    	return info;
    }

    void freeFileInfo(dfsFileInfo* info, int num){
  	   // free file info:
  	   FileSystemDescriptorBound::freeFileInfo(info, num);
     }

    /**
     * Ctor. Instance specific initializations.
     * @param mem_limit_percent - percent of free memory on the cache location to be utilized by cache.
     * @param root              - root location for cache
     * @param timeslice         - time slice duration, for age buckets management.
     * @param size_hard_limit   - hard cache size limit. Mostly for testing purposes.
     *
     */
	CacheLayerRegistry(int mem_limit_percent = 0, const std::string& root = "",
			boost::posix_time::time_duration timeslice = boost::posix_time::hours(-1),
			uintmax_t size_hard_limit = 0) {
		m_valid = false;
		// flag, indicates that fixed hard cache size is configured, only needed is to guarantee we have space enough
		// according to requested cache size
		bool hardsize = size_hard_limit != 0;

        std::string _root = root;

		if(_root.empty())
			_root = constants::DEFAULT_CACHE_ROOT;
		if(!localstorage(_root)){
			LOG (ERROR) << "Cache Layer is not initialized due to invalid cache location \"" << root << "\"";
		}

		uintmax_t covered = utilities::get_dir_busy_space(m_localstorageRoot);
		LOG (INFO) << "Cache load : busy space : \"" << std::to_string(covered) << "\"\n";

		// available bytes:
		uintmax_t available = 0;
		// percent from available cache data bytes configured to use:
		double percent = 0;

		if(hardsize){
			percent = 1.0;
		}
		else{
			// Get the max 85% of available space on the path specified + space covered already by cache root content:
			percent = ( (mem_limit_percent > 0 ) && ( mem_limit_percent <= 85) ) ? mem_limit_percent / 100.0 : m_available_capacity_ratio;
		}

		available = utilities::get_free_space_on_disk(m_localstorageRoot) * percent;
		LOG (INFO) << "Cache load : available space : \"" << std::to_string(available) << "\"; LRU percent from available space = \"" <<
				std::to_string(percent) << "\".";

		available = covered + available;
        if((hardsize && (size_hard_limit > available)) || (available == 0))
        	return;

        // if cache size is hardly configured, just assign the available space limit to this hard size
        if(hardsize)
        	available = size_hard_limit;

    	LOG (INFO) << "Space limit available, bytes = \"" << std::to_string(available) << "\" on path \""
    			<< m_localstorageRoot << "\".\n";

		// create the autoload LRU cache
    	managed_file::File::GetFileInfo getfileinfo = boost::bind(boost::mem_fn(&CacheLayerRegistry::getFileInfo), this, _1, _2);
    	managed_file::File::FreeFileInfo freefileinfo = boost::bind(boost::mem_fn(&CacheLayerRegistry::freeFileInfo), this, _1, _2);
		m_cache = new FileSystemLRUCache(available, m_localstorageRoot, getfileinfo, freefileinfo, timeslice, true);
		m_valid = true;
	}

	CacheLayerRegistry(CacheLayerRegistry const& l);            // disable copy constructor
	CacheLayerRegistry& operator=(CacheLayerRegistry const& l); // disable assignment operator

    /** Setter for Local storage root file system path */
    inline bool localstorage(std::string& localpath) {
    	const std::string alias = localpath;
		// reworked to check for possible symlink as an input. We ned to resolve symlink here to get the real physical
    	// location. We cannot rely on symlinks internally
    	LOG (INFO) << "Original path specified : \"" << alias << "\", run link resolve to a physical path.\n";
    	// resolve symlink:
    	localpath = boost::filesystem::canonical(alias).string();
    	if(localpath.empty()){
    		LOG (ERROR) << "Alias \"" << alias << "\" was not resolved to any physical path. \n";
    		localpath = alias;
    		return false;
    	}
    	// whether input path is specified with a trailing slash or not,
    	// the trailing slash would be removed as a side effect of canonize operation. This will be handled below.
    	LOG (INFO) << "Alias \"" << alias << "\" is resolved to a physical path \"" << localpath << "\".\n";

    	m_localstorageRoot = localpath;

        // add file separator if no specified:
    	bool trailing = impala::utilities::endsWith(m_localstorageRoot, fileSeparator);
    	if(!trailing){
    		m_localstorageRoot += fileSeparator;
    	}
    	return true;
    }

    /** reload the cache */
    inline bool reload(){
    	if(!m_valid)
    		return false;

    	// reload the cache:
    	if(m_cache->reload(m_localstorageRoot))
    		return m_valid = true;
    	return m_valid = false;
    }

public:

	~CacheLayerRegistry() {
		delete m_cache;
		LOG (INFO) << "cache layer registry destructor" << "\n";
	}
    static CacheLayerRegistry* instance() { return CacheLayerRegistry::instance_.get(); }

	/** *************************** External configuration  ************************************************************/

    /** Initialize registry. Call this before any Registry usage
     *
     * @param mem_limit_percent - limit of available memory on @a root, in percents, that can be
     * potentially consumed by cache
     *
     * @param root              - local cache root - file system absolute path
     * @param timeslice         - time slice duration, for age buckets management.
     * @param size_hard_limit   - hard size limit to configure the cache with. Once specified,
     * mem_limit_percent is ignored
     *
     * @return cache init status, false if cache init failed. True on success
     */
    static bool init(int mem_limit_percent = 0, const std::string& root = "",
    		boost::posix_time::time_duration timeslice = boost::posix_time::hours(-1),
    		int size_hard_limit = 0);

    /**
     * Return cache validity status.
     *
     * @return true if the cache is valid, false otherwise
     */
    inline bool valid() { return m_valid; }

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
	 * Get the File object by its relative path within the @a descriptor of related file system.
	 * To be called when file path should be resolved from @a path and @a descriptor (when query arrives)
	 *
	 * @param [in]  path       - file fqp
	 * @param [in]  descriptor - file system descriptor
	 * @param [out] file       - managed file instance (if any)
	 */
	bool findFile(const char* path, const FileSystemDescriptor& descriptor, managed_file::File*& file);

	/**
	 * Get the file object by its fully qualified path. Should be called for internal cache usage on
	 * existing local files
	 *
	 * @param [in]  path       - file fqp
	 * @param [out] file       - managed file instance (if any)
	 *
	 */
	bool findFile(const char* path, managed_file::File*& file);

	/**
	 * Insert the managed file into the set.
	 * The key is file fully qualified local path
	 *
	 * @param [in]     path         - file fqp
	 * @param [in]     descriptor   - file system descriptor
	 * @param [in/out] file         - managed file
	 * @param [in]     creationFlag - file creation option
	 *
	 * @return true is the file was inserted to the cache, false otherwise
	 */
	bool addFile(const char* path, const FileSystemDescriptor& descriptor, managed_file::File*& file,
			managed_file::NatureFlag creationFlag);

	/**
	 * Delete file from cache and from file system
	 * @param descriptor  - file system descriptor
	 * @param path 		  - relative path to a file to remove
	 * @param physically  - flag, indicates whether physical file removal is required
	 *
	 * @return status of operation, true means that file was removed according scenario.
	 * false - operation was not success due to reasons
	 */
	bool deleteFile(const FileSystemDescriptor &descriptor, const char* path, bool physically = true);

	/**
	 * Delete path from cache and from file system
	 * @param descriptor  - file system descriptor
	 * @param path 		  - relative path to remove along with a possible content
	 *
	 * @return status of operation, true means that path was removed according scenario.
	 * false - operation was not success due to reasons
	 */
	bool deletePath(const FileSystemDescriptor &descriptor, const char* path);

	/**
	 * start new "CREATE FROM SELECT" scenario.
	 *
	 * @param local  - handle to local file
	 * @param remote - handle to remote file
	 *
	 * @return operation status, true on success (scenario is registered), false otehrwise
	 * (do not use the scenario)
	 */
	bool registerCreateFromSelectScenario(const dfsFile& local, const dfsFile& remote);

	/**
	 * Completes "CREATE FROM SELECT" scenario.
	 *
	 * @param local - handle to local file
	 *
	 * @return operation status, true on success (scenario is unregistered)
	 */
	bool unregisterCreateFromSelectScenario(const dfsFile& local);

	/**
	 * Retrieve "CREATE FROM SELECT" scenario
	 *
	 * @param [in] local   - handle to local file
	 * @param [out] exists - flag, indicates that the requested scenario exists
	 *
	 * @return handle to remote file participating in scenario, NULL is scenario is not exist
	 */
	dfsFile getCreateFromSelectScenario(const dfsFile& local, bool& exists);

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
