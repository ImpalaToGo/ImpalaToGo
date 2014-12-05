/*
 * @file  filesystem-lru-cache.hpp
 * @brief implementation of LRU cache for local File System - remote FileSystems Cache.
 *
 * Publish API to operate with cache content basing on defined indexes,
 * which is "index by file local path" only right now.
 *
 * Provides underlying LRU concept with cleanup rule defined by
 * TellCapacityLimitPredicate predicate (for implementation see and edit if needed below)
 *
 * @date   Nov 14, 2014
 * @author elenav
 */

#ifndef FILESYSTEM_LRU_CACHE_HPP_
#define FILESYSTEM_LRU_CACHE_HPP_

#include <list>

#include "dfs_cache/managed-file.hpp"
#include "dfs_cache/lru-cache.hpp"

namespace impala{

namespace ph = std::placeholders;

/** Describe the storage for cached files metadata, the LRU cache.
 * Responsibilities:
 * - describe all cached metadata;
 * - provide fast metadata access by defined index (currently by full file path);
 * - provide the auto-cleanup routed by configurable predicate (rule).
 * Currently - the cleanup trigger is "configured capacity limit, Mb, is exceeded",
 * cleanup behavior is to delete least used files from local cache along with any mention
 * of them.
 *
 */
class FileSystemLRUCache : LRUCache<managed_file::File>{
private:
	IIndex<std::string>* m_idxFileLocalPath = nullptr; /**< the only index is for file local path  */
    std::size_t          m_capacityLimit;              /**< capacity limit for underlying LRU cache. For cleanup tuning */
    std::string          m_root;                       /**< root directory to manage */

    boost::condition_variable m_deletionHappensCondition; /**< deletion conditio variable */
    boost::mutex           m_deletionsmux;                /**< mux to protect deletions list */
    std::list<std::string> m_deletionList;                /**< list of pending deletion */

	/** try mark item for deletion
	 *  @param file - file to mark for deletion
	 *
	 *  @return true if file was marked for deletion and can be removed,
	 *  false otherwise
	 */
    inline bool markForDeletion(managed_file::File* file){
    	return file->mark_for_deletion();
    }

    /** get the current file timestamp
     * @param file - file to query for current timestamp (last access)
     */
    inline boost::posix_time::ptime getTimestamp(managed_file::File* file){
    	return file->last_access();
    }

    /** Set the current file timestamp
     * @param file      - file to update with current timestamp (last access)
     * @param timestamp - updated timstamp
     */
    inline void updateTimestamp(managed_file::File* file, const boost::posix_time::ptime& timestamp){
    	file->last_access(timestamp);
    }

    /** delete file object, delete file from file system
     * @param file       - file to remove
     * @param physically - flag, indicates whether physical removal is required
     *
     * @return operation status, true if the requested scenario succeeded
     */
    bool deleteFile(managed_file::File* file, bool physically = true);

    /** reply with configured capacity measurement units
     *  In place as there's the reason to assume that capacity may change during
     *  the system life due to external reasons...
     *  TODO: dynamic capacity planning right here
     *
     * @return capacity limit as configured.
     */
    inline std::size_t getCapacity(){
    	return m_capacityLimit;
    }

    /**
     * tell file weight in regards to capacity units
     */
    inline std::size_t getWeight(managed_file::File* file){
    	if(file == nullptr)
    		return 0;
    	return file->size();
    }

    /*
     * construct new object of File basing on its path
     *
     * @param path - file path
     *
     * @return constructed file object if its has correct configuration and nullptr otherwise
     */
    managed_file::File* constructNew(std::string path){
    	managed_file::File* file = new managed_file::File(path.c_str());
     	if(file->state() == managed_file::State::FILE_IS_FORBIDDEN){
    		delete file;
    		file = nullptr;
    	}
     	else
     		// mark file as "in progress" immediately, before to publish it to the outer world:
     		file->state(managed_file::State::FILE_IS_IN_USE_BY_SYNC);
    	return file;
    }

    /**
     * run continuation scenario, here:
     * -> run prepare scenario on Cache Manager
     *
     * @param file - file which intended to be prepared
     *
     */
    void sync(managed_file::File* file);

public:

    /**
     * construct the File System LRU cache
     *
     * @param capacity - initial cache capacity limit
     * @param root     - defines root folder for local cache storage
     * @param autoload - flag, indicates whether auto-load should be performed once the file is requested from cache by its name.
     * Currently is true by default.
     */
    FileSystemLRUCache(long long capacity, const std::string& root, bool autoload = true) :
    		LRUCache<managed_file::File>(boost::posix_time::microsec_clock::local_time(), capacity), m_root(root){

    	LOG (INFO) << "LRU cache capacity limit = " << std::to_string(capacity) << "\n";

    	m_tellCapacityLimitPredicate = boost::bind(boost::mem_fn(&FileSystemLRUCache::getCapacity), this);
    	m_tellWeightPredicate = boost::bind(boost::mem_fn(&FileSystemLRUCache::getWeight), this, _1);
    	m_tellItemIsIdle = boost::bind(boost::mem_fn(&FileSystemLRUCache::markForDeletion), this, _1);

    	m_tellItemTimestamp =  boost::bind(boost::mem_fn(&FileSystemLRUCache::getTimestamp), this, _1);
    	m_acceptAssignedTimestamp = boost::bind(boost::mem_fn(&FileSystemLRUCache::updateTimestamp), this, _1, _2);

    	m_itemDeletionPredicate = boost::bind(boost::mem_fn(&FileSystemLRUCache::deleteFile), this, _1, _2);

    	LRUCache<managed_file::File>::GetKeyFunc<std::string> gkf = [&](managed_file::File* file)->std::string { return file->fqp(); };
    	LRUCache<managed_file::File>::LoadItemFunc<std::string>      lif = 0;
    	LRUCache<managed_file::File>::ConstructItemFunc<std::string> cif = 0;

    	// initialize autoload-related predicates only in auto-load configuration:
    	if(autoload){
    		lif = boost::bind(boost::mem_fn(&FileSystemLRUCache::sync), this, _1);
    		cif = boost::bind(boost::mem_fn(&FileSystemLRUCache::constructNew), this, _1);
    	}

    	// finally define index "by file fully qualified local path"
    	m_idxFileLocalPath = addIndex<std::string>( "fqp", gkf, lif, cif);
    }

    /** reload the cache.
     *
     *  @param root - the actual root path to reload from
     *
     *  @return true if cache was reloaded, false otherwise
     */
    bool reload(const std::string& root);

    /**
     * Get the file by its local path
     *
     * @param path -file local path
     *
     * @return file if one found, nullptr otherwise
     */
    managed_file::File* find(std::string path);

    /** reset the cache */
    inline void reset() {
 	   this->clear();
    }

    /** add the file into the cache
     * @param path - fqp
     *
     * @return indication of fact that file is in the registry
     */
    bool add(std::string path, managed_file::File*& file);

    /** remove the file from cache by its local path
     * @param path - local path of file to be removed from cache
     *  */
    inline bool remove(std::string path){
 	   return m_idxFileLocalPath->remove(path, true);
    }
};

}

#endif /* FILESYSTEM_LRU_CACHE_HPP_ */
