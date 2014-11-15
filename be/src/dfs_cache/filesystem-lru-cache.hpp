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

#include "dfs_cache/managed-file.hpp"
#include "dfs_cache/lru-cache.hpp"

namespace impala{

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

	/** tell item is idle now (no usage so far). Is needed in the cleanup cache scenario
	 *  @param file - file to query for status
	 */
    inline bool isItemIdle(managed_file::File* file){
    	return file->state() == managed_file::State::FILE_IS_IDLE;
    }

    /** get the current file timestamp
     * @param file - file to query for current timestamp (last access)
     */
    inline boost::posix_time::ptime getTimestamp(managed_file::File* file){
    	return file->last_access();
    }

    /** delete file object, delete file from file system   */
    inline bool deleteFile(managed_file::File* file){
    	if(!isItemIdle(file))
    		return false;

    	// no usage so far, mark the file for deletion:
    	file->state(managed_file::State::FILE_IS_MARKED_FOR_DELETION);

    	// delete the file from file system
    	file->drop();

    	// get rid of file metadata object:
    	delete file;
    	return true;
    }

    /**
     * For autoload requested item scenario.
     * If file requested from the cache was not located there, we run the CacheManager "prepare request" scenario here
     * in order to load requested file.
     *
     * @param path - local path fully describe the file to get
     *
     * @return loaded file is succeeded to load it, nullptr otherwise
     */
    inline managed_file::File* loadFromFullLocalPath( std::string path )
    {
    	return nullptr;
    }

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

public:
    /**
     * construct the File System LRU cache
     *
     * @param capacity - initial cache capacity limit
     */
    FileSystemLRUCache(std::size_t capacity) : LRUCache<managed_file::File>(boost::posix_time::microsec_clock::local_time(), capacity){
    	//m_isValid = boost::bind(boost::mem_fn(&FileSystemLRUCache::IsDataValid), this);
    	m_tellCapacityLimitPredicate = boost::bind(boost::mem_fn(&FileSystemLRUCache::getCapacity), this);
    	m_tellWeightPredicate = boost::bind(boost::mem_fn(&FileSystemLRUCache::getWeight), this, _1);
    	m_tellItemIsIdle = boost::bind(boost::mem_fn(&FileSystemLRUCache::isItemIdle), this, _1);

    	m_tellItemTimestamp =  boost::bind(boost::mem_fn(&FileSystemLRUCache::getTimestamp), this, _1);
    	m_itemDeletionPredicate = boost::bind(boost::mem_fn(&FileSystemLRUCache::deleteFile), this, _1);

    	LRUCache<managed_file::File>::GetKeyFunc<std::string> gkf = [&](managed_file::File* file)->int { return file->fqp(); };
    	LRUCache<managed_file::File>::LoadItemFunc<std::string> lif = boost::bind(boost::mem_fn(&FileSystemLRUCache::loadFromFullLocalPath), this, _1);
    	m_idxFileLocalPath = addIndex<std::string>( "fqp", gkf, 0);
    }


    /**
     * Get the file by its local path
     *
     * @param path -file local path
     *
     * @return file is one found, exception is thrown is nothing found
     */
    inline managed_file::File findByFileLocalPath( std::string path ) {
    	managed_file::File file =  m_idxFileLocalPath->operator [](path);
    	return file;
    }

    /** reset the cache */
    inline void reset() {
 	   this->clear();
    }

    /** add the file into the cache */
    inline void add(managed_file::File* file){
 	   LRUCache<managed_file::File>::add(file);
    }

    /** remove the file by its local path
     * @param path - local path of file to be removed from cache
     *  */
    inline void removeByFileLocalPath(std::string path){
 	   m_idxFileLocalPath->remove(path);
    }
};

}

#endif /* FILESYSTEM_LRU_CACHE_HPP_ */
