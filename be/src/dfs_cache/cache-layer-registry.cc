/*
 * cache-layer-registry.cc
 *
 *  Created on: Oct 9, 2014
 *      Author: elenav
 */

#ifndef CACHE_LAYER_REGISTRY_CC_
#define CACHE_LAYER_REGISTRY_CC_

#include <boost/scoped_ptr.hpp>
#include "dfs_cache/cache-layer-registry.hpp"

namespace impala{

boost::scoped_ptr<CacheLayerRegistry> CacheLayerRegistry::instance_;

void CacheLayerRegistry::init() {
  if(CacheLayerRegistry::instance_.get() == NULL)
	  CacheLayerRegistry::instance_.reset(new CacheLayerRegistry());
}

status::StatusInternal CacheLayerRegistry::setupFileSystem(const FileSystemDescriptor & fsDescriptor){
		boost::mutex::scoped_lock lockconn(m_connmux);
		if(dfsConnections[fsDescriptor.dfs_type].count(fsDescriptor.host)){
			// descriptor is already a part of the registry, nothing to add
			return status::StatusInternal::OK;
		}
		// create the dfs-bound descriptor and assign the DFS adaptor to it
		boost::shared_ptr<FileSystemDescriptorBound> descriptor(new FileSystemDescriptorBound(fsDescriptor));
    	// and insert new {key-value} under the apropriate DFS
    	dfsConnections[fsDescriptor.dfs_type].insert(std::make_pair(fsDescriptor.host, descriptor));
    	return status::StatusInternal::OK;
}

const boost::shared_ptr<FileSystemDescriptorBound>* CacheLayerRegistry::getFileSystemDescriptor(const FileSystemDescriptor & fsDescriptor){
		boost::mutex::scoped_lock lock(m_connmux);
          if(dfsConnections.count(fsDescriptor.dfs_type) > 0 && dfsConnections[fsDescriptor.dfs_type].count(fsDescriptor.host) > 0){
        	  return &(dfsConnections[fsDescriptor.dfs_type][fsDescriptor.host]);
          }
          return nullptr;
	}

bool CacheLayerRegistry::getFileByPath(const char* key, ManagedFile::File*& file)
	{
		file = nullptr;
		boost::mutex::scoped_lock lock(m_cachemux);
		FileRegistry::iterator it = cache.find(key, StrExpComp());
		if( it == cache.end() )
	    	return false;
		file = &*it;
	    return true;
	}

bool CacheLayerRegistry::addFileByPath(ManagedFile::File file)
{
	boost::mutex::scoped_lock lock(m_cachemux);
    boost::intrusive::set<ManagedFile::File>::insert_commit_data insert_data;

    bool success = cache.insert_check(file.fqp(), StrExpComp(), insert_data).second;
    if(success) cache.insert_commit(file, insert_data);
	   return success;
}
}

#endif /* CACHE_LAYER_REGISTRY_CC_ */
