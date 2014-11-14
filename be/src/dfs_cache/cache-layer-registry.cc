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
		if(m_filesystems[fsDescriptor.dfs_type].count(fsDescriptor.host)){
			// descriptor is already a part of the registry, nothing to add
			return status::StatusInternal::OK;
		}
		// create the FileSystem-bound descriptor and assign the File System adaptor to it
		boost::shared_ptr<FileSystemDescriptorBound> descriptor(new FileSystemDescriptorBound(fsDescriptor));
    	// and insert new {key-value} under the appropriate FileSystem type
		m_filesystems[fsDescriptor.dfs_type].insert(std::make_pair(fsDescriptor.host, descriptor));
    	return status::StatusInternal::OK;
}

const boost::shared_ptr<FileSystemDescriptorBound>* CacheLayerRegistry::getFileSystemDescriptor(const FileSystemDescriptor & fsDescriptor){
		boost::mutex::scoped_lock lock(m_connmux);
          if(m_filesystems.count(fsDescriptor.dfs_type) > 0 && m_filesystems[fsDescriptor.dfs_type].count(fsDescriptor.host) > 0){
        	  return &(m_filesystems[fsDescriptor.dfs_type][fsDescriptor.host]);
          }
          return nullptr;
	}

bool CacheLayerRegistry::getFileByPath(const char* key, managed_file::File*& file)
	{
	    return true;
	}

bool CacheLayerRegistry::addFileByPath(managed_file::File file)
{
	return true;
}
}

#endif /* CACHE_LAYER_REGISTRY_CC_ */
