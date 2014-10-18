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

void CacheLayerRegistry::setupDFSPluginFactory(const boost::shared_ptr<dfsAdaptorFactory> & factory){
	// just share the ownership of a factory:
	m_dfsAdaptorFactory = factory;
}

status::StatusInternal CacheLayerRegistry::setupNamenode(const NameNodeDescriptor & namenode){
		boost::mutex::scoped_lock lockconn(m_connmux);
		if(dfsConnections[namenode.dfs_type].count(namenode.host)){
			// descriptor is already a part of the registry, nothing to add
			return status::StatusInternal::OK;
		}
		// create the dfs-bound descriptor and assign the DFS adaptor to it
    	boost::shared_ptr<RemoteAdaptor>* adaptor;
    	dfsAdaptorFactory::AdaptorState status = m_dfsAdaptorFactory->getAdaptor(namenode.dfs_type, adaptor);
    	if(adaptor == nullptr || status != dfsAdaptorFactory::INITIALIZED)
    		return status::StatusInternal::DFS_ADAPTOR_IS_NOT_CONFIGURED;

		// create the dfs-bound descriptor and assign the DFS adaptor to it
		boost::shared_ptr<NameNodeDescriptorBound> descriptor(new NameNodeDescriptorBound(*adaptor, namenode));
    	// and insert new {key-value} under the apropriate DFS
    	dfsConnections[namenode.dfs_type].insert(std::make_pair(namenode.host, descriptor));
    	return status::StatusInternal::OK;
}

const boost::shared_ptr<NameNodeDescriptorBound>* CacheLayerRegistry::getNamenode(const NameNodeDescriptor & namenodeDescriptor){
		boost::mutex::scoped_lock lock(m_connmux);
          if(dfsConnections.count(namenodeDescriptor.dfs_type) > 0 && dfsConnections[namenodeDescriptor.dfs_type].count(namenodeDescriptor.host) > 0){
        	  return &(dfsConnections[namenodeDescriptor.dfs_type][namenodeDescriptor.host]);
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
