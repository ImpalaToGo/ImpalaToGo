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

status::StatusInternal CacheLayerRegistry::setupFileSystem(FileSystemDescriptor & fsDescriptor){
	/* We may receive here following FileSystem configurations:
	 * 1. {"default", 0} - in this case we need to delegate the host and port resolution to the Hadoop FileSystem class
	 *    which will locate the CLASSPATH's available core-site.xml and get the FS host and port from URI defined in
	 *    <property>
  	  	  	  <name>fs.defaultFS</name>
  	  	  	  <value>hdfs://namenode_hostname:port</value>
		  </property>

	   2. {NULL, 0} - in this case local file system is constructed

	   3. {hostname, port} - in this case we construct the FileSystem explicitly
	 *
	 */
	if(fsDescriptor.host == constants::DEFAULT_FS){
		// run resolution scenario via hadoop filesystem:
		int status = FileSystemDescriptorBound::resolveFsAddress(fsDescriptor);
		if(status){
			LOG (ERROR) << "Failed to resolve default FileSystem. " << "n";
			return status::StatusInternal::DFS_ADAPTOR_IS_NOT_CONFIGURED;
		}

		// FileSystem is resolved. Proceed with updated file system descriptor
	}

	boost::mutex::scoped_lock lockconn(m_connmux);
	if (m_filesystems[fsDescriptor.dfs_type].count(fsDescriptor.host)) {
		// descriptor is already a part of the registry, nothing to add
		return status::StatusInternal::OK;
	}
	// create the FileSystem-bound descriptor and assign the File System adaptor to it
	boost::shared_ptr<FileSystemDescriptorBound> descriptor(
			new FileSystemDescriptorBound(fsDescriptor));
	// and insert new {key-value} under the appropriate FileSystem type
	m_filesystems[fsDescriptor.dfs_type].insert(
			std::make_pair(fsDescriptor.host, descriptor));
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
