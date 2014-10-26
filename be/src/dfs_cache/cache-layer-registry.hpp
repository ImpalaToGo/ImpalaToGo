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

#include <boost/intrusive/set.hpp>
#include <boost/shared_ptr.hpp>

#include "dfs_cache/cache-definitions.hpp"
#include "dfs_cache/common-include.hpp"
#include "dfs_cache/dfs-adaptor-factory.hpp"
#include "dfs_cache/namenode-descriptor-bound.hpp"

/**
 * @namespace impala
 */
namespace impala {

typedef std::map<dfs::DFS_TYPE, std::map<std::string, boost::shared_ptr<NameNodeDescriptorBound> > > DFSConnections;
typedef std::map<dfs::DFS_TYPE, boost::shared_ptr<RemoteAdaptor> > DFSAdaptors;

/**
 * Represent cache data registry.
 */
class CacheLayerRegistry{
private:
	/** Singleton instance. Instantiated in Init(). */
	static boost::scoped_ptr<CacheLayerRegistry> instance_;

	FileRegistry        cache;            					   /**< Registry of cache-managed files */
	DFSConnections      dfsConnections;    					   /**< Registry of DFS connections */

	boost::shared_ptr<dfsAdaptorFactory>  m_dfsAdaptorFactory; /**< dfs adaptor factory */

	std::string m_localstorageRoot;       /**< path to local file system storage root */

	boost::mutex m_cachemux;           /**< mutex for file cache collection */
	boost::mutex m_connmux;            /**< mutex for connections collection */
	boost::mutex m_adaptorsmux;        /**< mutex for adapters collection */

	CacheLayerRegistry() { };
	CacheLayerRegistry(CacheLayerRegistry const& l);            // disable copy constructor
	CacheLayerRegistry& operator=(CacheLayerRegistry const& l); // disable assignment operator

public:

	~CacheLayerRegistry() { LOG (INFO) << "cache layer registry destructor" << "\n"; }
    static CacheLayerRegistry* instance() { return CacheLayerRegistry::instance_.get(); }

	/** *************************** External configuration  ************************************************************/

    /** Initialize registry. Call this before any Registry usage */
    static void init();

    /** Setter for Local storage root file system path */
    inline void localstorage(const std::string& localpath) {m_localstorageRoot = localpath;}

    /** Getter for Local storage root file system path */
    inline std::string localstorage() {return m_localstorageRoot;}

	/**
	 * setup remote DFS adaptors factory
	 *
	 * @param factory - DFS Plugins factory
	 */
	void setupDFSPluginFactory(const boost::shared_ptr<dfsAdaptorFactory> & factory);

	/**
	 * Setup namenode
	 *
	 * @param namenode - namenode connection details
	 *
	 * @return Operation status
	 */
	status::StatusInternal setupNamenode(const NameNodeDescriptor & namenode);


	/** ***************************  DFS related registry API  ***********************************************************/

	/**
	 * Get connected namenode descriptor by its connection descriptor
	 *
	 * @param namenode - configured namenode connection details
	 *
	 * @return namenode adaptor
	 */
	const boost::shared_ptr<NameNodeDescriptorBound>* getNamenode(const NameNodeDescriptor & namenodeDescriptor);

	/** *************************** Local file system registry API *****************************************************/

	/**
	 * Get the File object by its path
	 *
	 * @param [in]  key  - file fqp
	 * @param [out] file - managed file instance (if any)
	 */
	bool getFileByPath(const char* key, ManagedFile::File*& file);

	/**
	 * Insert the managed file into the set.
	 * The key is file fully qualified local path
	 *
	 * @param key  - file fqp
	 * @param file - managed file instance
	 */
	bool addFileByPath( ManagedFile::File file);

	struct StrExpComp
	{
	   bool operator()(const std::string & str, const ManagedFile::File& file) const
	   {  //return std::strcmp(str, file.fqp().c_str()) < 0;
		   return str.compare(file.fqp()) == 0;
	   }

	   bool operator()(const ManagedFile::File& file, const std::string & str) const
	   {  //return std::strcmp(file.fqp().c_str(), str) < 0;
		   return file.fqp().compare(str) == 0;
	   }
	};
};

}

#endif /* CACHE_LAYER_REGISTRY_H_ */
