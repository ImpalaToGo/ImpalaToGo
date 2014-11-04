/*
 * @file filesystem-descriptor-bound.hpp
 * @brief definition of hadoop FileSystem mediator (mainly types translator and connections handler)
 *
 * @date   Oct 9, 2014
 * @author elenav
 */

#ifndef FILESYSTEM_DESCRIPTOR_BOUND_HPP_
#define FILESYSTEM_DESCRIPTOR_BOUND_HPP_

#include <utility>
#include "dfs_cache/common-include.hpp"
#include "dfs_cache/dfs-connection.hpp"

namespace impala{


/**
 * FileSystemDescriptor bound to hadoop FileSystem
 * Holds and manages connections to this file system.
 * Connections are hold in the list.
 * Lists have the important property that insertion
 * and splicing do not invalidate iterators to list elements, and that even removal
 * invalidates only the iterators that point to the elements that are removed
 */
class FileSystemDescriptorBound{
private:
	boost::mutex                                      m_mux;
	std::list<boost::shared_ptr<dfsConnection> >      m_connections;     /**< cached connections to this File System */
	FileSystemDescriptor                              m_fsDescriptor;    /**< File System connection details as configured */

	/** helper predicate to find free non-error connections. */
	struct freeConnectionPredicate
	{
		bool operator() ( const boost::shared_ptr<dfsConnection> & connection )
	    {
	        return connection->state == dfsConnection::FREE_INITIALIZED;
	    }
	};

	struct anyNonInitializedConnectionPredicate{
		bool operator() ( const boost::shared_ptr<dfsConnection> & connection )
	    {
	        return connection->state != dfsConnection::BUSY_OK && connection->state != dfsConnection::FREE_INITIALIZED;
	    }
	};

	/** Encapsulates File System connection logic */
	fsBridge connect();

public:
	inline FileSystemDescriptorBound(const FileSystemDescriptor & fsDescriptor) {
		// copy the FileSystem configuration
		m_fsDescriptor = fsDescriptor;
	}

	inline const FileSystemDescriptor& descriptor() { return m_fsDescriptor; }
	/**
	 * get free FileSystem connection
	 */
	raiiDfsConnection getFreeConnection();
};
}

#endif /* FILESYSTEM_DESCRIPTOR_BOUND_HPP_ */
