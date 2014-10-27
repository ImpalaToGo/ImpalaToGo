/*
 * dfs-adapter.hpp
 *
 * @date   Oct 9, 2014
 * @author elenav
 */

#ifndef DFS_ADAPTER_HPP_
#define DFS_ADAPTER_HPP_

#include <utility>
#include "dfs_cache/common-include.hpp"
#include "dfs_cache/dfs-connection.hpp"

namespace impala{


/**
 * Namenode descriptor bound to dfs adaptor
 * Holds and manages connections to this namenode.
 * Connections are hold in the list.
 * Lists have the important property that insertion
 * and splicing do not invalidate iterators to list elements, and that even removal
 * invalidates only the iterators that point to the elements that are removed
 */
class NameNodeDescriptorBound{
private:
	boost::mutex                                      m_mux;
	std::list<boost::shared_ptr<dfsConnection> >      m_connections;     /**< cached connections to this Name Node */
	boost::shared_ptr<RemoteAdaptor>                  m_dfsAdaptor;      /**< Adaptor instantiated depending on DFS type */
	NameNodeDescriptor                                m_namenode;        /**< Name Node connection details as configured */

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

public:
	inline NameNodeDescriptorBound(const boost::shared_ptr<RemoteAdaptor> & adaptor,
			const NameNodeDescriptor & namenode) {
		// copy the namenode configuration
		m_namenode = namenode;

		// became on of a clients of dfs adaptor
		m_dfsAdaptor = adaptor;
	}

	/**
	 * Publish DFS adaptor
	 *
	 * @return DFS adaptor
	 */
	inline const boost::shared_ptr<RemoteAdaptor>& adaptor() { return m_dfsAdaptor; }

	inline const NameNodeDescriptor& descriptor() { return m_namenode; }
	/**
	 * get free namenode connection
	 */
	raiiDfsConnection getFreeConnection();
};
}

#endif /* DFS_ADAPTER_HPP_ */
