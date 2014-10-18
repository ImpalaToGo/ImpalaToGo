/*
 * namenode-descriptor-bound.cc
 *
 *  Created on: Oct 10, 2014
 *      Author: elenav
 */

#include "dfs_cache/namenode-descriptor-bound.hpp"

namespace impala {

boost::shared_ptr<dfsConnection>* NameNodeDescriptorBound::getFreeConnection(){
	freeConnectionPredicate predicateFreeConnection;

		boost::mutex::scoped_lock(m_mux);
		std::list<boost::shared_ptr<dfsConnection> >::iterator i1;

		// First try to find the free connection:
		i1 = std::find_if( m_connections.begin(), m_connections.end(), predicateFreeConnection );
		if (i1 != m_connections.end()){
			// return the connection, mark it busy!
			(*i1)->state = dfsConnection::BUSY_OK;
			return &*i1;
		}

		// check any other connections except in "BUSY_OK" or "FREE_INITIALIZED" state.
		anyNonInitializedConnectionPredicate uninitializedPredicate;
		std::list<boost::shared_ptr<dfsConnection> >::iterator i2;

		i2 = std::find_if( m_connections.begin(), m_connections.end(), uninitializedPredicate );
		if (i2 != m_connections.end()){
			// have ubnormal connections, get the first and reinitialize it:
			if(m_dfsAdaptor->connect((*i2)) == 0){
				(*i2)->state = dfsConnection::BUSY_OK;
				return &*i2;
			}
			else
				return nullptr; // no connection can be established. No retries right now.
		}

		// seems there're no unused connections right now.
		// need to create new connection to DFS:
		boost::shared_ptr<dfsConnection> connection(new dfsConnection());
		connection->state = dfsConnection::NON_INITIALIZED;

		if(m_dfsAdaptor->connect(connection) == 0){
			connection->state = dfsConnection::FREE_INITIALIZED;
			m_connections.push_back(connection);
			return getFreeConnection();
		}
		// unable to connect to DFS.
		return nullptr;
}


} /** namespace impala */


