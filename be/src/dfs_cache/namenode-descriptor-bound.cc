/*
 * namenode-descriptor-bound.cc
 *
 *  Created on: Oct 10, 2014
 *      Author: elenav
 */

#include "dfs_cache/namenode-descriptor-bound.hpp"

namespace impala {

std::ostream& operator<<(std::ostream& out, const dfs::DFS_TYPE value){
    static std::map<dfs::DFS_TYPE, std::string> strings;
    if (strings.size() == 0){
#define INSERT_ELEMENT(p) strings[p] = #p
        INSERT_ELEMENT(dfs::HDFS);
        INSERT_ELEMENT(dfs::S3);
        INSERT_ELEMENT(dfs::OTHER);
#undef INSERT_ELEMENT
    }
    return out << strings[value];
}

raiiDfsConnection NameNodeDescriptorBound::getFreeConnection(){
	freeConnectionPredicate predicateFreeConnection;

		boost::mutex::scoped_lock(m_mux);
		std::list<boost::shared_ptr<dfsConnection> >::iterator i1;

		// First try to find the free connection:
		i1 = std::find_if( m_connections.begin(), m_connections.end(), predicateFreeConnection );
		if (i1 != m_connections.end()){
			LOG (INFO) << "Existing free connection is found and will be used for namenode \"" << m_namenode.dfs_type << ":" <<  m_namenode.host << "\"" << "\n";
			// return the connection, mark it busy!
			(*i1)->state = dfsConnection::BUSY_OK;
			return std::move(raiiDfsConnection(*i1));
		}

		// check any other connections except in "BUSY_OK" or "FREE_INITIALIZED" state.
		anyNonInitializedConnectionPredicate uninitializedPredicate;
		std::list<boost::shared_ptr<dfsConnection> >::iterator i2;

		i2 = std::find_if( m_connections.begin(), m_connections.end(), uninitializedPredicate );
		if (i2 != m_connections.end()){
			// have ubnormal connections, get the first and reinitialize it:
			if(m_dfsAdaptor->connect((*i2)) == 0){
				LOG (INFO) << "Existing non-initialized connection is initialized and will be used for namenode \"" << m_namenode.dfs_type << ":"
						<<  m_namenode.host << "\"" << "\n";
				(*i2)->state = dfsConnection::BUSY_OK;
				return std::move(raiiDfsConnection(*i2));
			}
			else
				return std::move(raiiDfsConnection(dfsConnectionPtr())); // no connection can be established. No retries right now.
		}

		// seems there're no unused connections right now.
		// need to create new connection to DFS:
		LOG (INFO) << "No free connection exists for namenode \"" << m_namenode.dfs_type << ":" <<  m_namenode.host << "\", going to create one." << "\n";
		boost::shared_ptr<dfsConnection> connection(new dfsConnection());
		connection->state = dfsConnection::NON_INITIALIZED;

		if(m_dfsAdaptor->connect(connection) == 0){
			connection->state = dfsConnection::FREE_INITIALIZED;
			m_connections.push_back(connection);
			return getFreeConnection();
		}
		LOG (ERROR) << "Unable to connect to namenode \"." << "\"" << "\n";
		// unable to connect to DFS.
		return std::move(raiiDfsConnection(dfsConnectionPtr()));
}


} /** namespace impala */


