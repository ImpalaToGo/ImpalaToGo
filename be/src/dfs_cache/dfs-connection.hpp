/*
 * @file  dfs-connection.hpp
 * @brief define raii dfs connection
 *
 * @date   Oct 27, 2014
 * @author elenav
 */

#ifndef DFS_CONNECTION_HPP_
#define DFS_CONNECTION_HPP_

#include "dfs_cache/common-include.hpp"

namespace impala{

/** reset connection to initialized free state when it is not more needed */
class raiiDfsConnection{
private:
	dfsConnectionPtr m_connection;     /**< dfs connection */

	raiiDfsConnection(const raiiDfsConnection&) = delete;           // prevent copy constructor to be used so that operation conn1 = conn2 is impossible
	raiiDfsConnection& operator=(const raiiDfsConnection&) = delete; // prevent copy assignment to be used so that operation conn1(conn2) is avoided

public:
	raiiDfsConnection(const dfsConnectionPtr& connection){
		// make a copy of shared connection
		m_connection = connection;
	}

	~raiiDfsConnection(){
		if(!m_connection)
			return;

		m_connection->state = dfsConnection::ConnectionState::FREE_INITIALIZED;
		m_connection.reset();
	}

	/** flag, indicates whether connection is valid */
	bool valid() { return m_connection != NULL; }

	/**
	 * Move assignment
	 */
	raiiDfsConnection& operator=(raiiDfsConnection&& other)
    {
		if (this == &other)
			return *this;

		raiiDfsConnection temp(std::forward<raiiDfsConnection>(other));
		temp.swap(std::forward<raiiDfsConnection>(*this));
        return *this;
    }

	void swap (raiiDfsConnection &&other) throw (){
		std::swap (this->m_connection, other.m_connection);
	}

   /**
    * Move constructor
    */
	raiiDfsConnection (raiiDfsConnection&& other)
    {
		m_connection.reset();
        // swap the resource from "other"
		std::swap(m_connection, other.m_connection);
    }

	/** connection state getter */
	dfsConnection::ConnectionState state() { return m_connection->state; }

	/** connection state setter */
	void state(dfsConnection::ConnectionState state) { m_connection->state = state; }

	dfsConnectionPtr& connection() { return m_connection; }
};

}

#endif /* DFS_CONNECTION_HPP_ */
