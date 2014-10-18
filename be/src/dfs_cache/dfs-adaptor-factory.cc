/*
 * dfs-adaptor-factory.cc
 *
 *  Created on: Oct 10, 2014
 *      Author: elenav
 */

#include "dfs_cache/dfs-adaptor-factory.hpp"

namespace impala{

dfsAdaptorFactory::AdaptorState dfsAdaptorFactory::addAdaptor(const dfs::DFS_TYPE& dfsType,
														      const boost::shared_ptr<RemoteAdaptor>& adaptor,
														      bool force){
    	AdaptorState state = DEFAULT;
    	boost::mutex::scoped_lock(m_mux);
    	if(m_adaptors.count(dfsType) > 0){
    		if(!force){ // skip redefinition if this was not an intention:
    			state = ALREADY_DEFINED;
    			return state;
    		}
    		// force reqrite the plugin.
    		// Drop the previous plugin
    		boost::shared_ptr<RemoteAdaptor> old =  m_adaptors[dfsType];
    		m_adaptors.erase(dfsType);
    		// say we do not reference old adaptor more
            old.reset();
    	}
        m_adaptors[dfsType] = adaptor;
        state = INITIALIZED;

        return state;
    }

dfsAdaptorFactory::AdaptorState dfsAdaptorFactory::getAdaptor(const dfs::DFS_TYPE& dfsType, boost::shared_ptr<RemoteAdaptor>*& adaptor){
    	boost::mutex::scoped_lock(m_mux);
    	if(m_adaptors.count(dfsType) <= 0){
    		adaptor = nullptr;
    		return NON_CONFIGURED;
    	}
    	adaptor = &m_adaptors[dfsType];
        return INITIALIZED;
    }

}


