/*
 * dfs-adaptor-factory.hpp
 *
 *  Created on: Oct 10, 2014
 *      Author: elenav
 */

#ifndef DFS_ADAPTOR_FACTORY_HPP_
#define DFS_ADAPTOR_FACTORY_HPP_

#include <map>

#include "dfs_cache/common-include.hpp"

namespace impala{

/**
 * Thin class to hold adaptors to remote dfs and provides them according to the specified type of DFS
 */
class dfsAdaptorFactory{
private:
	std::map<dfs::DFS_TYPE, boost::shared_ptr<RemoteAdaptor> > m_adaptors;

	boost::mutex m_mux;

public:
	enum AdaptorState{
		DEFAULT,
		INITIALIZED,
		ALREADY_DEFINED,
		NON_CONFIGURED,
	};

	/**
	 * Add the adaptor for one of DFS types.
	 * Note that this is "sink" method, do not use adaptor after calling this.
	 *
	 * @param dfsType - dfs type
	 * @param adaptor - DFS adaptor
	 * @param force   - flag, indicates whether force plugin registration is required.
	 * If specified, plugin will be added even if another one exists for the same dfs type already
	 */
    AdaptorState addAdaptor(const dfs::DFS_TYPE& dfsType, const boost::shared_ptr<RemoteAdaptor>& adaptor, bool force = false);

    /**
     * Get adaptor for specified DFS
     *
     * @param[In]  dfsType - dfsType - DFS type
     * @param[Out] adaptor - adaptor for DFS
     *
     * @return adaptor status
     */
    AdaptorState getAdaptor(const dfs::DFS_TYPE& dfsType, boost::shared_ptr<RemoteAdaptor>*& adaptor);
};


}
#endif /* DFS_ADAPTOR_FACTORY_HPP_ */
