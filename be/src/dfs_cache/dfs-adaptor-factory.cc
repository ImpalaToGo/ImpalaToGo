/*
 * @file dfs-adaptor-factory.cc
 * @brief implementation of dfs adaptor factory.
 *
 * @author elenav
 * @date   Oct 10, 2014
 */

#include "dfs_cache/dfs-adaptor-factory.hpp"

namespace impala{

std::ostream& operator<<(std::ostream &strm, const fsStatistics &statistic){
	return strm << statistic.bytesRead << " bytes read, " << statistic.bytesWritten << " bytes written, "
	          << statistic.readOps << " read ops, " << statistic.largeReadOps << " large read ops, "
	          << statistic.writeOps << " write ops";
}

std::ostream& operator<<(std::ostream &strm, const fileStatus &status)
{
	return strm << "path=" + status.path << "; isDirectory=" + status.isdir <<
	    (!status.isdir ?  ("; length=" << status.length << "; replication=" << status.block_replication <<
		"; blocksize=" << status.blocksize ) : "") << "; modification_time=" << status.modification_time <<
	    "; access_time=" << status.access_time << "; owner=" << status.owner << "; group=" << status.group <<
	    "; permission=" << status.permission << "; isSymlink=" << status.issymlink <<
	    (status.issymlink ? "; symlink=" << status.symlink : "");
}

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


