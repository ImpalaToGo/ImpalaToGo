/*
 * @file managed-file.cc
 * @brief internal implementation of Managed File
 *
 * @date   Oct 13, 2014
 * @author elenav
 */

#include "dfs_cache/managed-file.hpp"

namespace impala {

namespace managed_file {

status::StatusInternal File::open(const dfsFile & handle) {
	boost::mutex::scoped_lock lock(m_mux);
	m_handles.push_back(handle);
	return status::OK;
}

status::StatusInternal File::close(const dfsFile & handle) {
	boost::mutex::scoped_lock lock(m_mux);
	// remove the handle if it refers to the same raw object as arrived shared pointer:
	m_handles.remove_if(FileHandleEqPredicate(handle));
	return status::OK;
}

} /** namespace ManagedFile */
} /** namespace impala */

