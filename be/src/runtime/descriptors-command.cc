/*
 * descriptors-command.cc
 *
 *  Created on: Feb 26, 2015
 *      Author: elenav
 */

#include "runtime/descriptors-command.h"

namespace impala{

bool RenameCmdDescriptor::validate(const TRemoteShortCommand& cdesc){
	// check there's the path is specified:
    DCHECK(!cdesc.__isset.dfs_path);
    // check rename set is specified:
    DCHECK(!cdesc.__isset.rename_set);

    // check that there's connection to dfs exist:
	HdfsFsCache::instance()->GetConnection(
			cdesc.dfs_path, &dfs_connection_);
    DCHECK(dfs_connection_.valid);
    return true;
}

bool RenameCmdDescriptor::run() {
	std::map<std::string, std::string>::iterator iter;
	for (iter = m_rename_set.begin(); iter != m_rename_set.end(); iter++) {
		DCHECK(	dfsRename(dfs_connection_, iter->first.c_str(),
				iter->second.c_str()) == 0);
	}
	return true;
}

bool DeleteCmdDescriptor::run() {
	std::vector<std::string>::iterator iter;
	for (iter = m_deletion_set.begin(); iter != m_deletion_set.end(); iter++) {
		DCHECK(dfsDelete(dfs_connection_, (*iter).c_str()) == 0);
	}
	return true;
}

bool DeleteCmdDescriptor::validate(const TRemoteShortCommand& cdesc){
	// check there's the path is specified:
	DCHECK(!cdesc.__isset.dfs_path);
	// check deletion set is specified:
	DCHECK(!cdesc.__isset.delete_set);

	// check that there's connection to dfs exist:
	HdfsFsCache::instance()->GetConnection(cdesc.dfs_path, &dfs_connection_);
	DCHECK(dfs_connection_.valid);
	return true;
}

}


