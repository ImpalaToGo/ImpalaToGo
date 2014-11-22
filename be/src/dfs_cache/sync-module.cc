/*
 * @file sync-module.cpp
 * @brief implementation of sync module
 *
 *  Created on: Oct 3, 2014
 *      Author: elenav
 */

/**
 * @namespace impala
 */

#include <fcntl.h>

#include "dfs_cache/sync-module.hpp"
#include "dfs_cache/dfs-connection.hpp"
#include "dfs_cache/filesystem-mgr.hpp"

namespace impala {

status::StatusInternal Sync::estimateTimeToGetFileLocally(const FileSystemDescriptor & fsDescriptor, const char* path,
		request::MakeProgressTask<boost::shared_ptr<FileProgress> >* const & task){

	// Get the Namenode adaptor from the registry for requested namenode:
	boost::shared_ptr<FileSystemDescriptorBound> namenodeAdaptor = (*m_registry->getFileSystemDescriptor(fsDescriptor));
    if(namenodeAdaptor == nullptr){
    	// no namenode adaptor configured, go out
    	return status::StatusInternal::NAMENODE_IS_NOT_CONFIGURED;
    }
    raiiDfsConnection connection(namenodeAdaptor->getFreeConnection());
    if(!connection.valid()) {
    	LOG (ERROR) << "No connection to dfs available, no estimate actions will be taken for namenode \"" << fsDescriptor.dfs_type << ":" <<
    			fsDescriptor.host << "\"" << "\n";
    	return status::StatusInternal::DFS_NAMENODE_IS_NOT_REACHABLE;
    }

    // Execute the remote estimation operation on the adaptor.
    // wait for execution.
    // free the connection so it is available for further usage

    // get the file progress reference:
    boost::shared_ptr<FileProgress> fp = task->progress();

    // set the progress directly to the task
	return status::StatusInternal::OK;
}

status::StatusInternal Sync::prepareFile(const FileSystemDescriptor & fsDescriptor, const char* path,
		request::MakeProgressTask<boost::shared_ptr<FileProgress> >* const & task){
	// Get the Namenode adaptor from the registry for requested namenode:
	boost::shared_ptr<FileSystemDescriptorBound> fsAdaptor = (*m_registry->getFileSystemDescriptor(fsDescriptor));
    if(fsAdaptor == nullptr){
    	// no namenode adaptor configured, go out
    	return status::StatusInternal::NAMENODE_IS_NOT_CONFIGURED;
    }

    // get the file progress reference:
    boost::shared_ptr<FileProgress> fp = task->progress();

    /********** Synchronization context, for cancellation  *********/

	// while no cancellation and we still something to read, proceed.
	boost::mutex* mux;
	boost::condition_variable_any* conditionvar;

	// request synchronization context from ongoing task:
	task->mux(mux);
	task->conditionvar(conditionvar);
    /***************************************************************/

    raiiDfsConnection connection(fsAdaptor->getFreeConnection());
    if(!connection.valid()) {
    	LOG (ERROR) << "No connection to dfs available, no prepare actions will be taken for FileSystem \"" << fsDescriptor.dfs_type << ":" <<
    			fsDescriptor.host << "\"" << "\n";
		fp->error    = true;
		fp->errdescr = "Failed to establish remote fs connection";
    	return status::StatusInternal::DFS_NAMENODE_IS_NOT_REACHABLE;
    }

	// open remote file:
	dfsFile hfile = fsAdaptor->fileOpen(connection, path, O_RDONLY, 0, 0, 0);

	if(hfile == NULL){
		LOG (ERROR) << "Requested file \"" << path << "\" is not available on \"" << fsDescriptor.dfs_type << ":" <<
				fsDescriptor.host << "\"" << "\n";
		fp->error    = true;
		fp->errdescr = "Unable to open requested remote file";
		return status::StatusInternal::DFS_OBJECT_DOES_NOT_EXIST;
	}

	 #define BUFFER_SIZE 10
	 char* buffer = (char*)malloc(sizeof(char) * BUFFER_SIZE);
	 if(buffer == NULL){
		 LOG (ERROR) << "Insufficient memory to read remote file \"" << path << "\" from \"" << fsDescriptor.dfs_type << ":" <<
				 fsDescriptor.host << "\"" << "\n";
    	 fp->error    = true;
    	 fp->errdescr = "Insufficient memory for file operation";
		 return status::StatusInternal::DFS_OBJECT_DOES_NOT_EXIST;
	 }

	 bool available;
	 // open or create local file:
	 dfsFile file = filemgmt::FileSystemManager::instance()->dfsOpenFile(fsAdaptor->descriptor(), path, O_CREAT, 0, 0, 0, available);
     if(file == NULL || !available){
    	 LOG (ERROR) << "Unable to create local file \"" << path << "\", being cached from \"" << fsDescriptor.dfs_type << ":" <<
    	 				 fsDescriptor.host << "\"" << "\n";
    	 fp->error    = true;
    	 fp->errdescr = "Cannot create local file";
    	 return status::StatusInternal::FILE_OBJECT_OPERATION_FAILURE;
     }

     // Start work with cached file:
     managed_file::File* managed_file;
     CacheLayerRegistry::instance()->findFile(path, fsDescriptor, managed_file);
     if(managed_file == nullptr){
    	 LOG (ERROR) << "Failed to locate managed file \"" << path << "\" in cache registry for \"" << fsDescriptor.dfs_type << ":" <<
    	 				 fsDescriptor.host << "\"" << "\n";
    	 fp->error    = true;
    	 fp->errdescr = "Cache-managed registry file could not be located";
    	 return status::StatusInternal::CACHE_OBJECT_NOT_FOUND;
     }

	 // read from the remote file
	 tSize last_read = BUFFER_SIZE;
	 for (; last_read == BUFFER_SIZE;) {
		 boost::mutex::scoped_lock lock(*mux);
		 if(task->condition()){
			 // stop reading, cancellation received.
			 // If so, notify the caller (no matter if it waits for this or no):
			 conditionvar->notify_all();
			 break;
		 }
		 managed_file->estimated_size(managed_file->estimated_size() + last_read);

		 last_read = fsAdaptor->fileRead(connection, hfile, (void*)buffer, last_read);
		 filemgmt::FileSystemManager::instance()->dfsWrite(fsAdaptor->descriptor(), file, buffer, last_read);
		 fp->localBytes += last_read;
	 }
	 // whatever happens, clean resources:
	 free(buffer);
	 // close remote file:
	 fsAdaptor->fileClose(connection, hfile);
     // close local file:
	 filemgmt::FileSystemManager::instance()->dfsCloseFile(fsAdaptor->descriptor(), file);

	 if(task->condition()){ // cancellation was requested:
		 LOG (WARNING) << "Cancellation was requested during file read \"" << path << "\" from \"" << fsDescriptor.dfs_type << ":" <<
		 				 fsDescriptor.host << "\"" << ". This file was not cached. \n";
		 filemgmt::FileSystemManager::instance()->dfsDelete(fsAdaptor->descriptor(), path, true);
	 }

	return status::StatusInternal::OK;
}

status::StatusInternal Sync::cancelFileMakeProgress(bool async, request::CancellableTask* const & task){
	boost::mutex* mux;
	boost::condition_variable_any* conditionvar;
	task->mux(mux);
	task->conditionvar(conditionvar);

	boost::mutex::scoped_lock lock(*mux);
	// set the cancellation flag
	bool* condition;
	task->condition(condition);
    *condition = true;

	if(!async){
		conditionvar->wait(*mux);
		// update the history!1
	} // else return immediately
	return status::StatusInternal::OK;
}

status::StatusInternal Sync::validateLocalCache(bool& valid){
	return status::StatusInternal::NOT_IMPLEMENTED;
}
}


