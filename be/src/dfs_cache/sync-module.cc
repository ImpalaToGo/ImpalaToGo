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

#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>

#include "dfs_cache/sync-module.hpp"
#include "dfs_cache/dfs-connection.hpp"
#include "dfs_cache/filesystem-mgr.hpp"

#include "util/runtime-profile.h"

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

	status::StatusInternal status = status::StatusInternal::OK;

	// Get the Namenode adaptor from the registry for requested namenode:
	boost::shared_ptr<FileSystemDescriptorBound> fsAdaptor = (*m_registry->getFileSystemDescriptor(fsDescriptor));
    if(fsAdaptor == nullptr){
    	// no namenode adaptor configured, go out
    	return status::StatusInternal::NAMENODE_IS_NOT_CONFIGURED;
    }

    // get the file progress reference:
    boost::shared_ptr<FileProgress> fp = task->progress();
    fp->progressStatus = FileProgressStatus::fileProgressStatus::FILEPROGRESS_COMPLETED_OK;

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
		fp->progressStatus = FileProgressStatus::fileProgressStatus::FILEPROGRESS_REMOTE_DFS_IS_UNREACHABLE;
    	return status::StatusInternal::DFS_NAMENODE_IS_NOT_REACHABLE;
    }

    // Get the reference to LRU mirror of the file to prepare:
    managed_file::File* managed_file;
    CacheLayerRegistry::instance()->findFile(path, fsDescriptor, managed_file);

    if(managed_file == nullptr){
   	 LOG (ERROR) << "Failed to locate managed file \"" << path << "\" in cache registry for \"" << fsDescriptor.dfs_type << ":" <<
   	 				 fsDescriptor.host << "\"" << "\n";
   	 fp->error    = true;
   	 fp->errdescr = "Cache-managed registry file could not be located";
   	 fp->progressStatus = FileProgressStatus::fileProgressStatus::FILEPROGRESS_LOCAL_FAILURE;
   	 return status::StatusInternal::CACHE_OBJECT_NOT_FOUND;
    }
    // get estimated bytes from managed file statistics:
    fp->estimatedBytes = managed_file->remote_size();

    #define BUFFER_SIZE 17408
	// open remote file:
	dfsFile hfile = fsAdaptor->fileOpen(connection, managed_file->relative_name().c_str(), O_RDONLY, BUFFER_SIZE, 0, 0);

	if(hfile == NULL){
		LOG (ERROR) << "Requested file \"" << path << "\" is not available on \"" << fsDescriptor.dfs_type << "//:" <<
				fsDescriptor.host << "\"" << "\n";
		fp->error    = true;
		fp->errdescr = "Unable to open requested remote file";

		fp->progressStatus = FileProgressStatus::fileProgressStatus::FILEPROGRESS_IS_MISSED_REMOTELY;

		// update file status:
		managed_file->state(managed_file::State::FILE_IS_FORBIDDEN);
		managed_file->close();
		return status::StatusInternal::DFS_OBJECT_DOES_NOT_EXIST;
	}

	 char* buffer = (char*)malloc(sizeof(char) * BUFFER_SIZE);
	 if(buffer == NULL){
		 LOG (ERROR) << "Insufficient memory to read remote file \"" << path << "\" from \"" << fsDescriptor.dfs_type << ":" <<
				 fsDescriptor.host << "\"" << "\n";
    	 fp->error    = true;
    	 fp->errdescr = "Insufficient memory for file operation";
    	 fp->progressStatus = FileProgressStatus::fileProgressStatus::FILEPROGRESS_GENERAL_FAILURE;

    	 // update file status:
    	 managed_file->state(managed_file::State::FILE_IS_FORBIDDEN);
    	 managed_file->close();
		 return status::StatusInternal::DFS_OBJECT_DOES_NOT_EXIST;
	 }

	 bool available;
	 // open or create local file, temporary:
	 std::string temp_relativename = managed_file->relative_name() + "_tmp";
	 std::string tempname = managed_file->fqp() + "_tmp";

	 // here, we should recreate the file!
	 dfsFile file = filemgmt::FileSystemManager::instance()->dfsOpenFile(fsAdaptor->descriptor(), temp_relativename.c_str(), O_CREAT, 0, 0, 0, available);
     if(file == NULL || !available){
    	 LOG (ERROR) << "Unable to create local file \"" << path << "\", being cached from \""
    			 << fsDescriptor.dfs_type << ":" << fsDescriptor.host << "\"" << "\n";
    	 fp->error    = true;
    	 fp->errdescr = "Cannot create local file";
    	 fp->progressStatus = FileProgressStatus::fileProgressStatus::FILEPROGRESS_LOCAL_FAILURE;

    	 // update file status:
    	 managed_file->state(managed_file::State::FILE_IS_FORBIDDEN);
    	 managed_file->close();
    	 return status::StatusInternal::FILE_OBJECT_OPERATION_FAILURE;
     }

     MonotonicStopWatch sw;

     // since this time, the meta file became backed with a physical one
     managed_file->nature(managed_file::NatureFlag::PHYSICAL);

	 // read from the remote file
	 tSize last_read = 0;

	 sw.Start();  // start track time consumed by download:

	 // define a reader
	 boost::function<void ()> reader = [&]() {
		 last_read = fsAdaptor->fileRead(connection, hfile, (void*)buffer, BUFFER_SIZE);
		 for (; last_read > 0;) {
		 		 boost::mutex::scoped_lock lock(*mux);
		 		 if(task->condition()){
		 			 // stop reading, cancellation received.
		 			 // If so, notify the caller (no matter if it waits for this or no):
		 			 conditionvar->notify_all();
		 			 break;
		 		 }
		 		 // write bytes locally:
		 		 filemgmt::FileSystemManager::instance()->dfsWrite(fsAdaptor->descriptor(), file, buffer, last_read);
		 		 managed_file->estimated_size(managed_file->estimated_size() + last_read);
		 		 // update job progress:
		 		 fp->localBytes += last_read;
		 		 // read next data buffer:
		 		 last_read = fsAdaptor->fileRead(connection, hfile, (void*)buffer, BUFFER_SIZE);
		 	 }
	 };
	 // and run the reader
	 reader();

	 int retry = 0;

	 const int seconds  = 2;
	 unsigned int delay = 1000000 * seconds;

	 if(last_read == -1){
		 LOG (WARNING) << "Remote file \"" << path << "\" read encountered IO exception, going to retry 3 times." << "\n";

		 while(retry++ <= 2){
			LOG (INFO) << "Retry # " << std::to_string(retry) << " to deliver the file \"" << path << "\" after disconnection. position = "
					<< std::to_string(fp->localBytes) << "\n";
			// IO Exception happens, close the current remote file and reopen it,
			// seek to last read position and retry. Do this once per 2 seconds, 3 times
			usleep (delay);

			// close old remote handle:
			fsAdaptor->fileClose(connection, hfile);

			// reopen the remote file:
			hfile = fsAdaptor->fileOpen(connection,
					managed_file->relative_name().c_str(), O_RDONLY,
					BUFFER_SIZE, 0, 0);

			if (hfile == NULL){
				LOG (WARNING) << "Retry # " << std::to_string(retry) << " for \"" << path << "\". Failed to open remote file." << "\n";
				continue;
			}
			// seek the file to last successfully read position:
            int ret = fsAdaptor->fileSeek(connection, hfile, fp->localBytes);
            if(ret != 0){
            	LOG (WARNING) << "Retry # " << std::to_string(retry) << " for \"" << path << "\". Failed to seek remote file to position = "
            			<< std::to_string(fp->localBytes) << "\n";
            	continue;
            }
            // run reader
			reader();
			// check last-read. If stream is read to end, break the reader
			if(last_read == 0)
				break;
		 }
	 }
	 uint64_t ti = sw.ElapsedTime();
	 std::cout << "Elapsed time for \"" << path << "\" download = " << std::to_string(ti) << std::endl;
	 sw.Stop();

	 LOG (INFO) << "Remote bytes read = " << std::to_string(fp->localBytes) << " for file \"" << path << "\".\n";
	 // whatever happens, clean resources:
	 free(buffer);

     if(last_read != 0){
    	 // remote file was not read to end, report a problem:
    	 status = status::StatusInternal::DFS_OBJECT_OPERATION_FAILURE;
    	 fp->error    = true;
    	 fp->errdescr = "Error during remote file read";
    	 fp->progressStatus = FileProgressStatus::fileProgressStatus::FILEPROGRESS_INCONSISTENT_DATA;

    	 // update the managed file state:
    	 managed_file->state(managed_file::State::FILE_IS_FORBIDDEN);
     }

	 int ret;
	 // close remote file:
	 ret = fsAdaptor->fileClose(connection, hfile);
	 if(ret != 0){
		 LOG (WARNING) << "Remote file \"" << path << "\" close() failure. " << "\n";
		 status = status::StatusInternal::DFS_OBJECT_OPERATION_FAILURE;
	 }

     // close local file anyway:
	 status::StatusInternal statuslocal = filemgmt::FileSystemManager::instance()->dfsCloseFile(fsAdaptor->descriptor(), file);
	 if(statuslocal != status::StatusInternal::OK){
		 LOG (WARNING) << "Local file \"" << tempname << "\" close() failure. " << "\n";
		 status = statuslocal;
	 }

	 ret = std::rename(tempname.c_str(), managed_file->fqp().c_str());
	 // move the temporary to target location within the cache:
	 if(ret != 0){
		 LOG (ERROR) << "Temporary file \"" << tempname << "\" was not renamed to \"" << managed_file->fqp() <<
				 "\"; error : " << strerror(errno) << "\n";
		 fp->error = true;
		 fp->errdescr = strerror(errno);
		 fp->progressStatus = FileProgressStatus::fileProgressStatus::FILEPROGRESS_LOCAL_FAILURE;

		 managed_file->state(managed_file::State::FILE_IS_FORBIDDEN);
		 managed_file->close();
		 return status::StatusInternal::FILE_OBJECT_OPERATION_FAILURE;
	 }

	 if(task->condition()){ // cancellation was requested:
		 LOG (WARNING) << "Cancellation was requested during file read \"" << path << "\" from \"" << fsDescriptor.dfs_type << ":" <<
		 				 fsDescriptor.host << "\"" << ". This file was not cached. \n";
		 filemgmt::FileSystemManager::instance()->dfsDelete(fsAdaptor->descriptor(), managed_file->relative_name().c_str(), true);
	 }

     // check the integrity of local bytes and remote bytes for managed file and assign the appropriate status:
	 if(managed_file->remote_size() != managed_file->size()){
		 fp->errdescr = "File is not consistent with remote origin";
		 fp->error = true;
		 fp->progressStatus = FileProgressStatus::fileProgressStatus::FILEPROGRESS_GENERAL_FAILURE;

		 managed_file->state(managed_file::State::FILE_IS_FORBIDDEN);
		 LOG (ERROR) << "File \"" << managed_file->fqp() << "\" has inconsistent size and is marked as forbidden.\n";
		 status = status::StatusInternal::CACHE_OBJECT_IS_FORBIDDEN;
	 }

	 managed_file->close();
	 return status;
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


