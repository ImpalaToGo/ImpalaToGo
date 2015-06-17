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
#include <sys/types.h>
#include <sys/wait.h>
#include <cstdio>

#include "dfs_cache/sync-module.hpp"
#include "dfs_cache/dfs-connection.hpp"
#include "dfs_cache/filesystem-mgr.hpp"

#include "util/runtime-profile.h"

namespace impala {
namespace constants{
const int OK = 0;

/* External interruption causes data pipe shutdown */
const int EXTERNAL_INTERRUPTION = 98;

/* failure during I/O redirection in pipelines */
const int PIPELINE_FAILURE = 99;

/** status indicates bad command format encountered */
const int BAD_COMMAND_FORMAT = 100;

/* failure related to command execution attempt */
const int COMMAND_EXEC_FAILURE = 101;

/* failure to spawn the working process */
const int FORK_FAILURE = 102;

/* failure indicates that the child process was detached in unexpected way */
const int CHILD_PROCESS_DETACHED = 103;

/** write interruption failure */
const int INTERRUPTED_WRITE = 104;

/** read interruption failure */
const int INTERRUPTED_READ = 105;

/* failure during command's out read */
const int PIPELINE_READ_FAILURE = 106;

/** operation of waiting for pipe with transformed data is timed out */
const int TIMEOUT_WAIT_FOR_TRANSFORMED_DATA = 107;

}
void dataTransformationProgressStateMachine(int ret, managed_file::File* file){
	 // anaylze the retcode to understand where we are with data:
	 switch(ret){
	 case constants::OK:
		 file->compatible(true);
		 break;
	 // interrupted remote dfs read is being handled by retry mechanism
	 case constants::INTERRUPTED_READ:
		 LOG(ERROR) << "Transform data : remote dfs read interrupted." << "\n";
		 break;
	 case constants::PIPELINE_FAILURE:
		 LOG(ERROR) << "Transform data : failure to interact with externally defined command for data." << "\n";
		 file->compatible(false);
		 break;
	 case constants::EXTERNAL_INTERRUPTION:
		 LOG(ERROR) << "Transform data : external interruption." << "\n";
		 file->compatible(false);
		 break;
	 case constants::BAD_COMMAND_FORMAT:
		 LOG(ERROR) << "Transform data : bad command format." << "\n";
		 file->compatible(false);
		 break;
	 case constants::COMMAND_EXEC_FAILURE:
		 LOG(ERROR) << "Transform data : exec command failure." << "\n";
		 file->compatible(false);
		 break;
	 case constants::FORK_FAILURE:
		 LOG(ERROR) << "Transform data : forking for command failure." << "\n";
		 file->compatible(false);
		 break;
	 case constants::CHILD_PROCESS_DETACHED:
		 LOG(ERROR) << "Transform data : child process detached while holding its pipe." << "\n";
		 file->compatible(false);
		 break;
	 case constants::INTERRUPTED_WRITE:
		 LOG(ERROR) << "Transform data : failed to write into external command." << "\n";
		 file->compatible(false);
		 break;
	 case constants::PIPELINE_READ_FAILURE:
		 LOG(ERROR) << "Transform data : failed to read from external command." << "\n";
		 file->compatible(false);
		 break;
	 case constants::TIMEOUT_WAIT_FOR_TRANSFORMED_DATA:
		 LOG(ERROR) << "Transform data : operation of waiting for pipe with transformed data is timed out." << "\n";
		 file->compatible(false);
		 break;
	 }
}

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

#define BOOST_THREAD_PROVIDES_FUTURE
#include <boost/thread.hpp>
#include <boost/thread/future.hpp>

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

	 char* buffer = (char*)malloc(sizeof(char) * (BUFFER_SIZE + 1));
	 /* buffer for transformed data */
	 char* in_buffer = NULL;

	 /* allocate buffer for transformed data if transformation command is specified */
	 if(!managed_file->transformCmd().empty())
		 in_buffer = (char*)malloc(sizeof(char) * (BUFFER_SIZE + 1));

	 if(buffer == NULL || (!managed_file->transformCmd().empty() && in_buffer == NULL)){
		 LOG (ERROR) << "Insufficient memory to read remote file \"" << path << "\" from \"" << fsDescriptor.dfs_type << ":" <<
				 fsDescriptor.host << "\"" << "\n";
    	 fp->error    = true;
    	 fp->errdescr = "Insufficient memory for file operation";
    	 fp->progressStatus = FileProgressStatus::fileProgressStatus::FILEPROGRESS_GENERAL_FAILURE;

    	 // update file status:
    	 managed_file->state(managed_file::State::FILE_IS_FORBIDDEN);
    	 managed_file->close();

    	 if(buffer != NULL)
    		 free(buffer);

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

    	 if(buffer != NULL)
    		 free(buffer);
    	 if(in_buffer != NULL)
    		 free(buffer);

    	 return status::StatusInternal::FILE_OBJECT_OPERATION_FAILURE;
     }

     MonotonicStopWatch sw;

     // since this time, the meta file became backed with a physical one
     managed_file->nature(managed_file::NatureFlag::PHYSICAL);

	 // read from the remote file
	 tSize last_read = 0;

	 sw.Start();  // start track time consumed by download:

	 // define a reader
	 boost::function<void ()> reader_r = [&]() {
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
		 // file is "ok"
		 if(last_read == 0)
			 managed_file->compatible(true);
	 };

	 // define a reader + data transformation
	 // define a reader
	 boost::function<int ()> reader_t = [&]() {
		 using namespace std;

		 /** Pipe type, to describe the pipe direction */
		 enum PIPE_FILE_DESCRIPTORS
		 {
		   READ_FD  = 0,
		   WRITE_FD = 1
		 };

		 /* for ret codes or statuses */
		 int       ret = constants::OK;

		 /* duplex pipeline from the parent to child */
		 int       parent_pipeline[2];
		 /* duplex pipeline from the child to parent */
		 int       child_pipeline[2];

		 /* child process id */
		 pid_t     pid;

		 /*************** Buffered IO operations management *********************/

		 /* number of bytes participating buffered Read operation */
		 std::size_t  in_bytes;

		 /* transformed data size */
		 int transformed_data_size = 0;

		 /* open pipes: from parent to a child and vice versa */
		 if(-1 == (ret = pipe(parent_pipeline))){
			 LOG (ERROR) << "Unable to open parent-to-child pipeline." << "\n";
			 return constants::PIPELINE_FAILURE;
		 }
		 if(-1 == (ret = pipe(child_pipeline))){
			 LOG (ERROR) << "Unable to open child-to-parent pipeline." << "\n";
			 close(parent_pipeline[ READ_FD  ]);
			 close(parent_pipeline[ WRITE_FD ]);
			 return constants::PIPELINE_FAILURE;
		 }

     	 /* fork the current process: */
		 switch (pid = fork()){
		 case -1:
			 // Still in parent process:
			 LOG (ERROR) << "Fork failed" << "\n";

			 // close all pipes:
			 close(parent_pipeline[ READ_FD  ]);
			 close(parent_pipeline[ WRITE_FD ]);
			 close(child_pipeline [ READ_FD  ]);
			 close(child_pipeline [ WRITE_FD ]);

			 return constants::FORK_FAILURE;

		 case 0: /* Child */ {
			 /* Redirect the child's stdin to parent's "read" pipe, so, parent is able to write into child's stdin */
			 if(-1 == (ret = dup2(parent_pipeline[ READ_FD ], STDIN_FILENO)))
				 exit(constants::PIPELINE_FAILURE);

			 /* Redirect child's stdout to child's "write" pipe, so its "read" direction could be read by parent  */
			 if(-1 == (ret = dup2(child_pipeline[ WRITE_FD ], STDOUT_FILENO)))
				 exit(constants::PIPELINE_FAILURE);

			 /* Close unused pipes within visible pipelines */
			 /* Close parent's write direction */
			 if(0 != (ret = close(parent_pipeline[ WRITE_FD ])))
				 exit(constants::PIPELINE_FAILURE);

			 /* Close child's read direction */
			 if(0 != (ret = close(child_pipeline[ READ_FD ])))
				 exit(constants::PIPELINE_FAILURE);

			 /* prepare the program arguments */
			 utilities::ProgramInvocationDetails details(managed_file->transformCmd());
			 if(!details.valid())
				 exit(constants::BAD_COMMAND_FORMAT);

			 /* file, char** argv */
			 /* execute external command */
			 execvp(details.program(), details.args());

			 /* set the specific "failure" retcode, as this line should never be reached  */
			 exit(constants::COMMAND_EXEC_FAILURE);
		 }

		 default: /* Parent */
			 LOG (INFO) << "Transformation is in progress on pid = " << pid << "..." << ".\n";

			 // Define a functor to handle non-blocking read from the pipe attached to the
			 // external command's output, i.e., data acceptor:
			 boost::function<int()> transformed_data_acceptor = [&]() {
				 /* define a set of descriptors we are going to observe for activity */
				 fd_set          readfds;
				 /* max timeout the observer is going to wait for for activity on one of observed descriptors */
				 struct timeval  timeout;

				 /* max descriptor number, from the range of readfds - observed by select() */
				 int max_sd = -1;

				 /* set the timeout */
				 timeout.tv_sec  = 0;     /* seconds */
				 timeout.tv_usec = 10000; /* microseconds */

				 FD_ZERO(&readfds);
				 /* assign the "output" side of exec pipe to be tracked for activity */
				 FD_SET( child_pipeline[ READ_FD ], &readfds );

				 /* set the highest file descriptor number to the only we track */
				 if(child_pipeline[ READ_FD ] > max_sd)
					 max_sd = child_pipeline[ READ_FD ];

				 /* activity type */
				 int activity;

				 /* go observe the configured descriptors for activity */
				 while(true){
					 switch (activity = select(1 + max_sd, &readfds, (fd_set*)NULL, (fd_set*)NULL, NULL) ){
					 case 0: /* Timeout expired */
						 return constants::TIMEOUT_WAIT_FOR_TRANSFORMED_DATA;

					 case -1: /* and external interruption or failure with pipe descriptor */
						 if ((errno == EINTR) || (errno == EAGAIN))
							 return constants::EXTERNAL_INTERRUPTION;
						 else
							 /* devastation, unexpected pipe failure */
							 return constants::PIPELINE_FAILURE;

					 case 1:  /* The pipe is signaling */
						 if (FD_ISSET(child_pipeline[ READ_FD ], &readfds)){
							 memset(in_buffer, 0, BUFFER_SIZE + 1);

							 /* ready to read the data */
							 switch(in_bytes = read(child_pipeline[ READ_FD ], in_buffer, BUFFER_SIZE)){
							 case 0: /* End-of-File, or non-blocking read. */
								 LOG (INFO) << "Data transformation is completed." << "Data size = "
								 	 << transformed_data_size << "\".\n";

								 /* Wait for child finalization and check for status */
								 if(pid != waitpid( pid, & ret, 0 )){
									 LOG (ERROR) << "Failure while waiting on child's pid : " << ret << ".\n";
									 return constants::CHILD_PROCESS_DETACHED;
								 }

								 LOG (INFO) << "Data transform function exit status is:  " << WEXITSTATUS(ret) << ".\n";
			                     /* For transformed file, rewrite the execution state with actual statistic
								  * as we cannot predict the real data size before transformation is run */
						 		 fp->estimatedBytes = fp->localBytes;
								 return constants::OK;

		        				case -1:
		        					/* EINTR : If an I/O primitive (open, read, ...) is waiting for an I/O device, and the signal arrived and was handled,
		        					 * the primitive will fail immediately.
		        					 * EAGAIN : code for non-blocking I/O (no data available right away, try again later)
		        					 **/
		        					if ((errno == EINTR) || (errno == EAGAIN)){
		        						errno = 0;
		        						return constants::EXTERNAL_INTERRUPTION;
		        					}
		        					else {
		        						LOG (ERROR) << "Failed to read transformed data." << "\n";
		        						return constants::PIPELINE_READ_FAILURE;
		        					}

		        				default:
		        					transformed_data_size += in_bytes;
		        					// write bytes locally:
		        					filemgmt::FileSystemManager::instance()->dfsWrite(fsAdaptor->descriptor(), file, in_buffer, in_bytes);
		        					managed_file->estimated_size(managed_file->estimated_size() + in_bytes);
		        					// update job progress:
		        					fp->localBytes += last_read;
		        					break;
		        				} /* end of "read from pipe, the result" switch */
		        			} /* end of "The pipe is signaling" case */
		        			break;

		        		default:
		        			LOG (ERROR) << "select was fired despite no activity on observed descriptors." << "\n";
		        			return constants::PIPELINE_FAILURE;
					 } /* end of select() fire handler */
				 } /* end of select() processing loop */
			 };

			 auto process_transformed_stream = boost::bind(transformed_data_acceptor);
			 // spawn the task for transformed data non-blocking read processor:
			 //auto au = spawn_task(process_transformed_stream);

			 auto au = boost::async(boost::launch::async,
					 [&] {return process_transformed_stream();});

			 int written = 0;
			 memset(buffer, 0, BUFFER_SIZE + 1);

			 /* Read the original data and forward it into transformation process */
			 last_read = fsAdaptor->fileRead(connection, hfile, (void*)buffer, BUFFER_SIZE);

			 for (; last_read > 0;) {
				 if((written = write(parent_pipeline[ WRITE_FD ], buffer, last_read)) != last_read){
					 LOG (ERROR) << "Unable to write into the pipe.\n";

					 // set status to "interrupted"
					 return constants::INTERRUPTED_WRITE;
				 }
				 memset(buffer, 0, BUFFER_SIZE + 1);
				 // read next data buffer:
				 last_read = fsAdaptor->fileRead(connection, hfile, (void*)buffer, BUFFER_SIZE);
			 }

			 /* When write is finished, close parent's write direction, so that the transformation process could detect the eof
			  * and stop the buffering */
			 if(0 != (ret = close(parent_pipeline[ WRITE_FD ]))){
				 LOG (ERROR) << "Unable to close parent's write pipe." << "\n";
				 return constants::PIPELINE_FAILURE;
			 }

			 /* Close remained unused pipes */
			 /* Close child's write direction */
			 if(0 != (ret = close(child_pipeline[ WRITE_FD ]))){
				 LOG(ERROR) << "Unable to close child's write pipe." << "\n";
				 return constants::PIPELINE_FAILURE;
			 }
			 if(last_read == -1){
				 /* dfs I/O exception happened, no reason to proceed. Non-blocking read will complete automatically
				  * as its stdin forwrader is closed */
				 LOG(ERROR) << "Remote read is interrupted." << "\n";
				 return constants::INTERRUPTED_READ;
			 }
			 /* now, wait for non-blocking read the transformed data to complete */
			 if(!(ret = au.get()))
				 return ret;
		 } /* switch (pid = fork())*/
		 return ret;
	 };

	boost::function<void()> reader = [&]() {
		// if the data transformation is required, run the extended reader:
		if(!managed_file->transformCmd().empty()) {
			int ret = reader_t();
			dataTransformationProgressStateMachine(ret, managed_file);
		}
		else
			// run the regular reader
			reader_r();
	};

	// run the reader instantiated from the context:
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
	 if(in_buffer != NULL)
		 free(in_buffer);

     if(last_read != 0 || !managed_file->compatible()){
    	 // remote file was not read to end, report a problem:
    	 status = status::StatusInternal::DFS_OBJECT_OPERATION_FAILURE;
    	 fp->error    = true;
    	 fp->errdescr = "Error during remote file read";
    	 fp->progressStatus = FileProgressStatus::fileProgressStatus::FILEPROGRESS_INCONSISTENT_DATA;

    	 // update the managed file state:
    	 managed_file->state(managed_file::State::FILE_IS_FORBIDDEN);
    	 managed_file->compatible(false);
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

	 if(task->condition() || !managed_file->compatible()){ // cancellation was requested or the file is incompatible:
		 LOG (WARNING) << "Cancellation was requested during file read \"" << path << "\" from \"" << fsDescriptor.dfs_type << ":" <<
		 				 fsDescriptor.host << "\"" << ". This file was not cached. \n";
		 filemgmt::FileSystemManager::instance()->dfsDelete(fsAdaptor->descriptor(), managed_file->relative_name().c_str(), true);
	 }

     // check the integrity of local bytes and remote bytes for managed file and assign the appropriate status:
	 // this is relevant only for original data
	 if(managed_file->transformCmd().empty() && (managed_file->remote_size() != managed_file->size())){
		 fp->errdescr = "File is not consistent with remote origin";
		 fp->error = true;
		 fp->progressStatus = FileProgressStatus::fileProgressStatus::FILEPROGRESS_GENERAL_FAILURE;

		 managed_file->state(managed_file::State::FILE_IS_FORBIDDEN);
		 LOG (ERROR) << "File \"" << managed_file->fqp() << "\" has inconsistent size and is marked as forbidden.\n";
		 status = status::StatusInternal::CACHE_OBJECT_IS_FORBIDDEN;
	 }
	 // mark the file as just synchronized to avoid its possible recycling due "clients = 0" reason.
     managed_file->state(managed_file::State::FILE_SYNC_JUST_HAPPEN);
	 managed_file->close();
	 return status;
}

status::StatusInternal Sync::transformExistingFile(managed_file::File*& file){
	status::StatusInternal status = status::StatusInternal::OK;

	// reply the object is incompatible :
	if(file->transformCmd().empty() && !file->compatible())
		return status::StatusInternal::CACHE_OBJECT_IS_INCOMPATIBLE;

	// 1. extract the transformation command:
    std::string command = file->transformCmd();

    // 2. Parse command into the "program name" and varlen list of strings-parameters,
    // to be compatible with exec signature:
    utilities::ProgramInvocationDetails invocation_details(command);
    if(invocation_details.program() == NULL)
    	return status::StatusInternal::CACHE_OBJECT_IS_INCOMPATIBLE;

    /* 3. Run the externally defined transformation scenario.
     * We expect for result file would be of the same name. If there was a problem for externally defined
     * utility to transform the file (for example, no free space exists within the filesystem to complete transformation),
     * externally defined utility is responsible to return <> 0 ret status.
     * In this case, the file will be marked accordingly in the registry - as forbidden and non-compatible -
     * and removed physically from the file system.
     * If externally defined routine completes successfully, it should return 0 ret status.
     * In case of success, we look for local file and update the file metadata (currently, only size) according to renewed
     * file physical state.
     */
    pid_t   pid;                // process id acquired by spawning a child process to run externally defined command
    const int EXEC_ERROR = 100; // make a hint to distinguish the exec failure

    // if this is the child process (pid is zero).
    // TODO : more lightweight call to duplicate process?
    if (0 == (pid = fork())) {
    	// exec functions replace current (child) process.                                       (1)
    	if (-1 == execve(invocation_details.program(), (char **)invocation_details.args() , NULL)) {
    		// Due to (1), the only possible child outcome could happen here : failure to start the child process:
    		exit(EXEC_ERROR);
    	}
    	// anything here should not be reached
    	else
    		exit(0);
    } // else we are in the parent

    int ret;
    // we are in the parent, wait for child completion:
    while (0 == waitpid(pid , &ret , WNOHANG)) {
    	sleep(5);
    }

    if (1 != WIFEXITED(ret) || 0 != WEXITSTATUS(ret)) {
    	LOG (ERROR) << "Execution of \"" << invocation_details.program() << "\" resulted in error : "
    			<< WEXITSTATUS(ret) << ".\n";
    	if(ret == EXEC_ERROR)
    		LOG (ERROR) << "Failed to execute \"" << invocation_details.program() << "\".\n";

    	// data is unusable now!
    	file->state(managed_file::State::FILE_IS_FORBIDDEN);
    	file->compatible(false);
    	// anyway, update the actual file size:
    	file->estimated_size(file->size(), true);

    	// these flags will help the cache host to clean the file from the file system and
    	// reduce the cache capacity.
    	return status::StatusInternal::CACHE_OBJECT_IS_INCOMPATIBLE;
    }
    // rescan the underlying data size from the filesystem and update the managed file accordingly:
    file->estimated_size(file->size(), true);
    // mark file as "compatible":
    file->compatible(true);

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


