/*
 * @file filesystem-mgr.h
 * @brief Define Filesystem Management features
 *
 * @date   Oct 3, 2014
 * @author elenav
 */
#include <string>

#include <stdio.h>
#include <fcntl.h>
#include <dirent.h>
#include <fstream>

#include "dfs_cache/filesystem-mgr.hpp"
#include "dfs_cache/uri-util.hpp"

/**
 * @namespace impala
 */
namespace impala{

/**
 * @namespace filemgmt
 */
namespace filemgmt{

boost::scoped_ptr<FileSystemManager> FileSystemManager::instance_;

void FileSystemManager::init() {
  if(FileSystemManager::instance_.get() == NULL)
	  FileSystemManager::instance_.reset(new FileSystemManager());
}

std::string FileSystemManager::constructLocalPath(const FileSystemDescriptor& fsDescriptor, const char* path){
    std::string localPath(CacheLayerRegistry::instance()->localstorage());
    localPath += fsDescriptor.host;
    localPath += path;
    return localPath;
}

dfsFile FileSystemManager::dfsOpenFile(const FileSystemDescriptor & fsDescriptor, const char* path, int flags,
                      int bufferSize, short replication, tSize blocksize, bool& available){

	Uri uri = Uri::Parse(path);
	dfsFile file = new dfsFile_internal{nullptr, dfsStreamType::UNINITIALIZED};

	// calculate fully qualified local path from requested
	std::string localPath = constructLocalPath(fsDescriptor, uri.FilePath.c_str());
    const char* localPathAnsi = localPath.c_str();

    // check we are able to process requested file mode.
	// if no,  take no actions.
	std::string mode;
	// get the mode to open the file in:
	switch(flags){
		case O_RDONLY:
			mode = "r";
			break;
		case O_WRONLY:
			mode = "w";
			break;
	    case O_RDWR:
	    	mode = "rw";
	    	break;
	    case O_CREAT:
	    	mode = "w+b";
	    	break;
	    default:
	    	break;
	}
	if(mode.empty()){
		available = false;
		return NULL;
	}

	// create file scenario. It is only for internal layer usage:
	if(flags == O_CREAT){
		FILE *lofile;
		// first check if the file exists:
		lofile = fopen(localPathAnsi, "rw");
		if (lofile == NULL)
			available = false;
		else {
			available = true;
			fclose(lofile);
		}
		if(!available){
			lofile = fopen(localPathAnsi, mode.c_str());
			if(lofile != NULL){
				available = true;
				fclose(lofile);
			}
			else{
				// there was a problem to create the file..
				return NULL;
			}
		}
	}
	// If this file is not available locally, reply with error.
	int pfd; /* Integer for file descriptor returned by open() call. */

	if ((pfd = open(localPathAnsi, flags,
	    S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) == -1)
	{
	    available = false;
	    return NULL;
	}

	// get the file pointer from descriptor:
	FILE *fp;
	try{
		fp = fdopen(pfd, mode.c_str());
	}
	catch(...){
		fp = NULL;
	}
	if(fp == NULL){
	    // close file descriptor:
	    close(pfd);
		available = false;
		return NULL;
	}
	// Got file opened, reply it
	file = new dfsFile_internal{fp, dfsStreamType::INPUT};
	available = true;
	return file;
}

status::StatusInternal FileSystemManager::dfsCloseFile(const FileSystemDescriptor & fsDescriptor, dfsFile file){
	if(file->file == nullptr)
		return status::StatusInternal::FILE_OBJECT_OPERATION_FAILURE;
	int fd = fileno((FILE *)file->file);
	// close file stream:
    fclose((FILE*)file->file);
    // close file descriptor:
    close(fd);
	return status::StatusInternal::OK;
}

status::StatusInternal FileSystemManager::dfsExists(const FileSystemDescriptor & fsDescriptor, const char *path){
	Uri uri = Uri::Parse(path);
	std::string localPath = constructLocalPath(fsDescriptor, uri.FilePath.c_str());

	 std::ifstream infile(localPath.c_str());
	    return infile.good() ? status::StatusInternal::OK : status::StatusInternal::DFS_OBJECT_DOES_NOT_EXIST;
}

status::StatusInternal FileSystemManager::dfsSeek(const FileSystemDescriptor & fsDescriptor, dfsFile file, tOffset desiredPos){
	if(file->file == nullptr)
		return status::StatusInternal::FILE_OBJECT_OPERATION_FAILURE;
	int ret = 0;
	ret = fseek((FILE*)file->file, desiredPos, SEEK_SET);
	return ret == 0 ? status::StatusInternal::OK : status::StatusInternal::FILE_OBJECT_OPERATION_FAILURE;
}

tOffset FileSystemManager::dfsTell(const FileSystemDescriptor & fsDescriptor, dfsFile file){
	if(file->file == nullptr)
		return -1;

	return ftell ((FILE*)file->file);
}

tSize FileSystemManager::dfsRead(const FileSystemDescriptor & fsDescriptor, dfsFile file, void* buffer, tSize length){
	//Sanity check
	if (!file || file->type == UNINITIALIZED) {
		return -1;
	}

	// Error checking... make sure that this file is 'readable'
	if (file->type != INPUT) {
		return -1;
	}

   return fread(buffer, 1, length, (FILE*)file->file);
}

tSize FileSystemManager::dfsPread(const FileSystemDescriptor & fsDescriptor, dfsFile file, tOffset position, void* buffer, tSize length){
	ssize_t bytes_read;
	if(file->file == nullptr)
		return -1;

	int fd = fileno((FILE *)file->file);

	bytes_read = read(fd, buffer, length);
	return bytes_read;
}

tSize FileSystemManager::dfsWrite(const FileSystemDescriptor & fsDescriptor, dfsFile file, const void* buffer, tSize length){
	if(file->file == nullptr)
			return -1;

	ssize_t bytes_written;
	int fd = fileno((FILE *)file->file);
	bytes_written = write(fd, buffer, length);
	return bytes_written;
}

status::StatusInternal FileSystemManager::dfsFlush(const FileSystemDescriptor & fsDescriptor, dfsFile file){
	return status::StatusInternal::NOT_IMPLEMENTED;
}

status::StatusInternal FileSystemManager::dfsHFlush(const FileSystemDescriptor & fsDescriptor, dfsFile file){
	return status::StatusInternal::NOT_IMPLEMENTED;
}

tOffset FileSystemManager::dfsAvailable(const FileSystemDescriptor & fsDescriptor, dfsFile file){
	return -1;
}

status::StatusInternal FileSystemManager::dfsCopy(const FileSystemDescriptor & fsDescriptor, const char* src, const char* dst){
	return status::StatusInternal::NOT_IMPLEMENTED;
}

status::StatusInternal FileSystemManager::dfsCopy(const FileSystemDescriptor & fsDescriptor1, const char* src,
		const FileSystemDescriptor & namenode2, const char* dst){
	return status::StatusInternal::NOT_IMPLEMENTED;
}

status::StatusInternal FileSystemManager::dfsMove(const FileSystemDescriptor & fsDescriptor, const char* src, const char* dst){
	return status::StatusInternal::NOT_IMPLEMENTED;
}

status::StatusInternal FileSystemManager::dfsDelete(const FileSystemDescriptor & fsDescriptor, const char* path, int recursive){
	// construct the fqlp:
	Uri uri = Uri::Parse(path);
	std::string localPath = constructLocalPath(fsDescriptor,  uri.FilePath.c_str());

	if(std::remove(localPath.c_str()) == 0)
		return status::StatusInternal::OK;
	return status::StatusInternal::FILE_OBJECT_OPERATION_FAILURE;
}

status::StatusInternal FileSystemManager::dfsRename(const FileSystemDescriptor & fsDescriptor, const char* oldPath, const char* newPath){
	return status::StatusInternal::NOT_IMPLEMENTED;
}

status::StatusInternal FileSystemManager::dfsCreateDirectory(const FileSystemDescriptor & fsDescriptor, const char* path){
	return status::StatusInternal::OK;
}

status::StatusInternal FileSystemManager::dfsSetReplication(const FileSystemDescriptor & fsDescriptor, const char* path, int16_t replication){
	return status::StatusInternal::OK;
}

static int
getFileInfoInternal(dirent ent, dfsFileInfo *fileInfo)
{
    fileInfo->mKind = ent.d_type == 'd' ? kObjectKindDirectory : kObjectKindFile;
    fileInfo->mReplication = 0;
    fileInfo->mBlockSize = 0;
    fileInfo->mLastMod = (tTime) (0);
    fileInfo->mLastAccess = (tTime) (0);
    if (fileInfo->mKind == kObjectKindFile) {
        fileInfo->mSize = 0;
    }
    fileInfo->mName = strdup(ent.d_name);
    fileInfo->mOwner = strdup("user_name");
    fileInfo->mGroup = strdup("group_name");
    fileInfo->mPermissions = 0;

    return 1;
}

dfsFileInfo * FileSystemManager::dfsListDirectory(const FileSystemDescriptor & fsDescriptor, const char* path,
                                int *numEntries){
	DIR *dir;
	struct dirent *ent;
	std::vector<dirent> entries;

	Uri uri = Uri::Parse(path);
	std::string localPath = constructLocalPath(fsDescriptor, uri.FilePath.c_str());

	dfsFileInfo* reply;

	if ((dir = opendir (localPath.c_str())) != NULL) {
	  /* print all the files and directories within directory */
	  while ((ent = readdir (dir)) != NULL) {
		  entries.push_back(*ent);
		  printf ("%s\n", ent->d_name);
	  }
	  closedir (dir);
	} else {
	  /* could not open directory */
	  perror ("");
	  return nullptr;
	}
	reply = (dfsFileInfo*)calloc(entries.size(), sizeof(dfsFileInfo));
	if (reply == NULL) {
		return reply;
	}

	//Save path information in pathList
	for (auto item : entries) {
		getFileInfoInternal(item, reply++);
	}

	return reply;
}

dfsFileInfo * FileSystemManager::dfsGetPathInfo(const FileSystemDescriptor & fsDescriptor, const char* path){
	return nullptr;
}

void FileSystemManager::dfsFreeFileInfo(const FileSystemDescriptor & fsDescriptor, dfsFileInfo *dfsFileInfo, int numEntries){
    // Free the mName, mOwner, and mGroup
    int i;
    for (i = 0; i < numEntries; ++i) {
        if (dfsFileInfo[i].mName) {
            free(dfsFileInfo[i].mName);
        }
        if (dfsFileInfo[i].mOwner) {
        	free(dfsFileInfo[i].mOwner);
        }
        if (dfsFileInfo[i].mGroup) {
            free(dfsFileInfo[i].mGroup);
        }
    }
    // Free entire block
    free(dfsFileInfo);

}

tOffset FileSystemManager::dfsGetCapacity(const FileSystemDescriptor & fsDescriptor, const char* host){
	return status::StatusInternal::OK;
}

tOffset FileSystemManager::dfsGetUsed(const FileSystemDescriptor & fsDescriptor, const char* host){
	return status::StatusInternal::OK;
}

status::StatusInternal FileSystemManager::dfsChown(const FileSystemDescriptor & fsDescriptor, const char* path, const char *owner, const char *group){
	return status::StatusInternal::OK;
}

status::StatusInternal FileSystemManager::dfsChmod(const FileSystemDescriptor & fsDescriptor, const char* path, short mode){
	return status::StatusInternal::OK;
}

} // filemgmt
} // impala

