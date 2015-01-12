/*
 * @file filesystem-descriptor-bound.cc
 * @brief implementation of hadoop FileSystem mediator (mainly types translator)
 *
 * @date   Oct 10, 2014
 * @author elenav
 */

#include "dfs_cache/filesystem-descriptor-bound.hpp"
#include "dfs_cache/hadoop-fs-adaptive.h"

namespace impala {

std::ostream& operator<<(std::ostream& out, const DFS_TYPE& value) {
	static std::map<DFS_TYPE, std::string> strings;
	if (strings.size() == 0) {
#define INSERT_ELEMENT(p) strings[p] = #p
		INSERT_ELEMENT(hdfs);
		INSERT_ELEMENT(s3n);
		INSERT_ELEMENT(LOCAL);
		INSERT_ELEMENT(DEFAULT_FROM_CONFIG);
		INSERT_ELEMENT(OTHER);
		INSERT_ELEMENT(NON_SPECIFIED);
#undef INSERT_ELEMENT
	}
	return out << strings[value];
}

fsBridge FileSystemDescriptorBound::connect() {
	fsBuilder* fs_builder = _dfsNewBuilder();
	if (!m_fsDescriptor.host.empty()) {
		_dfsBuilderSetHostAndFilesystemType(fs_builder,	m_fsDescriptor.host.c_str(),
				m_fsDescriptor.dfs_type);
	} else {
		// Connect to local filesystem
		_dfsBuilderSetHost(fs_builder, NULL);
	}
	// forward the port to the unsigned builder's port only if the port is positive
	if(m_fsDescriptor.port > 0)
		_dfsBuilderSetPort(fs_builder, m_fsDescriptor.port);
	return _dfsBuilderConnect(fs_builder);
}

FileSystemDescriptorBound::~FileSystemDescriptorBound(){
	// Disconnect any conections we have to a target file system:
	for(auto item : m_connections){
		_dfsDisconnect(item->connection);
	}
}

int FileSystemDescriptorBound::resolveFsAddress(FileSystemDescriptor& fsDescriptor){
	int status = -1;
	// create the builder from descriptor
	fsBuilder* fs_builder = _dfsNewBuilder();
	// is there's host specified, set it:

	if (!fsDescriptor.host.empty())
		_dfsBuilderSetHost(fs_builder, fsDescriptor.host.c_str());
	else
		// Connect to local filesystem
		_dfsBuilderSetHost(fs_builder, NULL);

	// set the port:
	_dfsBuilderSetPort(fs_builder, fsDescriptor.port);

	// now get effective host, port and filesystem type from Hadoop FileSystem resolver:
	char host[HOST_NAME_MAX];
    status = _dfsGetDefaultFsHostPortType(host, sizeof(host), fs_builder, &fsDescriptor.port, &fsDescriptor.dfs_type);

    if(!status){
    	fsDescriptor.host = std::string(host);
    	// if port is not specified, set 0
    	fsDescriptor.port = fsDescriptor.port < 0 ? 0 : fsDescriptor.port;
    }
	return status;
}

raiiDfsConnection FileSystemDescriptorBound::getFreeConnection() {
	freeConnectionPredicate predicateFreeConnection;

	boost::mutex::scoped_lock(m_mux);
	std::list<boost::shared_ptr<dfsConnection> >::iterator i1;

	// First try to find the free connection:
	i1 = std::find_if(m_connections.begin(), m_connections.end(),
			predicateFreeConnection);
	if (i1 != m_connections.end()) {
		// return the connection, mark it busy!
		(*i1)->state = dfsConnection::BUSY_OK;
		return std::move(raiiDfsConnection(*i1));
	}

	// check any other connections except in "BUSY_OK" or "FREE_INITIALIZED" state.
	anyNonInitializedConnectionPredicate uninitializedPredicate;
	std::list<boost::shared_ptr<dfsConnection> >::iterator i2;

	i2 = std::find_if(m_connections.begin(), m_connections.end(),
			uninitializedPredicate);
	if (i2 != m_connections.end()) {
		// have ubnormal connections, get the first and reinitialize it:
		fsBridge conn = connect();
		if (conn != NULL) {
			LOG (INFO)<< "Existing non-initialized connection is initialized and will be used for file system \"" << m_fsDescriptor.dfs_type << ":"
			<< m_fsDescriptor.host << "\"" << "\n";
			(*i2)->connection = conn;
			(*i2)->state = dfsConnection::BUSY_OK;
			return std::move(raiiDfsConnection(*i2));
		}
		else
		return std::move(raiiDfsConnection(dfsConnectionPtr())); // no connection can be established. No retries right now.
	}

	// seems there're no unused connections right now.
	// need to create new connection to DFS:
	LOG (INFO)<< "No free connection exists for file system \"" << m_fsDescriptor.dfs_type << ":" << m_fsDescriptor.host << "\", going to create one." << "\n";
	boost::shared_ptr<dfsConnection> connection(new dfsConnection());
	connection->state = dfsConnection::NON_INITIALIZED;

	fsBridge conn = connect();
	if (conn != NULL) {
		connection->connection = conn;
		connection->state = dfsConnection::FREE_INITIALIZED;
		m_connections.push_back(connection);
		return getFreeConnection();
	}
	LOG (ERROR)<< "Unable to connect to file system \"." << "\"" << "\n";
	// unable to connect to DFS.
	return std::move(raiiDfsConnection(dfsConnectionPtr()));
}

dfsFile FileSystemDescriptorBound::fileOpen(raiiDfsConnection& conn, const char* path, int flags, int bufferSize,
		short replication, tSize blocksize){
	return _dfsOpenFile(conn.connection()->connection, path, flags, bufferSize, replication, blocksize);
}

int FileSystemDescriptorBound::fileClose(raiiDfsConnection& conn, dfsFile file){
	return _dfsCloseFile(conn.connection()->connection, file);
}

tOffset FileSystemDescriptorBound::fileTell(raiiDfsConnection& conn, dfsFile file){
    return _dfsTell(conn.connection()->connection, file);
}

int FileSystemDescriptorBound::fileSeek(raiiDfsConnection& conn, dfsFile file,
		tOffset desiredPos){
	return _dfsSeek(conn.connection()->connection, file, desiredPos);
}

tSize FileSystemDescriptorBound::fileRead(raiiDfsConnection& conn, dfsFile file,
		void* buffer, tSize length){
	return _dfsRead(conn.connection()->connection, file, buffer, length);
}

tSize FileSystemDescriptorBound::filePread(raiiDfsConnection& conn, dfsFile file, tOffset position,
		void* buffer, tSize length){
	return _dfsPread(conn.connection()->connection, file, position, buffer, length);
}

tSize FileSystemDescriptorBound::fileWrite(raiiDfsConnection& conn, dfsFile file, const void* buffer, tSize length){
	return _dfsWrite(conn.connection()->connection, file, buffer, length);
}

int FileSystemDescriptorBound::fileRename(raiiDfsConnection& conn, const char* oldPath, const char* newPath){
	return _dfsRename(conn.connection()->connection, oldPath, newPath);
}

int FileSystemDescriptorBound::pathDelete(raiiDfsConnection& conn, const char* path, int recursive){
	return _dfsDelete(conn.connection()->connection, path, recursive);
}

dfsFileInfo* FileSystemDescriptorBound::fileInfo(raiiDfsConnection& conn, const char* path){
	return _dfsGetPathInfo(conn.connection()->connection, path);
}

void FileSystemDescriptorBound::freeFileInfo(dfsFileInfo* fileInfo, int numOfEntries){
	return _dfsFreeFileInfo(fileInfo, numOfEntries);
}

bool FileSystemDescriptorBound::pathExists(raiiDfsConnection& conn, const char* path){
	return (_dfsPathExists(conn.connection()->connection, path) == 0 ? true : false);
}

int FileSystemDescriptorBound::fileCopy(raiiDfsConnection& conn_src, const char* src, raiiDfsConnection& conn_dest, const char* dst){
	return (_dfsCopy(conn_src.connection()->connection, src, conn_dest.connection()->connection, dst) == 0 ? true : false);
}

int64_t FileSystemDescriptorBound::getDefaultBlockSize(raiiDfsConnection& conn){
	return _dfsGetDefaultBlockSize(conn.connection()->connection);
}

} /** namespace impala */

