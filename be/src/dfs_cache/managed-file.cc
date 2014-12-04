/*
 * @file managed-file.cc
 * @brief internal implementation of Managed File
 *
 * @date   Oct 13, 2014
 * @author elenav
 */

#include "dfs_cache/cache-layer-registry.hpp"
#include "dfs_cache/managed-file.hpp"
#include "dfs_cache/utilities.hpp"

namespace impala {

namespace managed_file {

std::string File::fileSeparator;
std::vector<std::string> File::m_supportedFs;

int File::_defaultTimeSliceInMinutes = 5;

void File::initialize(){
	  // configure platform-specific file separator:
	  boost::filesystem::path slash("/");
	  boost::filesystem::path::string_type preferredSlash = slash.make_preferred().native();
	  fileSeparator = preferredSlash;

	  // configure supported file systems:
	  m_supportedFs.push_back(constants::HDFS_SCHEME);
	  m_supportedFs.push_back(constants::S3N_SCHEME);
}

std::string File::constructLocalPath(const FileSystemDescriptor& fsDescriptor, const char* path){
    std::string localPath(CacheLayerRegistry::instance()->localstorage());

    std::ostringstream stream;
    stream << fsDescriptor.dfs_type;

    // add FileSystem type on top of hierarchy:
    localPath += stream.str();

    if(!fsDescriptor.host.empty())
    	localPath += fileSeparator;
    localPath += fsDescriptor.host;
    localPath += constants::HOST_PORT_SEPARATOR;
    localPath += std::to_string(fsDescriptor.port);
    localPath += path;
    return localPath;
}

FileSystemDescriptor File::restoreNetworkPathFromLocal(const std::string& local, std::string& fqnp, std::string& relative){
	std::string root(CacheLayerRegistry::instance()->localstorage());

	fqnp = "";
	FileSystemDescriptor descriptor;
	descriptor.valid = false;

	// create the path object from local path:
	boost::filesystem::path local_path(local);
	LOG (INFO) << "substr to cut the local configured cache root from path. Root : \"" <<
			root << "\"; local_path : \"" << local_path << "\" n";

	// cut the local cache configured root:
	std::string temp = local_path.string().substr(root.length(), local_path.string().length() - root.length());

	// remainder is what describes the source filesystem for cached file:
	boost::filesystem::path fqdn_to_resolve(temp);

	boost::filesystem::path::iterator it = fqdn_to_resolve.begin();

	if(it == fqdn_to_resolve.end())
		return descriptor;

	// 1. this should be one of schema we support:
	std::string schema = (*it++).string();
	if(std::find_if(m_supportedFs.begin(), m_supportedFs.end(), utilities::insensitive_compare(schema)) == m_supportedFs.end())
		// nothing found for parsed schema
		return descriptor;

	if(boost::iequals(schema, constants::HDFS_SCHEME))
		descriptor.dfs_type = DFS_TYPE::hdfs;
	if(boost::iequals(schema, constants::S3N_SCHEME))
		descriptor.dfs_type = DFS_TYPE::s3n;

	if(it == fqdn_to_resolve.end())
		return descriptor;

	// 2. Parse host_port:
	std::string host_port = (*it++).string();

	// if there's no remainder with catalog and file name, or host_port separator is not found, go out
	if((it == fqdn_to_resolve.end()) || (host_port.find(constants::HOST_PORT_SEPARATOR) == std::string::npos))
		return descriptor;

	// check that we have host_port separator in the host_port
	std::vector<std::string> host_port_pair = utilities::split(host_port, constants::HOST_PORT_SEPARATOR);

	if(host_port_pair.size() != 2)
		return descriptor;

	// 2.1 remote origin fs host
	descriptor.host = host_port_pair[0];
	try{
		// 2.2 remote origin fs port
		descriptor.port = std::stoul(host_port_pair[1]);
	}
	catch(...){
		// invalidate port
		descriptor.port = -1;
		return descriptor;
	}

    if(descriptor.host.empty())
    	return descriptor;

    LOG (INFO) << "substr to cut the catalog and filename : initial string \"" <<
    			temp << "\"; schema : \"" << schema << "\"; host_port \"" << host_port << "\".\n";

    int offsetcatalog = schema.length() + host_port.length() + fileSeparator.length();

    // 3. get catalog and the file name:
	temp = temp.substr(offsetcatalog, (temp.length() - offsetcatalog ));

	boost::filesystem::path catalog_filename(temp);

	// 3.1 catalog:
	std::string catalog = catalog_filename.parent_path().string();
	// 3.2 file name:
	std::string filename = catalog_filename.filename().string();
	if(catalog.empty() || filename.empty())
		return descriptor;

	// 4. save relative file name:
	relative = catalog_filename.string();
	// now having all we need to construct remote origin file system path which we call fqdn, go and construct:
	fqnp = schema;
	fqnp += "://";
	fqnp += descriptor.host;
	fqnp += ":";
	fqnp += std::to_string(descriptor.port);
	fqnp += catalog;
	fqnp += filename;

	// validate descriptor:
	descriptor.valid = true;
	return descriptor;
}

status::StatusInternal File::open() {
	m_state = State::FILE_HAS_CLIENTS;
	std::atomic_fetch_add_explicit (&m_users, 1u, std::memory_order_relaxed);
	return status::OK;
}

status::StatusInternal File::close() {
	if ( std::atomic_fetch_sub_explicit (&m_users, 1u, std::memory_order_release) == 1 ) {
		std::atomic_thread_fence(std::memory_order_acquire);
		m_state = State::FILE_IS_IDLE;
	}
	return status::OK;
}

void File::drop(){
	// if there're clients using the file in read/write or clients who is waiting for the file update,
	// the file cannot be deleted
	if(m_state.load(std::memory_order_acquire) == State::FILE_HAS_CLIENTS || m_subscribers.load(std::memory_order_acquire) != 0){
		  LOG (WARNING) << "Rejecting an attempt to delete file \"" << fqp() << "\". Reason : in direct use or referenced." << "\n";
		return;
	}

	boost::system::error_code ec;

	try {
		boost::filesystem::remove(fqp(), ec);
	}
	catch (boost::filesystem::filesystem_error &e) {
		LOG (ERROR) << "Failed to delete the file \"" << fqp() << "\". Ex : " <<
		e.what() << "\n";
	}
	if(!ec){
		LOG (INFO) << "File \"" << fqp() << "\" is removed from file system." << "\n";
		return;
	}
	LOG (ERROR) << "Failed to delete the file \"" << fqp() << "\". Message : \"" << ec.message() << "\".\n";
}

status::StatusInternal File::forceDelete(){
	boost::system::error_code ec;
	try {
		boost::filesystem::remove(fqp(), ec);
	}
	catch (boost::filesystem::filesystem_error &e) {
		LOG (ERROR) << "Failed to forcibly delete the file \"" << fqp() << "\". Ex : " <<
		e.what() << "\n";
	}
	if(!ec)
		return status::StatusInternal::OK;
	else{
		LOG (ERROR) << "Failed to forcibly delete the file \"" << fqp() << "\"." << "\n";
		return status::StatusInternal::FILE_OBJECT_OPERATION_FAILURE;
	}
}


} /** namespace ManagedFile */
} /** namespace impala */

