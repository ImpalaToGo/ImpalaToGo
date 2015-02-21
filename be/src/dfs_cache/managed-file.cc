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

/*
std::ostream& operator<<(std::ostream& out, const managed_file::State value){
	static std::map<managed_file::State, std::string> strings;
	if (strings.size() == 0) {
#define INSERT_ELEMENT(p) strings[p] = #p
		INSERT_ELEMENT(FILE_IS_MARKED_FOR_DELETION);
		INSERT_ELEMENT(FILE_IS_IN_USE_BY_SYNC);
		INSERT_ELEMENT(FILE_HAS_CLIENTS);
		INSERT_ELEMENT(FILE_IS_AMORPHOUS);
		INSERT_ELEMENT(FILE_IS_IDLE);
		INSERT_ELEMENT(FILE_IS_FORBIDDEN);
		INSERT_ELEMENT(FILE_IS_UNDER_WRITE);
#undef INSERT_ELEMENT
	}
	return out << strings[value];
}
*/

std::string File::fileSeparator;
std::vector<std::string> File::m_supportedFs;

int File::_defaultTimeSliceInSeconds = 20;

void File::initialize(){
	  // configure platform-specific file separator:
	  boost::filesystem::path slash("/");
	  boost::filesystem::path::string_type preferredSlash = slash.make_preferred().native();
	  fileSeparator = preferredSlash;

	  // configure supported file systems:
	  m_supportedFs.push_back(constants::HDFS_SCHEME);
	  m_supportedFs.push_back(constants::S3N_SCHEME);
	  m_supportedFs.push_back(constants::LOCAL_SCHEME);
}

std::string File::constructLocalPath(const FileSystemDescriptor& fsDescriptor, const char* path){
    std::string localPath(CacheLayerRegistry::instance()->localstorage());

    std::ostringstream stream;
    stream << fsDescriptor.dfs_type;

    // add FileSystem type on top of hierarchy:
    localPath += stream.str();

    //if(!fsDescriptor.host.empty())
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
	if(boost::iequals(schema, constants::LOCAL_SCHEME))
		descriptor.dfs_type = DFS_TYPE::local;

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

	// if there's other schema than local filesystem is detected but the host is unknown, report back immediatelly
    if((descriptor.dfs_type != DFS_TYPE::local) && descriptor.host.empty())
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
	fqnp += (descriptor.dfs_type == DFS_TYPE::local ? ":/" : "://");
	fqnp += descriptor.host;
	// for 3sn or local filesystem, don't add the port into uri
	if((descriptor.dfs_type != DFS_TYPE::s3n) && (descriptor.dfs_type != DFS_TYPE::local)){
		fqnp += ":";
		fqnp += std::to_string(descriptor.port);
	}
	fqnp += catalog;
	fqnp += fileSeparator;
	fqnp += filename;

	// validate descriptor:
	descriptor.valid = true;
	return descriptor;
}

status::StatusInternal File::open( int ref_count) {
	if(m_state == State::FILE_IS_MARKED_FOR_DELETION)
		return status::StatusInternal::CACHE_OBJECT_UNDER_FINALIZATION;

	// don't change 2 states below:
	if((m_state != State::FILE_IS_FORBIDDEN) && (m_state != State::FILE_IS_IN_USE_BY_SYNC))
		m_state = State::FILE_HAS_CLIENTS;
	std::atomic_fetch_add_explicit (&m_users, ref_count, std::memory_order_relaxed);
	LOG (INFO) << "File open \"" << fqp() << "\" refs = " << m_users.load(std::memory_order_acquire) << " ; File status = \""
			<< m_state << "\"" << std::endl;
	return status::OK;
}

status::StatusInternal File::close(int ref_count) {
	if(m_state == State::FILE_IS_MARKED_FOR_DELETION)
		return status::StatusInternal::CACHE_OBJECT_UNDER_FINALIZATION;

	// protect all the flow below to guarantee the whole method will be executed even if the object
	// is being watched by cleanup right now, when the last client is detaching the ownership from
	// this object and running this flow in its context
	boost::unique_lock<boost::mutex> lock(m_closure_mux);
    if ( std::atomic_fetch_sub_explicit (&m_users, ref_count, std::memory_order_release) == ref_count ) {
		std::atomic_thread_fence(std::memory_order_acquire);
		if((m_state != State::FILE_IS_IN_USE_BY_SYNC) && (m_state != State::FILE_SYNC_JUST_HAPPEN))
			// don't change the state!
			m_state = State::FILE_IS_IDLE;

		LOG (INFO) << "File \"" << fqp() << "\" is no more referenced. refs = " << m_users.load(std::memory_order_acquire) << std::endl;
	}
	LOG (INFO) << "File close \"" << fqp() << "\" refs = " << m_users.load(std::memory_order_acquire) <<
			" ; File status = \"" << m_state << "\"" << std::endl;
	return status::OK;
}

bool File::drop(){
	// we only drop objects marked for finalization:
	if(m_state.load(std::memory_order_acquire) != State::FILE_IS_MARKED_FOR_DELETION)
		return false;

	// if there're clients using the file in read/write or clients who is waiting for the file update,
	// the file cannot be deleted
	if(m_state.load(std::memory_order_acquire) == State::FILE_HAS_CLIENTS || m_subscribers.load(std::memory_order_acquire) != 0){
		  LOG (WARNING) << "Rejecting an attempt to delete file \"" << fqp() << "\". Reason : in direct use or referenced." << "\n";
		return false;
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
		return true;
	}
	LOG (ERROR) << "Failed to delete the file \"" << fqp() << "\". Message : \"" << ec.message() << "\".\n";
	return false;
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


} /** namespace managed_file */
} /** namespace impala */

