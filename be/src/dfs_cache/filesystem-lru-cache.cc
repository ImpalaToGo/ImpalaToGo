/*
 * @file  filesystem-lru-cache.cc
 * @brief additional implementation of file system LRU cache
 *
 * @date Nov 20, 2014
 * @author elenav
 */

#include <map>
#include <boost/regex.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include "dfs_cache/cache-mgr.hpp"
#include "dfs_cache/filesystem-lru-cache.hpp"
#include "dfs_cache/utilities.hpp"

namespace impala{

bool FileSystemLRUCache::deleteFile(managed_file::File* file, bool physically){
	// preserve path for future usage:
	std::string path = file->fqp();
	{
		std::lock_guard<std::mutex> lock(m_deletionsmux);

		// add the item into deletions list
		m_deletionList.push_back(file->fqp());
	}
	// notify deletion action is scheduled
	m_deletionHappensCondition.notify_all();

	// for physical removal scenario, drop the file from file system
	if (physically) {
		LOG (INFO) << "File \"" << file->fqp() << "\" is near to be removed from the disk." << "\n";
		// delegate further deletion scenario to the file itself:
		file->drop();
	}

	// get rid of file metadata object:
	delete file;

	{
		std::lock_guard<std::mutex> lock(m_deletionsmux);

    	// now drop the file from deletions list:
    	m_deletionList.remove(path);
	}
	// notify deletion happens
	m_deletionHappensCondition.notify_all();

	return true;
}

bool FileSystemLRUCache::deletePath(const std::string& path){
	boost::filesystem::recursive_directory_iterator end_iter;

	// collection to hold files under the path:
	typedef std::vector<boost::filesystem::path> files;
	typedef std::vector<boost::filesystem::path>::iterator files_it;
	files _files;

	bool ret = true;

	if (!boost::filesystem::exists(path))
		return false;

	// if path is the directory:
	if (boost::filesystem::is_directory(path)){
		for (boost::filesystem::recursive_directory_iterator dir_iter(path);
				dir_iter != end_iter; ++dir_iter) {
			if (boost::filesystem::is_regular_file(dir_iter->status())) {
				_files.push_back(*dir_iter);
			}
		}
		files_it it = _files.begin();

		if (it == _files.end())
			return true;

		for (; it != _files.end(); it++) {
			// drop all files:
			ret = ret && remove((*it).string(), true);
		}
		// if all files were removed, its safe to remove the path completely
		if(ret)
			boost::filesystem::remove_all(path);
		return ret;
	}

	// the path is a file, so just remove it physically
	return remove(path, true);
}

void FileSystemLRUCache::sync(managed_file::File* file){
	// if file is not valid, break the handler
	if (file == nullptr || !file->valid())
		return;

	// and wait for prepare operation will be finished:
	DataSet data;
	data.push_back(file->relative_name().c_str());

	bool condition = false;
	boost::condition_variable condition_var;
	boost::mutex completion_mux;

	status::StatusInternal cbStatus;

	PrepareCompletedCallback cb =
			[&] (SessionContext context,
					const std::list<boost::shared_ptr<FileProgress> > & progress,
					request_performance const & performance, bool overall,
					bool canceled, taskOverallStatus status) -> void {

				cbStatus = (status == taskOverallStatus::COMPLETED_OK ? status::StatusInternal::OK : status::StatusInternal::REQUEST_FAILED);
				if(status != taskOverallStatus::COMPLETED_OK) {
					LOG (ERROR) << "Failed to load file \"" << file->fqp() << "\"" << ". Status : "
					<< status << ".\n";
					file->state(managed_file::State::FILE_IS_FORBIDDEN);
				}
				if(context == NULL)
				LOG (ERROR) << "NULL context received while loading the file \"" << file->fqp()
				<< "\"" << ".Status : " << status << ".\n";

				if(progress.size() != data.size())
				LOG (ERROR) << "Expected amount of progress is not equal to received for file \""
				<< file->fqp() << "\"" << ".Status : " << status << ".\n";

				if(!overall)
				LOG (ERROR) << "Overall task status is failure.\""
				<< file->fqp() << "\"" << ".Status : " << status << ".\n";

				boost::lock_guard<boost::mutex> lock(completion_mux);
				condition = true;
				condition_var.notify_one();
			};

	requestIdentity identity;

	auto f1 = std::bind(&CacheManager::cachePrepareData,
			CacheManager::instance(), ph::_1, ph::_2, ph::_3, ph::_4, ph::_5);

	boost::uuids::uuid uuid = boost::uuids::random_generator()();

	std::string local_client = boost::lexical_cast<std::string>(uuid);
	SessionContext ctx = static_cast<void*>(&local_client);

	status::StatusInternal status;

	FileSystemDescriptor fsDescriptor;
	fsDescriptor.dfs_type = file->origin();
	fsDescriptor.host = file->host();
	try {
		fsDescriptor.port = std::stoi(file->port());
	} catch (...) {
		return;
	}
	// execute request in async way to utilize requests pool:
	status = f1(ctx, std::cref(fsDescriptor), std::cref(data), cb,
			std::ref(identity));

	// check operation scheduling status:
	if (status != status::StatusInternal::OPERATION_ASYNC_SCHEDULED) {
		LOG (ERROR)<< "Prepare request - failed to schedule - for \"" << file->fqnp() << "\"" << ". Status : "
		<< status << ".\n";
		// no need to wait for callback to fire, operation was not scheduled
		return;
	}

	// wait when completion callback will be fired by Prepare scenario:
	boost::unique_lock<boost::mutex> lock(completion_mux);
	condition_var.wait(lock, [&] {return condition;});

	lock.unlock();

	// check callback status:
	if (cbStatus != status::StatusInternal::OK) {
		LOG (ERROR)<< "Prepare request failed for \"" << file->fqnp() << "\"" << ". Status : "
		<< cbStatus << ".\n";
		file->state(managed_file::State::FILE_IS_FORBIDDEN);
		return;
	}
	file->state(managed_file::State::FILE_HAS_CLIENTS);

}

bool FileSystemLRUCache::reload(const std::string& root){
	if(root.empty())
		return false;
	m_root = root;

	boost::filesystem::recursive_directory_iterator end_iter;

	// sort files in the root in ascending order basing on their timestamp:
	typedef std::multimap<std::time_t, boost::filesystem::path> last_access_multi;
	// iterator for sorted collection:
	typedef std::multimap<std::time_t, boost::filesystem::path>::iterator last_access_multi_it;
	last_access_multi result_set;

	// note that std::time_t accurate to a second
	if ( boost::filesystem::exists(m_root) && boost::filesystem::is_directory(m_root)){
	  for( boost::filesystem::recursive_directory_iterator dir_iter(m_root) ; dir_iter != end_iter ; ++dir_iter){
	    if (boost::filesystem::is_regular_file(dir_iter->status()) ){
	    	result_set.insert(last_access_multi::value_type(boost::filesystem::last_write_time(dir_iter->path()), *dir_iter));
	    }
	  }
	}

	// reset the underlying LRU cache.
	reset();
	last_access_multi_it it = result_set.begin();
	if(it == result_set.end()){
		// leave start time default (now)
		return true;
	}

	// reload most old timestamp:
	m_startTime = boost::posix_time::from_time_t((*it).first);

	// and populate sorted root content:
    for(; it != result_set.end(); it++){
    	std::string lp = (*it).second.string();
    	// create the managed file instance if there's network path can be successfully restored from its name
    	// so that the file can be managed by Imapla-To-Go:
    	std::string fqnp;
    	std::string relative;
        FileSystemDescriptor desciptor = managed_file::File::restoreNetworkPathFromLocal(lp, fqnp, relative);
        if(!desciptor.valid)
        	continue; // do not register this file

        managed_file::File* file;
    	// and add it into the cache
    	add(lp, file);
    	// and mark the file as "idle":
    	file->state(managed_file::State::FILE_IS_IDLE);
    }
    return true;
}

managed_file::File* FileSystemLRUCache::find(std::string path) {
    	// first find the file within the registry
    	managed_file::File* file = m_idxFileLocalPath->operator [](path);

    	if(file == nullptr)
    		return file;

    	std::unique_lock<std::mutex> lock(m_deletionsmux);
    	// check whether the requested file is under finalization maybe?
    	bool under_finalization = std::find(m_deletionList.begin(), m_deletionList.end(), path) != m_deletionList.end();

    	// if file is under finalization already or was unable to be opened, wait while it will be finalized
    	// and then reclaim it. open() should be called while collection of "deletions" is locked to prevent the dangling pointer reference
        if(under_finalization || (file->open() != status::StatusInternal::OK)){

        	// Check the active deletions list, if the file is there, do not use it and reclaim it for reload when deletion completes:
        	std::list<std::string>::iterator it;
        	m_deletionHappensCondition.wait(lock, [&] {
        		it = std::find(m_deletionList.begin(), m_deletionList.end(), path);
        		return it == m_deletionList.end();
        	}
        	);
        	lock.unlock();

        	// reclaim the file:
        	file = m_idxFileLocalPath->operator [](path);
        	if(file == nullptr)
        		return nullptr;

        	return file;
        }

    	// unlock deletions list, will work only if alive file was "opened" successfully
    	lock.unlock();

     	// if file is "forbidden but the time between sync attempts elapsed", it should be resync.
    	// prevent outer world from usage of invalid:
    	// if file state is "FORBIDDEN" (which means the file was not synchronized locally successfully on last attempt)
    	if(file->state() == managed_file::State::FILE_IS_FORBIDDEN){
    		// resync the file if the time between sync attempts elapsed:
    		if(file->shouldtryresync()){
    			sync(file);
    		}
    	}
    	return file;
    }

bool FileSystemLRUCache::add(std::string path, managed_file::File*& file){
    	bool duplicate = false;
    	bool success   = false;

    	// we create and destruct File objects only here, in LRU cache layer
    	file = new managed_file::File(path.c_str(), m_weightChangedPredicate);

    	// increase refcount to this file before being shared to outer world
    	file->open();
    	// when item is externally injected to the cache, it should have time "now"
    	success = LRUCache<managed_file::File>::add(file, duplicate);
    	if(duplicate){
    		LOG(WARNING) << "Attempt to add the duplicate to the cache, path = \"" << path << "\"\n";
    		// no need for this file, get the rid of
    		delete file;
    	}

    	return success;
}

void FileSystemLRUCache::handleCapacityChanged(long long size_delta){
	if(size_delta == 0)
		return;
	if(size_delta > 0)
		std::atomic_fetch_add_explicit(&m_currentCapacity, size_delta, std::memory_order_relaxed);
	else
		std::atomic_fetch_sub_explicit(&m_currentCapacity, size_delta, std::memory_order_relaxed);
}
}



