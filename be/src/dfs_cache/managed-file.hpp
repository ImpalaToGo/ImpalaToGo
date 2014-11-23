/*
 * @file managed-file.hpp
 * @brief represents managed by cache file.
 *
 * @date   Oct 3, 2014
 * @author elenav
 */

#ifndef MANAGED_FILE_HPP_
#define MANAGED_FILE_HPP_

#include <list>
#include <atomic>

#include <boost/intrusive/set.hpp>
#include <boost/weak_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/filesystem.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/posix_time/time_formatters.hpp>

#include "util/hash-util.h"
#include "dfs_cache/common-include.hpp"
#include "dfs_cache/utilities.hpp"

/** @namespace impala */
namespace impala{

/** @namespace ManagedFile */
namespace managed_file {

   /**
    * Defines the state of concrete physical file system file just now
    */
   enum State {
      FILE_IS_MARKED_FOR_DELETION,       /**< File is marked for deletion. This may be done by Sync module in case if:
      	  	  	  	  	  	  	  	  	 * Disk memory is low and cleanup is required. in this case, there's no
      	  	  	  	  	  	  	  	     * reason to rely on this file. It should be requested for reload from Sync module
      	  	  	  	  	  	  	  	     */

      FILE_IS_IN_USE_BY_SYNC,            /**< File is currently processed by Sync module (is being read from network).
                                         * In this case, there's the reason to rely on this file once it will be ready from
                                         * Sync module perspective. In order to say that client relies on the file,
      	  	  	  	  	  	  	  	     * add client entity to the file.
      	  	  	  	  	  	  	  	     */

      FILE_HAS_CLIENTS,                  /**< File is being processed in client(s) context(s).
      	  	  	  	  	  	  	  	  	 * This state equals to lock for Sync manager.
      	  	  	  	  	  	  	  	     * Once all clients are finished with the file, this state will be triggered to
      	  	  	  	  	  	  	  	  	 * "FILE_IS_IDLE"
      	  	  	  	  	  	  	  	  	 */

      FILE_IS_AMORPHOUS,                 /**< TODO : To discuss. We may have this state for files that are scheduled for remote load
                                         * but are not processed yet at all.
                                         * This is also default status of file when it is created in registry (for further scheduling)
      	  	  	  	  	  	  	  	  	 * but its status is not approved by nobody.
      	  	  	  	  	  	  	  	  	 */

      FILE_IS_IDLE,                      /**< File is idle. No client sessions exist for this file. It is not handled by nobody.
      	  	  	  	  	  	  	  	  	 * This is the only state when file may be deleted from the cache.
      	  	  	  	  	  	  	  	  	 */
      FILE_IS_FORBIDDEN                 /**< File is forbidden, do not use it */
   };

   /**
    * Represents managed file.
    * - keeps state;
    * - keeps list of opened handles to this file to be sure we have no handles leak if somebody forgot to call the close()
    *   on the file handle when complete with a file.
    * - keeps unique name (hash key)
    */
   class File {
   private:
	   volatile State   m_state;                   /**< current file state */
	   std::string      m_fqp;                     /**< fully qualified path (local) */
	   std::string      m_fqnp;                    /**< fully qualified path (network) */
	   boost::uintmax_t m_size;                    /**< file size. For internal and user statistics and memory planning. */
	   std::size_t      m_estimatedsize;           /**< estimated file size. For files that are being loaded right now. */

       std::string        m_originhost;       /**< origin host */
       std::string        m_originport;       /**< origin port */
       DFS_TYPE           m_schema;           /**< origin schema */

       volatile std::atomic<unsigned> m_users;        /**< number of users so far */

	   static std::string              fileSeparator;  /**< platform-specific file separator */

	   static std::vector<std::string> m_supportedFs;  /**< list of supported file systems, string representation */

   public:

       static void initialize();

	   /** Search predicate to find the handle by its shared pointer */
	   struct FileHandleEqPredicate
	   {
		   private:
		   	   const dfsFile & m_item;

		   public:
		   	   FileHandleEqPredicate(const dfsFile & item) : m_item(item) {	}

		   	   bool operator () (const dfsFile & item) const{
		   		   return item == m_item;
		   	   }
	   };

	   /** When created, file is "not approved".
        * it became approved once all its metadata is validated
        *
        * @param path       - full file local path
	    */
	   File(const char* path)
         :  m_state(State::FILE_IS_AMORPHOUS), m_fqp(path), m_size(0), m_estimatedsize(0),
            m_schema(DFS_TYPE::NON_SPECIFIED){

		   FileSystemDescriptor descriptor = restoreNetworkPathFromLocal(std::string(path), m_fqnp);
           if(!descriptor.valid){
        	   m_state = State::FILE_IS_FORBIDDEN;
        	   return;
           }

           m_schema = descriptor.dfs_type;

           m_originhost = descriptor.host;
           m_originport = std::to_string(descriptor.port);

           m_users.store(0);
	   }

	   ~File(){
	   }

	   /** restore File options representing the network identification of supplied file.
	    *  @param [in]     local  - fqp of file.
	    *  @param [in/out] fqnp   - resolved fqnp string, in case of failure - empty string
	    *
	    *  @return fqdn if it was restored or empty string otherwise
	    */
	   static FileSystemDescriptor restoreNetworkPathFromLocal(const std::string& local, std::string& fqnp);

	   static std::string constructLocalPath(const FileSystemDescriptor& fsDescriptor, const char* path);

	   /** ********************* File object getters and setters **********************************************/
	   /** getter for File state */
	   inline State state() { return m_state; }

	   /** flag, idicates that the file is in valid state and can be used */
	   inline bool valid() {
		   return (m_state == State::FILE_HAS_CLIENTS || m_state == State::FILE_IS_IDLE);
	   }

	   /** flag, indicates whether the file was resolved by registry */
	   inline bool exist() {
		   return !(m_state == State::FILE_IS_AMORPHOUS || m_state == State::FILE_IS_FORBIDDEN || m_state == State::FILE_IS_MARKED_FOR_DELETION);
	   }

	   /** setter for file state
	    * @param state - file state to mark the file with
	    */
	   inline void state(State state) {
		   m_state = state;
	   }


	   /** reply origin file system host */
	   inline std::string host() { return m_originhost; }

	   /** reply origin file system port */
	   inline std::string port() { return m_originport; }

	   /** reply origin file system type */
	   inline DFS_TYPE origin() { return m_schema; }

	   /** getter for File fully qualified path */
	   inline const std::string fqp() const  { return m_fqp; }

	   /** setter for file fully qualified path
	    * @param fqp - file fully qualified path
	    */
	   inline void fqp(std::string fqp) {
		   m_fqp = fqp;
		   // TODO : reconstruct origin host and port
	   }

	   /** getter for File network path. When the file is reconstructed from existing local cache,
	    * this pat his assigned in the following way:
	    * dfs_type:/dfs_namenamenode_address/file_path_within_that_dfs
	    * */
	   inline const std::string fqnp() const { return m_fqnp; }

	   /** setter for file network path.
	    * @param fqnp - file network path (constructed to have an ability to locate this file on remote dfs)
	    */
	   inline void fqnp(std::string fqnp) { m_fqnp = fqnp; }


	   /** getter for File size (available locally) */
	   inline boost::uintmax_t size() {
		   boost::system::error_code ec;
		   boost::uintmax_t size = boost::filesystem::file_size(m_fqp, ec);
		   // check ec, should be 0 in case of success:
		   if(!ec)
			   return size;
		   return 0;
	   }

	   /** getter for File estimated size (for file which is not yet locally).
	    *  This size is only meaningful for files that are in progress of loading from remote dfs into cache.
	    */
	   inline std::size_t estimated_size() { return m_estimatedsize; }

	   /** setter for file estimated size (for file which is not yet locally).
	    * The scenario which scheduled the file for load should fill this field for estimation scenario calculations
	    * to be possible.
	    * @param size - estimated file size
	    */
	   inline void estimated_size(std::size_t size) { m_estimatedsize = size; }


	   /** getter for File last access (local)
	    *
	    * @return If there was an error during last access retrieval, the default time will be returned.
	    *         Otherwise, last access time will be returned
	    */
	   inline boost::posix_time::ptime last_access() {
		   boost::system::error_code ec;
		   std::time_t last_access_time = boost::filesystem::last_write_time(m_fqp, ec);
		   // check ec, should be 0 in case of success:
		   if(!ec)
			   return boost::posix_time::from_time_t(last_access_time);
		   return boost::posix_time::time_from_string("1970-01-01 00:00:00.000");
	   }

	   /** update file last_write time.
	    * @param time - timstamp to assign to be a last access time for file
	    *
	    * @return 0          - if operation succeeded
	    *         error code - in case of failure
	    */
	   inline int last_access(const boost::posix_time::ptime& time){
		   boost::system::error_code ec;
		   boost::filesystem::last_write_time(m_fqp, utilities::posix_time_to_time_t(time), ec);
		   return ec.value();
	    }

       /* Force delete file ignoring its usage statistic
        */
      status::StatusInternal forceDelete();

      /**
       * Add new opened handle to the list of handles.
       *
       * @return Operation status
       */
      status::StatusInternal open();

      /**
       * Explicitly remove the reference to a handle from the list of handles
       *
       * @return Operation status
       */
      status::StatusInternal close();

      /**
       * Drop the file from file system
       */
      void drop();

	   /* ***********************   Methods group to fit the intrusive concept (LRU Cache)   ******************************/

	   friend bool operator <  (const File &a, const File &b)
	   	   	   {  return a.fqp() < b.fqp();  }


	   friend bool operator == (const File &a, const File &b)
			   {  return a.fqp() == b.fqp();  }

	   friend std::size_t hash_value(const File &object) {
		   uint32_t expected_hash = 0;
		   return expected_hash = HashUtil::Hash(object.fqp().c_str(),
				   strlen(object.fqp().c_str()),
				   expected_hash);
		   return expected_hash;
        }
      /* *******************************************************************************************************/
   };
}  /** namespace managed_file */
}  /** namespace impala */


#endif /* MANAGED_FILE_HPP_ */
