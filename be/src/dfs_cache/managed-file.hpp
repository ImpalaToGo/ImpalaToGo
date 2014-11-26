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
      FILE_IS_MARKED_FOR_DELETION,       /**< File is marked for deletion. This may be done by LRU cache in case if:
      	  	  	  	  	  	  	  	  	 * Disk memory is low and cleanup is required. in this case, there's no
      	  	  	  	  	  	  	  	     * reason to rely on this file. It should be requested for reload from LRU cache module
      	  	  	  	  	  	  	  	     * if this status is observed.
      	  	  	  	  	  	  	  	     */

      FILE_IS_IN_USE_BY_SYNC,            /**< File is currently processed by Sync module (is being read from network).
                                         * In this case, there's the reason to rely on this file before it will be ready from
                                         * Sync module perspective. In order to say that client relies on the file transition from
                                         * this status to whatever next status, we count "file state changed" event subscribers.
      	  	  	  	  	  	  	  	     */

      FILE_HAS_CLIENTS,                  /**< File is being processed in client(s) context(s).
      	  	  	  	  	  	  	  	  	 * This state equals to lock for Sync manager.
      	  	  	  	  	  	  	  	     * Once all clients are finished with the file, this state will be triggered to
      	  	  	  	  	  	  	  	  	 * "FILE_IS_IDLE"
      	  	  	  	  	  	  	  	  	 */

      FILE_IS_AMORPHOUS,                 /**< Default status of file when it is created in registry but its status is not
       	   	   	   	   	   	   	   	   	 * approved by nobody.
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
	   std::atomic<State> m_state;                   /**< current file state */
	   std::atomic<int>   m_subscribers;             /**< number of subscribers of this file (who may wait for this file to be downloaded */

	   std::string        m_fqp;                     /**< fully qualified path (local) */
	   std::string        m_fqnp;                    /**< fully qualified path (network) */
	   boost::uintmax_t   m_size;                    /**< file size. For internal and user statistics and memory planning. */
	   std::size_t        m_estimatedsize;           /**< estimated file size. For files that are being loaded right now. */

	   std::string        m_filename;         /**< relative file name. Within the scope where it is accessed now (remotely, locally) */
       std::string        m_originhost;       /**< origin host */
       std::string        m_originport;       /**< origin port */
       DFS_TYPE           m_schema;           /**< origin schema */

       static int         _defaultTimeSliceInMinutes;  /**< default time slice between unsuccessful attempts to sync the file.
                                                           * this means that attempt to sync the file may be performed once in 6 minutes
                                                           */

       boost::posix_time::time_duration m_duration_next_attempt_to_sync; /**< min duration between attempts to sync forbidden file */
       boost::posix_time::ptime m_lastsyncattempt;    	/**< last attempt to synchronize the file locally. Is relevant for file
        												* only if it is in FORBIDDEN state */

       volatile std::atomic<unsigned> m_users;        /**< number of users so far */

	   static std::string              fileSeparator;  /**< platform-specific file separator */

	   static std::vector<std::string> m_supportedFs;  /**< list of supported file systems, string representation */

	   // synchronization section for possible file state changed event awaiters:
	   boost::condition_variable m_state_changed_condition;   /**< condition variable for those who waits for file state changed */
	   boost::mutex m_state_changed_mux;                      /**< protector for "file state changed" condition */

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
         :  m_fqp(path), m_size(0), m_estimatedsize(0),
            m_schema(DFS_TYPE::NON_SPECIFIED){

		   m_state.store(State::FILE_IS_AMORPHOUS, std::memory_order_release);

		   FileSystemDescriptor descriptor = restoreNetworkPathFromLocal(std::string(path), m_fqnp, m_filename);
           if(!descriptor.valid){
        	   m_state = State::FILE_IS_FORBIDDEN;
        	   return;
           }

           m_schema = descriptor.dfs_type;

           m_originhost = descriptor.host;
           m_originport = std::to_string(descriptor.port);

           m_users.store(0);
           m_subscribers.store(0);
           // specify that the attempt to resync the file from remote side can be performed once at 5 minutes
           m_duration_next_attempt_to_sync = boost::posix_time::minutes(_defaultTimeSliceInMinutes);
	   }

	   ~File(){
	   }

	   /** restore File options representing the network identification of supplied file.
	    *  @param [in]     local    - fqp of file.
	    *  @param [in/out] fqnp     - resolved fqnp string, in case of failure - empty string
	    *  @param [in/out] relative - relative filename, within any origin
	    *
	    *  @return fqdn if it was restored or empty string otherwise;
	    *  filename if it was restored or empty otherwise
	    */
	   static FileSystemDescriptor restoreNetworkPathFromLocal(const std::string& local,
			   std::string& fqnp, std::string& relative);

	   static std::string constructLocalPath(const FileSystemDescriptor& fsDescriptor, const char* path);

	   /** ********************* File object getters and setters **********************************************/
	   /** getter for File state */
	   inline State state() { return m_state.load(std::memory_order_acquire); }

	   /** flag, idicates that the file is in valid state and can be used */
	   inline bool exists() {
		   return (m_state == State::FILE_HAS_CLIENTS || m_state == State::FILE_IS_IDLE);
	   }

	   /** flag, indicates whether the file was resolved by registry */
	   inline bool valid() {
		   return !(m_state == State::FILE_IS_FORBIDDEN || m_state == State::FILE_IS_MARKED_FOR_DELETION);
	   }

	   inline bool shouldtryresync(){
		   boost::posix_time::ptime now = boost::posix_time::microsec_clock::local_time();
		   return ( (now - m_lastsyncattempt) > m_duration_next_attempt_to_sync );
	   }

	   /**
	    * flag, indicates that there's no references to the file are in use.
	    * Should be checked before to delete the file
	    */
	   inline bool nonreferenced(){
		   return !(m_state.load(std::memory_order_acquire) == State::FILE_HAS_CLIENTS ||
				   m_subscribers.load(std::memory_order_acquire) != 0 ||
				   m_state.load(std::memory_order_acquire) == State::FILE_IS_IN_USE_BY_SYNC);
	   }
	   /** setter for file state
	    * @param state - file state to mark the file with
	    */
	   inline void state(State state) {
		   if(state == State::FILE_IS_IN_USE_BY_SYNC)
			   m_lastsyncattempt = boost::posix_time::microsec_clock::local_time();
		   // fire the condition variable for whoever waits for file status to be changed:
		   m_state.store(state, std::memory_order_release);
		   boost::mutex::scoped_lock lock(m_state_changed_mux);
		   m_state_changed_condition.notify_all();
	   }

	   /** Provides the subscription mechanism for file state changed for outside
	    *
	    * @param [out] condition_var - condition variable to signal that the state is changed
	    * @param [out] mux           - mutex to protect the condition subject (the status)
	    */
	   inline void subscribe_for_updates(boost::condition_variable*& condition_var, boost::mutex*& mux){
		   condition_var = &m_state_changed_condition;
		   mux           = &m_state_changed_mux;
		   m_subscribers++;
	   }

	   /**
	    * unsubscribe from file status updates, say, no usage more is needed.
	    */
       inline void unsubscribe_from_updates(){
    	   m_subscribers--;
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

	   /**
	    * getter for relative file name within origin (remote, local)
	    * @return relative file name if assigned, empty string means the file is invalid
	    */
	   inline std::string relative_name() { return m_filename; }

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


	   /** getter for File last access (local).
	    *
	    * @return If there was an error during last access retrieval, the default time will be returned.
	    *         Otherwise, last access time will be returned
	    */
	   inline boost::posix_time::ptime last_access() {
		   // if last access is requested while the item is amorphous (to-be-restored-from file-system),
		   // reply the real timestamp from file system.
		   if(state() == State::FILE_IS_AMORPHOUS){
			   boost::system::error_code ec;
			   std::time_t last_access_time = boost::filesystem::last_write_time(m_fqp, ec);
			   // check ec, should be 0 in case of success:
			   if(!ec)
				   return boost::posix_time::from_time_t(last_access_time);
			   return boost::posix_time::time_from_string("1970-01-01 00:00:00.000");
		   }
		   // if file last access time is requested on the constructed file, reply "now" for timestamp
		   return boost::posix_time::microsec_clock::local_time();
	   }

	   /** update file last_write time.
	    * @param time - timstamp to assign to be a last access time for file
	    *
	    * @return 0          - if operation succeeded
	    *         error code - in case of failure
	    */
	   inline int last_access(const boost::posix_time::ptime& time){
		   // do nothing is the file is marked as forbidden:
		   if(m_state.load(std::memory_order_acquire) == State::FILE_IS_FORBIDDEN)
			   return -1;

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
