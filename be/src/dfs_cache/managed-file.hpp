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

#include <boost/weak_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/filesystem.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/posix_time/time_formatters.hpp>

#include "util/hash-util.h"
#include "dfs_cache/common-include.hpp"
#include "dfs_cache/utilities.hpp"
#include "dfs_cache/filesystem-descriptor-bound.hpp"

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

      FILE_SYNC_JUST_HAPPEN,             /**< Status indicates that file sync just happen. As sync is triggered by client request,
       	   	   	   	   	   	   	   	   	 * the client waits for sync result, thus, the file need to be available for awaiting client
       	   	   	   	   	   	   	   	   	 * till the moment the client will "open" it. Such file couldn't be deleted by cleanup mechanism
       	   	   	   	   	   	   	   	   	 * despite it does not have attached clients yet.
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
      FILE_IS_FORBIDDEN,                 /**< File is forbidden, do not use it */

      FILE_IS_UNDER_WRITE,               /**< File is being written by some scenario */
   };

   /** explain the managed file origination nature */
   enum NatureFlag{
	   AMORPHOUS,            /** file is only metadata yet and is not backed wit ha physical fine */
	   FOR_WRITE,            /** file is being created may change its size in the future as it is opened for write */
	   PHYSICAL,             /** file is backed by physical and thus its size is known in advance */
	   NON_SPECIFIED
   };
   /**
    * stringify the File Status
    */
   // extern std::ostream& operator<<(std::ostream& out, const managed_file::State value);

   /**
    * Represents managed file.
    * - keeps state;
    * - keeps list of opened handles to this file to be sure we have no handles leak if somebody forgot to call the close()
    *   on the file handle when complete with a file.
    * - keeps unique name (hash key)
    */
   class File {
   public:

		/** callback to be invoked on LRU from its item to update about the item weight */
		using WeightChangedEvent = typename boost::function<void(long long delta)>;
		/** callback to get the remote file info */
		using GetFileInfo = typename boost::function<dfsFileInfo*(const char* path, const FileSystemDescriptor& descriptor)>;
		/** callback to free remote file info */
        using FreeFileInfo = typename boost::function<void(dfsFileInfo*, int)>;

 	   static std::string              fileSeparator;  /**< platform-specific file separator */

   private:
	   std::atomic<State> m_state;                   /**< current file state */
	   std::atomic<int>   m_subscribers;             /**< number of subscribers of this file (who may wait for this file to be downloaded */

	   std::string        m_fqp;                     /**< fully qualified path (local) */
	   std::string        m_fqnp;                    /**< fully qualified path (network) */
	   boost::uintmax_t   m_remotesize;              /**< remote file size. For internal and user statistics and memory planning. */
	   std::size_t        m_estimatedsize;           /**< estimated file size. For files that are being loaded right now. */
	   NatureFlag         m_filenature;              /**< file nature, the initial condition of creation */

	   std::size_t        m_prevsize;                /**< always contains the "previous size", initially 0 */

	   std::string        m_filename;         /**< relative file name. Within the scope where it is accessed now (remotely, locally) */
       std::string        m_originhost;       /**< origin host */
       std::string        m_originport;       /**< origin port */
       DFS_TYPE           m_schema;           /**< origin schema */

       static int         _defaultTimeSliceInSeconds;  /**< default time slice between unsuccessful attempts to sync the file.
                                                       * this means that attempt to sync the file may be performed once per 20 seconds
                                                       */

       boost::posix_time::time_duration m_duration_next_attempt_to_sync; /**< min duration between attempts to sync forbidden file */
       boost::posix_time::ptime m_lastsyncattempt;    	/**< last attempt to synchronize the file locally. Is relevant for file
        												* only if it is in FORBIDDEN state */

       volatile std::atomic<int>       m_users;        /**< number of users so far */

	   static std::vector<std::string> m_supportedFs;  /**< list of supported file systems, string representation */

	   // synchronization section for possible file state changed event awaiters:
	   boost::condition_variable m_state_changed_condition;   /**< condition variable for those who waits for file state changed */
	   boost::mutex m_state_changed_mux;                      /**< protector for "file state changed" condition */

	   boost::mutex m_closure_mux;                            /**< protector of final object detach from clients. Is required in order
	    													  * to guard the close() remainder right after last client detaches
	    													  **/

	   WeightChangedEvent m_weightIsChangedcallback;          /**< "weight is changed" event callback */
	   GetFileInfo     m_getFielInfoCb;                       /**< "get file info" callback */
	   FreeFileInfo    m_freeFileInfoCb;                      /**< "free file info" callback */

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
	   File(const char* path, NatureFlag creationFlag,  GetFileInfo getinfo = 0, FreeFileInfo freeinfo = 0)
         :  m_fqp(path), m_remotesize(0), m_estimatedsize(0), m_filenature(creationFlag), m_prevsize(0),
            m_schema(DFS_TYPE::NON_SPECIFIED), m_weightIsChangedcallback(0), m_getFielInfoCb(getinfo), m_freeFileInfoCb(freeinfo){

		   LOG (INFO) << "Creating new managed file on top of \"" << path << "\".\n";

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
           // specify that the attempt to resync the file from remote side can be performed once per 20 seconds
           m_duration_next_attempt_to_sync = boost::posix_time::seconds(_defaultTimeSliceInSeconds);

		   // check creation flag. If this is amorphous file, need to ask its remote size to plan this file:
           if(creationFlag == NatureFlag::AMORPHOUS && m_getFielInfoCb && m_freeFileInfoCb){
        	   LOG (INFO) << "File name \"" << m_fqnp << "\"\n";
        	   dfsFileInfo* info = m_getFielInfoCb(m_filename.c_str(), descriptor);
        	   if(info == NULL){
        		   LOG (ERROR) << "Unable to create new file from path \"" << path <<
        				   "\". Unable to retrieve remote file info.\n";
        		   m_state = State::FILE_IS_FORBIDDEN;
        		   return;
        	   }

        	   m_remotesize = info->mSize;
        	   m_freeFileInfoCb(info, 1);
           }
	   }

	   /**
	    * Construct the managed file object basing on path.
	    * Assign the "weight is changed" callback to be fired when the file detects its size is changed
	    * (local size)
	    */
	   File(const char* path, const WeightChangedEvent& eve, NatureFlag creationFlag, GetFileInfo getinfo = 0, FreeFileInfo freeinfo = 0) :
		   File(path, creationFlag, getinfo, freeinfo){
		   m_weightIsChangedcallback = eve;
	   }

	   ~File(){
		   LOG(INFO) << "Going to destruct the file \"" << fqp() << "\".\n";
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

	   /** flag, indicates that the file is in valid state and can be used */
	   inline bool exists() {
		   return ((m_state == State::FILE_HAS_CLIENTS) || (m_state == State::FILE_IS_IDLE) || (m_state == State::FILE_SYNC_JUST_HAPPEN)) &&
				   ((m_filenature == NatureFlag::PHYSICAL) || (m_filenature == NatureFlag::FOR_WRITE));
	   }

	   /** flag, indicates whether the file was resolved by registry */
	   inline bool valid() {
		   return !(m_state == State::FILE_IS_FORBIDDEN || m_state == State::FILE_IS_MARKED_FOR_DELETION);
	   }

	   inline bool shouldtryresync(){
		   boost::posix_time::ptime now = boost::posix_time::microsec_clock::local_time();
		   return ( (now - m_lastsyncattempt) > m_duration_next_attempt_to_sync );
	   }

	   /** change the file nature */
	   inline void nature(NatureFlag nature = NatureFlag::PHYSICAL){ m_filenature = nature; }

	   /** getter for file nature */
	   inline NatureFlag getnature() { return m_filenature; }

	   /**
	    * Try mark the file for deletion. Only few file states permit this operation to happen.
	    *
        * @return true if file was marked for deletion
        * No one should reference this file since it is marked for deletion
	    */
	   inline bool mark_for_deletion(){
		   boost::mutex::scoped_lock lock(m_state_changed_mux);

		   bool marked = false;
		   // lock the closure mux
		   boost::mutex::scoped_lock c_lock(m_closure_mux);
		   LOG (INFO) << "Managed file OTO \"" << fqp() << "\" with state \"" << state() << "\" is requested for deletion." <<
				   "subscribers # = " <<  m_subscribers.load(std::memory_order_acquire) << "\n";
		   // check all states that allow to mark the file for deletion:
		   State expected = State::FILE_IS_IDLE;

		   // compare "idle marker" under the closure mux
           marked = m_state.compare_exchange_strong(expected, State::FILE_IS_MARKED_FOR_DELETION);
           c_lock.unlock();

           if(marked){
        	   m_state_changed_condition.notify_all();
        	   LOG (INFO) << "Managed file OTO \"" << fqp() << "\" with state \"" << state() <<
        			   "\" is successfully marked for deletion." << "\n";
        	   if(m_subscribers.load(std::memory_order_acquire) == 0)
        		   return true;
        	   return false;
           }

           expected = State::FILE_IS_FORBIDDEN;
           marked   =  m_state.compare_exchange_strong(expected, State::FILE_IS_MARKED_FOR_DELETION);

           if(marked){
        	   m_state_changed_condition.notify_all();
        	   LOG (INFO) << "Managed file OTO \"" << fqp() << "\" with state \"" << state() <<
        			   "\" is successfully marked for deletion." << "\n";
        	   if(m_subscribers.load(std::memory_order_acquire) == 0)
        		   return true;
        	   return false;
           }

           expected = State::FILE_IS_AMORPHOUS;
           marked   =  m_state.compare_exchange_strong(expected, State::FILE_IS_MARKED_FOR_DELETION);

           m_state_changed_condition.notify_all();
           marked = (marked && (m_subscribers.load(std::memory_order_acquire) == 0));
           std::string marked_str = marked ? "successfully" : "NOT";
    	   LOG (INFO) << "Managed file OTO \"" << fqp() << "\" with state \"" << state() <<
    			   "\" is " << marked_str  << " marked for deletion." << "\n";

    	   return marked;
	   }

	   /** setter for file state
	    * @param state - file state to mark the file with
	    */
	   inline void state(State state) {
		   // do not change file state when it is marked for deletion:
		   if(m_state.load(std::memory_order_acquire) == State::FILE_IS_MARKED_FOR_DELETION)
			   return;

		   if(state == State::FILE_IS_IN_USE_BY_SYNC)
			   m_lastsyncattempt = boost::posix_time::microsec_clock::local_time();

		   // fire the condition variable for whoever waits for file status to be changed:
		   m_state.exchange(state, std::memory_order_release);
		   boost::mutex::scoped_lock lock(m_state_changed_mux);
		   m_state_changed_condition.notify_all();
	   }

	   /** Provides the subscription mechanism for file state changed for outside
	    *
	    * @param [out] condition_var - condition variable to signal that the state is changed
	    * @param [out] mux           - mutex to protect the condition subject (the status)
	    *
	    * @return if subscription is valid (if file is marked for deletion, the subscription is not valid)
	    */
	   inline bool subscribe_for_updates(boost::condition_variable*& condition_var, boost::mutex*& mux){
		   if(m_state.load(std::memory_order_acquire) == State::FILE_IS_MARKED_FOR_DELETION)
		   			   return false;

		   condition_var = &m_state_changed_condition;
		   mux           = &m_state_changed_mux;
		   m_subscribers++;
		   return true;
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
		   // for amorphous file, reply the remote size instead of local as the file does not exist locally:
		   if(m_filenature == NatureFlag::AMORPHOUS)
			   return m_remotesize;

		   if(m_filenature == NatureFlag::FOR_WRITE)
			   return m_estimatedsize;

		   // for physical file, reply its physical size from local FS:
		   boost::system::error_code ec;
		   boost::uintmax_t size = boost::filesystem::file_size(m_fqp, ec);
		   // check ec, should be 0 in case of success:
		   if(!ec)
			   return size;
		   return 0;
	   }

	   /** getter for remote file - the origin of managed file - size */
	   inline tOffset remote_size(){ return m_remotesize; }

	   /** getter for File estimated size (for file which is not yet locally).
	    *  This size is only meaningful for files that are in progress of loading from remote dfs into cache.
	    */
	   inline std::size_t estimated_size() { return m_estimatedsize; }

	   /** setter for file estimated size (for file which is not yet locally).
	    * The scenario which scheduled the file for load should fill this field for estimation scenario calculations
	    * to be possible.
	    * @param size - estimated file size
	    */
	   inline void estimated_size(std::size_t size) {
		   long long delta = size - m_prevsize;
		   // if any subscribers for size change, send the signal with a delta.
		   // In current design this is only relevant for files opened for write (as we cannot predict their final size):
		   if(m_weightIsChangedcallback && m_filenature == NatureFlag::FOR_WRITE)
			   m_weightIsChangedcallback(delta);
		   m_prevsize = size;
		   m_estimatedsize = size;
	   }


	   /** getter for File last access (local).
	    *
	    * @return If there was an error during last access retrieval, the default time will be returned.
	    * Otherwise, last access time will be returned
	    */
	   inline boost::posix_time::ptime last_access() {
		   boost::system::error_code ec;
		   std::time_t last_access_time = boost::filesystem::last_write_time(m_fqp, ec);
		   // check ec, should be 0 in case of success:
		   if(!ec)
			   return boost::posix_time::from_time_t(last_access_time);
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
       * Mark the file with one more usage
       *
       * @return Operation status
       */
      status::StatusInternal open(int ref_count = 1);

      /**
       * Unbind 1 or more usages of the file
       *
       * @return Operation status
       */
      status::StatusInternal close(int ref_count = 1);

      /**
       * Drop the file from file system
       *
       * @return operation status, true if file was removed
       */
      bool drop();

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
