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
#include <boost/intrusive/set.hpp>
#include <boost/weak_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include "dfs_cache/common-include.hpp"

/** @namespace impala */
namespace impala{

/**
 * Base hook for boost intrusive set
 */
typedef boost::intrusive::set_base_hook<> SetBaseHook;

/** @namespace ManagedFile */
namespace ManagedFile {
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
   };

   /**
    * Represents managed file.
    * - keeps state;
    * - keeps list of opened handles to this file to be sure we have no handles leak if somebody forgot to call the close()
    *   on the file handle when complete with a file.
    * - keeps unique name (hash key)
    */
   class File
   // Base hook with default tag, raw pointers and safe_link mode
   :  public SetBaseHook
   {
   private:
	   State       m_state;                   /**< current file state */
	   std::string m_fqp;                     /**< fully qualified path (local) */
	   std::string m_fqnp;                    /**< fully qualified path (network) */
	   std::size_t m_size;                    /**< file size. For internal and user statistics and memory planning. */
	   std::size_t m_estimatedsize;           /**< estimated file size. For files that are being loaded right now. */

	   boost::posix_time::ptime m_lastaccess; /**< last file access. For cleanup planning. */

	   std::list<dfsFile> m_handles;          /**< list of opened handles to this file. */

	   boost::mutex m_mux;   /**< locking mechanism for collection of opened file handles */

   public:

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

	   /** When created, file is "not approved". Sync module should run through all directories / files under configured root,
	    * populate the registry with all files metadata and then approve each file in the registry
	    */
	   File(const char *local_path, const char* network_path)
         :  m_state(State::FILE_IS_AMORPHOUS), m_fqp(local_path), m_fqnp(network_path), m_size(0), m_estimatedsize(0) {}

	   ~File(){
		   boost::mutex::scoped_lock lock(m_mux);
		   m_handles.clear();
	   }
	   /** ********************* File object getters and setters **********************************************/
	   /** getter for File state */
	   inline State state() { return m_state; }

	   /** setter for file state
	    * @param state - file state to mark the file with
	    */
	   inline void state(State state) { m_state = state; }


	   /** getter for File fully qualified path */
	   inline const std::string fqp() const  { return m_fqp; }

	   /** setter for file fully qualified path
	    * @param fqp - file fully qualified path
	    */
	   inline void fqp(std::string fqp) { m_fqp = fqp; }

	   /** getter for File network path. When the file is reconstructed from existing local cache,
	    * this pat his assigned in the following way:
	    * dfs_type:/dfs_namenamenode_address/file_path_within_that_dfs
	    * */
	   inline const std::string fqpn() const { return m_fqnp; }

	   /** setter for file network path.
	    * @param fqnp - file network path (constructed to have an ability to locate this file on remote dfs)
	    */
	   inline void fqnp(std::string fqnp) { m_fqnp = fqnp; }


	   /** getter for File size (available locally) */
	   inline std::size_t size() { return m_size; }

	   /** setter for file size (available locally)
	    * @param size - file size
	    */
	   inline void size(std::size_t size) { m_size = size; }


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
	    *  This size is only meaningful for files that are in progress of loading from remote dfs into cache.
	    */
	   inline boost::posix_time::ptime last_access() { return m_lastaccess; }

	   /** setter for file last access (local)
	    * The scenario which  performs cache cleanup should take this field into account
	    * when select the candidate for deletion.
	    *
	    * @param last_access - last local file access timestamp
	    */
	   inline void last_access(boost::posix_time::ptime last_access) { m_lastaccess = last_access; }

       /* Force delete file. Force deletion is possible only in case if not opened handles to the file
        * exists. This may happen if Sync module detects that the file is deleted from the cache abnormally
        * (in non-managed way, for example, manually)
        */
      status::StatusInternal forceDelete(){ return status::NOT_IMPLEMENTED; }

      /**
       * Add new opened handle to the list of handles.
       *
       * @param handle - opened file handle
       *
       * @return Operation status
       */
      status::StatusInternal open(const dfsFile & handle);

      /**
       * Explicitly remove the reference to a handle from the list of handles
       *
       * @param handle - handle to file, one of opened
       *
       * @return Operation status
       */
      status::StatusInternal close(const dfsFile & handle);

	   /* ***********************   Methods group to fit the intrusive concept   ******************************/

	   friend bool operator <  (const File &a, const File &b)
	   	   	   {  return a.fqp() < b.fqp();  }

	   friend bool operator == (const File &a, const File &b)
			   {  return a.fqp() == b.fqp();  }

	   friend std::size_t hash_value(const File &object) {
		   uint32_t expected_hash = 0;
		   /*
		        	  return expected_hash = HashUtil::Hash(object.get_path().c_str(),
   			  	  	  	  	  	  	  	     strlen(object.get_path().c_str()),
   			  	  	  	  	  	  	  	     expected_hash);
   			  	  	  	  	  	  	  	     */
   	  return expected_hash;
        }
      /* *******************************************************************************************************/
   };
}  /** namespace ManagedFile */
}  /** namespace impala */


#endif /* MANAGED_FILE_HPP_ */
