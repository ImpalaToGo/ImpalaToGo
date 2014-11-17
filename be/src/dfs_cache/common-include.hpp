/*
 * @file cached-file.hpp
 *
 *  on: Oct 1, 2014
 *      Author: elenav
 */

#ifndef COMMON_INCLUDE_HPP_
#define COMMON_INCLUDE_HPP_

#include <time.h>
#include <list>
#include <map>
#include <utility>

#include "common/multi-precision.h"
#include <boost/thread/mutex.hpp>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>

#include <boost/preprocessor/punctuation/comma.hpp>
#include <boost/preprocessor/control/iif.hpp>
#include <boost/preprocessor/comparison/equal.hpp>
#include <boost/preprocessor/stringize.hpp>
#include <boost/preprocessor/seq/for_each.hpp>
#include <boost/preprocessor/seq/size.hpp>
#include <boost/preprocessor/seq/seq.hpp>

#include "common/logging.h"
#include "dfs_cache/hadoop-fs-definitions.h"

namespace impala {

namespace constants
{
    /** default location for cache */
    extern const std::string DEFAULT_CACHE_ROOT;

    /** default filesystem configuration is requested.
     * See core-site.xml
     *
     * <property>
 	 *	  <name>fs.defaultFS</name>
 	 *	  <value>file:///</value>
	 * </property>
    */
    extern const std::string DEFAULT_FS;
}

/**
 * Represents context of File-related operation session.
 * Context should provide at least the callback and the underlying session entity.
 */
typedef void* SessionContext;

/** Define request identity */
typedef struct {
	SessionContext ctx;          /**< client session context (shell session) */
	std::string    timestamp;    /**< client request timestamp */
} requestIdentity;

namespace status {
/**
 * Internal operation status
 */
typedef enum {
	OK,
	OPERATION_ASYNC_SCHEDULED,
	OPERATION_ASYNC_REJECTED,
	FINALIZATION_IN_PROGRESS,

	REQUEST_IS_NOT_FOUND,           /**< request is not found */
	REQUEST_FAILED,

	NAMENODE_IS_NOT_CONFIGURED,
	NAMENODE_IS_UNREACHABLE,
	NAMENODE_CONNECTION_FAILED,

	DFS_ADAPTOR_IS_NOT_CONFIGURED,
	DFS_OBJECT_DOES_NOT_EXIST,
	DFS_NAMENODE_IS_NOT_REACHABLE,  /**< requested namenode is not reachable */

	FILE_OBJECT_OPERATION_FAILURE,

	NOT_IMPLEMENTED,                 /**< for developer purposes */
} StatusInternal;
}

/**
 * Any task overall status
 */
enum taskOverallStatus{
            NOT_RUN = 0,
            PENDING,
            IN_PROGRESS,
            COMPLETED_OK,
            FAILURE,
            CANCELATION_SENT,
            CANCELED_CONFIRMED,    /**< task cancellation was performed successfully */
            INTERRUPTED_EXTERNAL,  /**< task execution was interrupted because of external reason */
            NOT_FOUND,             /**< task not found */
            IS_NOT_MANAGED      /**< task is not managed */
};

/** Formatters for enumerations */
extern std::ostream& operator<<(std::ostream& out, const taskOverallStatus value);
extern std::ostream& operator<<(std::ostream& out, const status::StatusInternal value);
extern std::ostream& operator<<(std::ostream& out, const DFS_TYPE value);

/**
 * Connection details as configured
 */
struct FileSystemDescriptor{
	DFS_TYPE      dfs_type;
	std::string   host;
	int           port;
	std::string   credentials;
	std::string   password;

	bool          valid;      /** this flag is introduced in order to overcome the non-nullable struct nature.
	                           *  an object with "valid" = false should be treated as non-usable (nullptr, NULL)
	                           */
	static FileSystemDescriptor getNull() {
		FileSystemDescriptor descriptor;
		descriptor.valid = false;
		return descriptor;
	}
};

/** Impala client code is aware of remote FileSystem mapping only */
typedef FileSystemDescriptor     dfsFS;

/** Represent data set in terms of data string descriptors */
typedef std::list<const char*> DataSet;

/**
 * Represent the single DFS connection
 */
typedef struct {
	typedef enum{
		NON_INITIALIZED,
		FREE_INITIALIZED,
		FREE_FAILURE,
		BUSY_OK,
	} ConnectionState;

	fsBridge         connection;      /**< the connection handle */
	ConnectionState  state;           /**< connection status, to help manage it */
} dfsConnection;

typedef boost::shared_ptr<dfsConnection> dfsConnectionPtr;

/**
 * File progress (prepare or any other operation) status.
 * Leave old c++98 enum due this file will be included indirectly by other imapla modules
 * that are still under c++98
 */
struct FileProgressStatus {
	enum fileProgressStatus {
		FILEPROGRESS_NOT_RUN = 0,
		FILEPROGRESS_COMPLETED_OK = 1,
		FILEPROGRESS_IS_MISSED_REMOTELY = 2,
		FILEPROGRESS_REMOTE_DFS_IS_UNREACHABLE = 3,
		FILEPROGRESS_GENERAL_FAILURE = 4,
	};
};

/**
 * File progress, defines the status of the file ManagedFile::File in context of warmup request
 */
struct FileProgress {
	std::size_t localBytes;       /**< number of locally existing bytes for this file  */
	std::size_t estimatedBytes;   /**< size of file, remote, total */
	std::time_t estimatedTime;    /**< estimated time remained to get the file locally */
	std::string localPath; 		  /**< file local path */
	std::string dfsPath; 		  /**< file dfs path */
    FileSystemDescriptor namenode;  /** focal namenode of the cluster which owns this file */
	std::time_t processTime; 	  /**< time file operation was actively performed. It can be used to calculate bandwidth used by the operation */

	FileProgressStatus::fileProgressStatus progressStatus; /**< file progress status */

	bool error; /**< flag, indicates file error */
	std::string errdescr; /**< error description (if any) */

	FileProgress() :
			localBytes(0), estimatedBytes(-1), estimatedTime(0), localPath(""), dfsPath(
					""), processTime(0), progressStatus(
					FileProgressStatus::FILEPROGRESS_NOT_RUN), error(false), errdescr(
					"") {
	}
	/**
	 * isReady - check whether the file is ready
	 */
	bool isReady() {
		return ((localBytes == estimatedBytes) && !error
				&& progressStatus
						== FileProgressStatus::FILEPROGRESS_COMPLETED_OK);
	}

};

/**
 * Defines the request performance statistic
 */
typedef struct {
	int64_t cpu_time_miliseconds;    /**< time which request spent on CPU */
	int64_t lifetime;                /**< time which request was active */

} request_performance;


/**
 * The callback to the context where the Prepare Operation completion report is expected (coordinator).
 * @param context     - session context (client context) which requested for prepare operation
 * @param progress    - list of files scheduled for prepare along with their final progress
 * @param performance - to hold request current performance statistic
 * @param overall     - overall status of operation, true - succeed, false - failure.
 * @param canceled    - flag, indicates whether operation was canceled
 *
 * @return operation status
 */
typedef boost::function<
		void(SessionContext context,
				const std::list<boost::shared_ptr<FileProgress> > & progress,
				request_performance const & performance, bool overall,
				bool canceled, taskOverallStatus status)> PrepareCompletedCallback;


/**
 * The callback to the context where the Estimate Operation completion report is expected.
 * @param context     - session context (client context) which requested for prepare operation
 * @param progress    - list of files along with their estimated metrics.
 * @param time        - overall time required to get the requested dataset locally
 *
 * @return operation status
 */

typedef boost::function<
		void(SessionContext context,
				const std::list<boost::shared_ptr<FileProgress> > & estimation,
				time_t const & time, bool overall, bool canceled, taskOverallStatus status)> CacheEstimationCompletedCallback;

}
#endif /* COMMON_INCLUDE_HPP_ */
