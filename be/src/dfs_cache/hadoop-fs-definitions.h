/*
 * @file  hadoop-fs-definitions.hpp
 * @brief wraps hadoop FileSystem and around it Java types
 *
 * @date   Oct 30, 2014
 * @author elenav
 */

#ifndef HADOOP_FS_DEFINITIONS_HPP_
#define HADOOP_FS_DEFINITIONS_HPP_

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/** supported / configured DFS types */
enum _DFS_TYPE {
	HDFS,
	S3,
	LOCAL,
	DEFAULT_FROM_CONFIG,
	OTHER,               // for testing purposes
	NON_SPECIFIED,
};

typedef enum _DFS_TYPE DFS_TYPE;

/** resolve fs type from given scheme
 *
 * @param scheme - string scheme
 * @return resolved file system type
 * */
extern DFS_TYPE fsTypeFromScheme(const char* scheme);

/** Here defined java-managed types that are passed via c++ through without need of their details */

/** bridge to abstract FileSystem */
typedef void* fsBridge;

/** org.apache.hadoop.conf.Configuration
 *  FileSystem configuration */
typedef void* fsConfiguration;

/** org.apache.hadoop.security.UserGroupInformation */
typedef void* userGroupInformation;

/** org.apache.hadoop.security.token.Token
 * security token for this fs */
typedef void* fsToken;

/**  org.apache.hadoop.security.Credentials */
typedef void* fsCredentials;

/** org.apache.hadoop.fs.FileChecksum
 * An abstract class representing file checksums for files.*/
typedef void* fsChecksum;

/** file system object file */
typedef enum tObjectKind {
	kObjectKindFile = 'F', kObjectKindDirectory = 'D',
} tObjectKind;

/** The C equivalent of org.apache.org.hadoop.FSData(Input|Output)Stream */
enum dfsStreamType {
	UNINITIALIZED = 0, INPUT = 1, OUTPUT = 2,
};

/** File stream accompanied with its type (input or output).
 * Input stream is READ-ONLY. */
struct dfsFile_internal{
	void*              file;   /**< file handle */
	enum dfsStreamType type;   /**< bound stream type */
	int                flags;  /**< flags which the stream was opened with */
};

/** A type definition for internal dfs file */
typedef struct dfsFile_internal* dfsFile;

typedef int32_t tSize;   /** size of data for read/write io ops */
typedef time_t tTime;    /** time type in seconds */
typedef int64_t tOffset; /** offset within the file */
typedef uint16_t tPort;  /** port */

/**
 * The C reflection of org.apache.org.hadoop.FileSystem .
 * We will use this entity to hold the connection handle to remote DFS.
 * Currently this type is widely used by several impala layers but in the future it will be only
 * available inside the Cache layer.
 * All clients that distinguish remote DFS, should specify dfsClusterid instead (which has the form of "dfs_type-host"
 * depicting the remote DFS).
 */

/** DFS Cluster unique representation. Currently "dfs_type-host". */
typedef void* dfsclusterId;

/** Information about a file/directory. */
typedef struct {
	tObjectKind mKind; 		  /**< file or directory */
	char*       mName; 		  /**< the name of the file */
	tTime       mLastMod; 	  /**< the last modification time for the file in seconds */
	tOffset     mSize; 		  /**< the size of the file in bytes */
	short       mReplication; /**< the count of replicas */
	tOffset     mBlockSize;   /**< the block size for the file */
	char*       mOwner; 	  /**< the owner of the file */
	char*       mGroup;       /**< the group associated with the file */
	short       mPermissions; /**< the permissions associated with the file */
	tTime       mLastAccess;  /**< the last access time for the file in seconds */
} dfsFileInfo;


struct dfsReadStatistics{
    uint64_t totalBytesRead;
    uint64_t totalLocalBytesRead;
    uint64_t totalShortCircuitBytesRead;
    uint64_t totalZeroCopyBytesRead;
};

/** Represents org.apache.hadoop.fs.FileStatus */
typedef struct {
	char*  path;
  	long   length;
  	bool   isdir;
  	bool   issymlink;
  	short  block_replication;
  	long   blocksize;
  	long   modification_time;
  	long   access_time;
  	int    permission;
    char*  owner;
    char*  group;
    char*  symlink;
} fileStatus;

/** Represent org.apache.hadoop.fs.FileSystem.Statistics
 *  The statistic of File System
 */
typedef struct {
	char* scheme;
	long  bytesRead;
	long  bytesWritten;
	int   readOps;
	int   largeReadOps;
	int   writeOps;
 } fsStatistics;

/** Represent org.apache.hadoop.fs.BlockLocation
 *  Represents the network location of a block, information about the hosts
 *  that contain block replicas, and other block metadata (E.g. the file
 *  offset associated with the block, length, whether it is corrupt, etc).
 * */
typedef struct {
	char** hosts;         /**< Datanode hostnames */
	char** names;         /**< Datanode IP:xferPort for accessing the block */
	int    numDatanodes;  /**< Number of data nodes */
	char** topologyPaths; /**< Full path name in network topology */

	long offset;  /**< Offset of the block in the file */
	long length;  /**< file length */
	bool corrupt; /**< flag, indicates whether the file is corrupted */
} fsBlockLocation;

/** Represent org.apache.hadoop.fs.ContentSummary
 *  Store the summary of a content (a directory or a file).
 * */
typedef struct {
	long length;
	long fileCount;
	long directoryCount;
	long quota;
	long spaceConsumed;
	long spaceQuota;
} fsContentSummary;

#ifdef __cplusplus
}
#endif

#endif /* HADOOP_FS_DEFINITIONS_HPP_ */
