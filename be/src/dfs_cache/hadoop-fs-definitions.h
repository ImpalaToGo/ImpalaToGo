/*
 * @file  hadoop-fs-definitions.hpp
 * @brief wraps hadoop FileSystem and around it Java types
 *
 * @date   Oct 30, 2014
 * @author elenav
 */

#include <boost/filesystem.hpp>

#ifndef FS_DEFINITIONS_HPP_
#define FS_DEFINITIONS_HPP_

namespace boost{

namespace filesystem {

// Not present in boost 1.46 which impala uses
enum perms {
    no_perms = 0,       // file_not_found is no_perms rather than perms_not_known

    // POSIX equivalent macros given in comments.
    // Values are from POSIX and are given in octal per the POSIX standard.

    // permission bits

    owner_read  = 0400,   // S_IRUSR, Read permission, owner
    owner_write = 0200,   // S_IWUSR, Write permission, owner
    owner_exe   = 0100,   // S_IXUSR, Execute/search permission, owner
    owner_all   = 0700,   // S_IRWXU, Read, write, execute/search by owner

    group_read  = 040,    // S_IRGRP, Read permission, group
    group_write = 020,    // S_IWGRP, Write permission, group
    group_exe   = 010,    // S_IXGRP, Execute/search permission, group
    group_all   = 070,    // S_IRWXG, Read, write, execute/search by group

    others_read  = 04,    // S_IROTH, Read permission, others
    others_write = 02,    // S_IWOTH, Write permission, others
    others_exe   = 01,    // S_IXOTH, Execute/search permission, others
    others_all   = 07,    // S_IRWXO, Read, write, execute/search by others

    all_all = 0777,           // owner_all|group_all|others_all

    // other POSIX bits

    set_uid_on_exe = 04000,   // S_ISUID, Set-user-ID on execution
    set_gid_on_exe = 02000,   // S_ISGID, Set-group-ID on execution
    sticky_bit     = 01000,   // S_ISVTX,
                              // (POSIX XSI) On directories, restricted deletion flag
	                          // (V7) 'sticky bit': save swapped text even after use
                              // (SunOS) On non-directories: don't cache this file
                              // (SVID-v4.2) On directories: restricted deletion flag
                              // Also see http://en.wikipedia.org/wiki/Sticky_bit

    perms_mask = 07777,       // all_all|set_uid_on_exe|set_gid_on_exe|sticky_bit

    perms_not_known = 0xFFFF, // present when directory_entry cache not loaded

    // options for permissions() function

    add_perms    = 0x1000,     // adds the given permission bits to the current bits
    remove_perms = 0x2000,     // removes the given permission bits from the current bits;
                               // choose add_perms or remove_perms, not both; if neither add_perms
                               // nor remove_perms is given, replace the current bits with
                               // the given bits.

    symlink_perms = 0x4000     // on POSIX, don't resolve symlinks; implied on Windows
  };

/** wrapping for org.apache.hadoop.fs.CreateFlag */
enum createStreamFlag {
  CREATE     = 0x01,           // Create a file
  OVERWRITE  = 0x02,           // Truncate/overwrite a file. Same as POSIX O_TRUNC. See javadoc for description.
  APPEND     = 0x04,           // Append to a file. See javadoc for more description.
  SYNC_BLOCK = 0x08            // Force closed blocks to disk. Similar to POSIX O_SYNC. See javadoc for description.
  };

  BOOST_BITMASK(perms)
  BOOST_BITMASK(createStreamFlag)
} // filesystem
} // boost

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
struct dfsFile_internal {
	void* file;
	enum dfsStreamType type;
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


struct dfsReadStatistics {
    uint64_t totalBytesRead;
    uint64_t totalLocalBytesRead;
    uint64_t totalShortCircuitBytesRead;
    uint64_t totalZeroCopyBytesRead;
};

/** Represents org.apache.hadoop.fs.FileStatus */
struct fileStatus {
	boost::filesystem::path path;
  	long       				length;
  	bool        		    isdir;
  	bool                    issymlink;
  	short       			block_replication;
  	long        		    blocksize;
  	long       				modification_time;
  	long        			access_time;

  	enum boost::filesystem::perms permission;
    char*                   owner;
    char*                   group;
    char*                   symlink;
};

/** Represent org.apache.hadoop.fs.FileSystem.Statistics
 *  The statistic of File System
 */
struct fsStatistics {
	char* scheme;
	long  bytesRead;
	long  bytesWritten;
	int   readOps;
	int   largeReadOps;
	int   writeOps;
 };

/** Represent org.apache.hadoop.fs.BlockLocation
 *  Represents the network location of a block, information about the hosts
 *  that contain block replicas, and other block metadata (E.g. the file
 *  offset associated with the block, length, whether it is corrupt, etc).
 * */
struct fsBlockLocation {
	char** hosts;         /**< Datanode hostnames */
	char** names;         /**< Datanode IP:xferPort for accessing the block */
	int    numDatanodes;  /**< Number of data nodes */
	char** topologyPaths; /**< Full path name in network topology */

	long offset;  /**< Offset of the block in the file */
	long length;  /**< file length */
	bool corrupt; /**< flag, indicates whether the file is corrupted */
};

/** Represent org.apache.hadoop.fs.ContentSummary
 *  Store the summary of a content (a directory or a file).
 * */
struct fsContentSummary {
	long length;
	long fileCount;
	long directoryCount;
	long quota;
	long spaceConsumed;
	long spaceQuota;
};

namespace impala{

extern std::ostream& operator<<(std::ostream &strm, const fsStatistics &statistic);
extern std::ostream& operator<<(std::ostream &strm, const fileStatus &status);

}
#endif /* FS_DEFINITIONS_HPP_ */
