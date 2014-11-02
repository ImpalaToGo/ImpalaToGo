/** @file dfs-types.h
 *  @brief Definitions of dfs-related entities relevant for Cache layer.
 *
 *  @date   Sep 29, 2014
 *  @author elenav
 */

#ifndef DFS_TYPES_H_
#define DFS_TYPES_H_

#include <sys/types.h>
#include <stdint.h>

namespace impala {

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
// typedef void* dfsFS;

/**
 * DFS Cluster unique representation. Currently "dfs_type-host".
 */
typedef void* dfsclusterId;

typedef enum tObjectKind {
	kObjectKindFile = 'F', kObjectKindDirectory = 'D',
} tObjectKind;

/**
 * The C equivalent of org.apache.org.hadoop.FSData(Input|Output)Stream .
 */
enum dfsStreamType {
	UNINITIALIZED = 0, INPUT = 1, OUTPUT = 2,
};

/**
 * File stream accompanied with its type (input or output).
 * Input stream is READ-ONLY.
 */
struct dfsFile_internal {
	void* file;
	enum dfsStreamType type;
};
/**
 * @typedef dfsFile
 * @brief A type definition for internal dfs file.
 */
typedef struct dfsFile_internal* dfsFile;

/**
 * Information about a file/directory.
 */
typedef struct {
	tObjectKind mKind; /**< file or directory */
	char *mName; /**< the name of the file */
	tTime mLastMod; /**< the last modification time for the file in seconds */
	tOffset mSize; /**< the size of the file in bytes */
	short mReplication; /**< the count of replicas */
	tOffset mBlockSize; /**< the block size for the file */
	char *mOwner; /**< the owner of the file */
	char *mGroup; /**< the group associated with the file */
	short mPermissions; /**< the permissions associated with the file */
	tTime mLastAccess; /**< the last access time for the file in seconds */
} dfsFileInfo;

struct dfsReadStatistics {
  uint64_t totalBytesRead;
  uint64_t totalLocalBytesRead;
  uint64_t totalShortCircuitBytesRead;
};

}
#endif /* DFS_TYPES_H_ */
