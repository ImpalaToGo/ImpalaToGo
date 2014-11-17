/*
 * @file  hadoopfs-adaptive.cc
 * @brief wrapping for org.apache.hadoop.fs.FileSystem and types that directly used in its API
 * More for wrapped types see "hadoopfs-definitions.h"
 *
 * For this file, hadoop libhdfs sources were utilized:
 * https://svn.apache.org/repos/asf/hadoop/common/tags/release-2.3.0/hadoop-hdfs-project/hadoop-hdfs/src/main/native/libhdfs/hdfs.c
 *
 * @author elenav
 * @date   Nov 4, 2014
 */

#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>

#include "util/exception.h"
#include "util/jni-helper.h"

#include "dfs_cache/hadoop-fs-adaptive.h"

/* Some frequently used Java paths */
#define HADOOP_CONF     "org/apache/hadoop/conf/Configuration"
#define HADOOP_PATH     "org/apache/hadoop/fs/Path"
#define HADOOP_LOCALFS  "org/apache/hadoop/fs/LocalFileSystem"
#define HADOOP_FS       "org/apache/hadoop/fs/FileSystem"
#define HADOOP_FSSTATUS "org/apache/hadoop/fs/FsStatus"
#define HADOOP_BLK_LOC  "org/apache/hadoop/fs/BlockLocation"
#define HADOOP_DFS      "org/apache/hadoop/hdfs/DistributedFileSystem"
#define HADOOP_ISTRM    "org/apache/hadoop/fs/FSDataInputStream"
#define HADOOP_OSTRM    "org/apache/hadoop/fs/FSDataOutputStream"
#define HADOOP_STAT     "org/apache/hadoop/fs/FileStatus"
#define HADOOP_FSPERM   "org/apache/hadoop/fs/permission/FsPermission"
#define JAVA_NET_ISA    "java/net/InetSocketAddress"
#define JAVA_NET_URI    "java/net/URI"
#define JAVA_STRING     "java/lang/String"
#define READ_OPTION     "org/apache/hadoop/fs/ReadOption"

/** URI scheme max length */
#define URI_SCHEME_LENGTH 10;

/** Scheme definitions */
#define SCHEME_HDFS  "hdfs"
#define SCHEME_S3N   "s3n"
#define SCHEME_LOCAL "file"

#define JAVA_VOID       "V"

/* Macros for constructing method signatures */
#define JPARAM(X)           "L" X ";"
#define JARRPARAM(X)        "[L" X ";"
#define JMETHOD1(X, R)      "(" X ")" R
#define JMETHOD2(X, Y, R)   "(" X Y ")" R
#define JMETHOD3(X, Y, Z, R)   "(" X Y Z")" R

#define KERBEROS_TICKET_CACHE_PATH "hadoop.security.kerberos.ticket.cache.path"

// Bit fields for dfsFile_internal flags
#define DFS_FILE_SUPPORTS_DIRECT_READ (1<<0)

DFS_TYPE fsTypeFromScheme(const char* scheme){
	if(strcmp(scheme, SCHEME_HDFS) == 0)
		return HDFS;
    if(strcmp(scheme, SCHEME_S3N) == 0)
    	return S3;
    if(strcmp(scheme, SCHEME_LOCAL) == 0)
    	return LOCAL;
	return NON_SPECIFIED;
}

/**
 * dfsJniEnv: A wrapper struct to be used as 'value'
 * while saving thread -> JNIEnv* mappings
 */
typedef struct
{
    JNIEnv* env;
} dfsJniEnv;

/************************* filesystem/file utility functions ***************/

/** Reads using the read(ByteBuffer) API, which does fewer copies
 *
 *  @param filesystem - filesystem handle
 *  @param file       - file stream
 *  @param size       - length (how much to read from current position)
 *
 *  @return number of bytes read
 */
tSize readDirect(fsBridge fs, dfsFile f, void* buffer, tSize length);

/**
 * Free specified FileInfo entry on its original runtime
 *
 * @param dfsFileInfo - file info entry
 */
static void fsFreeFileInfoEntry(dfsFileInfo *dfsFileInfo);


/********************** Construction Java objects utilities **********************/

/**
 * Helper function to create a org.apache.hadoop.fs.Path object.
 *
 * @param env  - JNIEnv pointer.
 * @param path - file-path for which to construct org.apache.hadoop.fs.Path
 * 				 object.
 *
 * @return jobject on success and NULL on error.
 */
static jthrowable constructNewObjectOfPath(JNIEnv *env, const char *path,
                                           jobject *out)
{
    jthrowable jthr;
    jstring jPathString;
    jobject jPath;

    //Construct a java.lang.String object
    jthr = newJavaStr(env, path, &jPathString);
    if (jthr)
        return jthr;
    //Construct the org.apache.hadoop.fs.Path object
    jthr = constructNewObjectOfClass(env, &jPath, "org/apache/hadoop/fs/Path",
                                     "(Ljava/lang/String;)V", jPathString);
    destroyLocalReference(env, jPathString);
    if (jthr)
        return jthr;
    *out = jPath;
    return NULL;
}

/**
 * Get the default block size of a FileSystem object.
 *
 * @param env       The Java env
 * @param jFS       The FileSystem object
 * @param jPath     The path to find the default blocksize at
 * @param out       (out param) the default block size
 *
 * @return          NULL on success; or the exception
 */
static jthrowable getDefaultBlockSize(JNIEnv *env, jobject jFS,
                                      jobject jPath, jlong *out)
{
    jthrowable jthr;
    jvalue jVal;

    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                 "getDefaultBlockSize", JMETHOD1(JPARAM(HADOOP_PATH), "J"), jPath);
    if (jthr)
        return jthr;
    *out = jVal.j;
    return NULL;
}

static jthrowable
getFileInfoFromStat(JNIEnv *env, jobject jStat, dfsFileInfo *fileInfo)
{
    jvalue jVal;
    jthrowable jthr;
    jobject jPath = NULL;
    jstring jPathName = NULL;
    jstring jUserName = NULL;
    jstring jGroupName = NULL;
    jobject jPermission = NULL;

    jthr = invokeMethod(env, &jVal, INSTANCE, jStat,
                     HADOOP_STAT, "isDir", "()Z");
    if (jthr)
        goto done;
    fileInfo->mKind = jVal.z ? kObjectKindDirectory : kObjectKindFile;

    jthr = invokeMethod(env, &jVal, INSTANCE, jStat,
                     HADOOP_STAT, "getReplication", "()S");
    if (jthr)
        goto done;
    fileInfo->mReplication = jVal.s;

    jthr = invokeMethod(env, &jVal, INSTANCE, jStat,
                     HADOOP_STAT, "getBlockSize", "()J");
    if (jthr)
        goto done;
    fileInfo->mBlockSize = jVal.j;

    jthr = invokeMethod(env, &jVal, INSTANCE, jStat,
                     HADOOP_STAT, "getModificationTime", "()J");
    if (jthr)
        goto done;
    fileInfo->mLastMod = jVal.j / 1000;

    jthr = invokeMethod(env, &jVal, INSTANCE, jStat,
                     HADOOP_STAT, "getAccessTime", "()J");
    if (jthr)
        goto done;
    fileInfo->mLastAccess = (tTime) (jVal.j / 1000);

    if (fileInfo->mKind == kObjectKindFile) {
        jthr = invokeMethod(env, &jVal, INSTANCE, jStat,
                         HADOOP_STAT, "getLen", "()J");
        if (jthr)
            goto done;
        fileInfo->mSize = jVal.j;
    }

    jthr = invokeMethod(env, &jVal, INSTANCE, jStat, HADOOP_STAT,
                     "getPath", "()Lorg/apache/hadoop/fs/Path;");
    if (jthr)
        goto done;
    jPath = jVal.l;
    if (jPath == NULL) {
        jthr = newRuntimeError(env, "org.apache.hadoop.fs.FileStatus#"
            "getPath returned NULL!");
        goto done;
    }

    jthr = invokeMethod(env, &jVal, INSTANCE, jPath, HADOOP_PATH,
                     "toString", "()Ljava/lang/String;");
    if (jthr)
        goto done;
    jPathName = jVal.l;
    const char *cPathName =
        (const char*) ((*env)->GetStringUTFChars(env, jPathName, NULL));
    if (!cPathName) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }
    fileInfo->mName = strdup(cPathName);
    (*env)->ReleaseStringUTFChars(env, jPathName, cPathName);
    jthr = invokeMethod(env, &jVal, INSTANCE, jStat, HADOOP_STAT,
                    "getOwner", "()Ljava/lang/String;");
    if (jthr)
        goto done;
    jUserName = jVal.l;
    const char* cUserName =
        (const char*) ((*env)->GetStringUTFChars(env, jUserName, NULL));
    if (!cUserName) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }
    fileInfo->mOwner = strdup(cUserName);
    (*env)->ReleaseStringUTFChars(env, jUserName, cUserName);

    const char* cGroupName;
    jthr = invokeMethod(env, &jVal, INSTANCE, jStat, HADOOP_STAT,
                    "getGroup", "()Ljava/lang/String;");
    if (jthr)
        goto done;
    jGroupName = jVal.l;
    cGroupName = (const char*) ((*env)->GetStringUTFChars(env, jGroupName, NULL));
    if (!cGroupName) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }
    fileInfo->mGroup = strdup(cGroupName);
    (*env)->ReleaseStringUTFChars(env, jGroupName, cGroupName);

    jthr = invokeMethod(env, &jVal, INSTANCE, jStat, HADOOP_STAT,
            "getPermission",
            "()Lorg/apache/hadoop/fs/permission/FsPermission;");
    if (jthr)
        goto done;
    if (jVal.l == NULL) {
        jthr = newRuntimeError(env, "%s#getPermission returned NULL!",
            HADOOP_STAT);
        goto done;
    }
    jPermission = jVal.l;
    jthr = invokeMethod(env, &jVal, INSTANCE, jPermission, HADOOP_FSPERM,
                         "toShort", "()S");
    if (jthr)
        goto done;
    fileInfo->mPermissions = jVal.s;
    jthr = NULL;

done:
    if (jthr)
        fsFreeFileInfoEntry(fileInfo);
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jPathName);
    destroyLocalReference(env, jUserName);
    destroyLocalReference(env, jGroupName);
    destroyLocalReference(env, jPermission);
    destroyLocalReference(env, jPath);
    return jthr;
}

static jthrowable
getFileInfo(JNIEnv *env, jobject jFS, jobject jPath, dfsFileInfo **fileInfo)
{
    // JAVA EQUIVALENT:
    //  fs.isDirectory(f)
    //  fs.getModificationTime()
    //  fs.getAccessTime()
    //  fs.getLength(f)
    //  f.getPath()
    //  f.getOwner()
    //  f.getGroup()
    //  f.getPermission().toShort()
    jobject jStat;
    jvalue  jVal;
    jthrowable jthr;

    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                     "exists", JMETHOD1(JPARAM(HADOOP_PATH), "Z"),
                     jPath);
    if (jthr)
        return jthr;
    if (jVal.z == 0) {
        *fileInfo = NULL;
        return NULL;
    }
    jthr = invokeMethod(env, &jVal, INSTANCE, jFS,
            HADOOP_FS, "getFileStatus",
            JMETHOD1(JPARAM(HADOOP_PATH), JPARAM(HADOOP_STAT)), jPath);
    if (jthr)
        return jthr;
    jStat = jVal.l;
    *fileInfo = calloc(1, sizeof(dfsFileInfo));
    if (!*fileInfo) {
        destroyLocalReference(env, jStat);
        return newRuntimeError(env, "getFileInfo: OOM allocating hdfsFileInfo");
    }
    jthr = getFileInfoFromStat(env, jStat, *fileInfo);
    destroyLocalReference(env, jStat);
    return jthr;
}

/**
 * Checks input file for readiness for reading.
 *
 * @param [in] env           - jni environment
 * @param [in] fs            - filesystem handle
 * @param [in] f             - file stream
 * @param [out] jInputStream - java stream
 */
static int readPrepare(JNIEnv* env, fsBridge fs, dfsFile f,
                       jobject* jInputStream)
{
    *jInputStream = (jobject)(f ? f->file : NULL);

    //Sanity check
    if (!f || f->type == UNINITIALIZED) {
      errno = EBADF;
      return -1;
    }

    //Error checking... make sure that this file is 'readable'
    if (f->type != INPUT) {
      fprintf(stderr, "Cannot read from a non-InputStream object!\n");
      errno = EINVAL;
      return -1;
    }

    return 0;
}

tSize readDirect(fsBridge filesystem, dfsFile file, void* buffer, tSize length)
{
    // JAVA EQUIVALENT:
    //  ByteBuffer bbuffer = ByteBuffer.allocateDirect(length) // wraps C buffer
    //  fis.read(bbuffer);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jInputStream;
    if (readPrepare(env, filesystem, file, &jInputStream) == -1) {
      return -1;
    }

    jvalue jVal;
    jthrowable jthr;

    //Read the requisite bytes
    jobject bb = (*env)->NewDirectByteBuffer(env, buffer, length);
    if (bb == NULL) {
        errno = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "readDirect: NewDirectByteBuffer");
        return -1;
    }

    jthr = invokeMethod(env, &jVal, INSTANCE, jInputStream,
        HADOOP_ISTRM, "read", "(Ljava/nio/ByteBuffer;)I", bb);
    destroyLocalReference(env, bb);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "readDirect: FSDataInputStream#read");
        return -1;
    }
    return (jVal.i < 0) ? 0 : jVal.i;
}

static int fsCopyImpl(fsBridge srcFS, const char* src, fsBridge dstFS,
        const char* dst, jboolean deleteSource)
{
    //JAVA EQUIVALENT
    //  FileUtil#copy(srcFS, srcPath, dstFS, dstPath,
    //                 deleteSource = false, conf)

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Parameters
    jobject jSrcFS = (jobject)srcFS;
    jobject jDstFS = (jobject)dstFS;
    jobject jConfiguration = NULL, jSrcPath = NULL, jDstPath = NULL;
    jthrowable jthr;
    jvalue jVal;
    int ret;

    jthr = constructNewObjectOfPath(env, src, &jSrcPath);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "fsCopyImpl(src=%s): constructNewObjectOfPath", src);
        goto done;
    }
    jthr = constructNewObjectOfPath(env, dst, &jDstPath);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "fsCopyImpl(dst=%s): constructNewObjectOfPath", dst);
        goto done;
    }

    //Create the org.apache.hadoop.conf.Configuration object
    jthr = constructNewObjectOfClass(env, &jConfiguration,
                                     HADOOP_CONF, "()V");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "fsCopyImpl: Configuration constructor");
        goto done;
    }

    //FileUtil#copy
    jthr = invokeMethod(env, &jVal, STATIC,
            NULL, "org/apache/hadoop/fs/FileUtil", "copy",
            "(Lorg/apache/hadoop/fs/FileSystem;Lorg/apache/hadoop/fs/Path;"
            "Lorg/apache/hadoop/fs/FileSystem;Lorg/apache/hadoop/fs/Path;"
            "ZLorg/apache/hadoop/conf/Configuration;)Z",
            jSrcFS, jSrcPath, jDstFS, jDstPath, deleteSource,
            jConfiguration);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "fsCopyImpl(src=%s, dst=%s, deleteSource=%d): "
            "FileUtil#copy", src, dst, deleteSource);
        goto done;
    }
    if (!jVal.z) {
        ret = EIO;
        goto done;
    }
    ret = 0;

done:
    destroyLocalReference(env, jConfiguration);
    destroyLocalReference(env, jSrcPath);
    destroyLocalReference(env, jDstPath);

    if (ret) {
        errno = ret;
        return -1;
    }
    return 0;
}

static void fsFreeFileInfoEntry(dfsFileInfo *hdfsFileInfo)
{
    free(hdfsFileInfo->mName);
    free(hdfsFileInfo->mOwner);
    free(hdfsFileInfo->mGroup);
    memset(hdfsFileInfo, 0, sizeof(hdfsFileInfo));
}

/**************************** hadoop configuration utilties ****************************/

static jthrowable hadoopConfGetStr(JNIEnv *env, jobject jConfiguration,
        const char *key, char **val)
{
    jthrowable jthr;
    jvalue jVal;
    jstring jkey = NULL, jRet = NULL;

    jthr = newJavaStr(env, key, &jkey);
    if (jthr)
        goto done;
    jthr = invokeMethod(env, &jVal, INSTANCE, jConfiguration,
            HADOOP_CONF, "get", JMETHOD1(JPARAM(JAVA_STRING),
                                         JPARAM(JAVA_STRING)), jkey);
    if (jthr)
        goto done;
    jRet = jVal.l;
    jthr = newCStr(env, jRet, val);
done:
    destroyLocalReference(env, jkey);
    destroyLocalReference(env, jRet);
    return jthr;
}

static jthrowable hadoopConfGetInt(JNIEnv *env, jobject jConfiguration,
        const char *key, int32_t *val)
{
    jthrowable jthr = NULL;
    jvalue jVal;
    jstring jkey = NULL;

    jthr = newJavaStr(env, key, &jkey);
    if (jthr)
        return jthr;
    jthr = invokeMethod(env, &jVal, INSTANCE, jConfiguration,
            HADOOP_CONF, "getInt", JMETHOD2(JPARAM(JAVA_STRING), "I", "I"),
            jkey, (jint)(*val));
    destroyLocalReference(env, jkey);
    if (jthr)
        return jthr;
    *val = jVal.i;
    return NULL;
}

int _dfsConfGetStr(const char *key, char **val)
{
    JNIEnv *env;
    int ret;
    jthrowable jthr;
    jobject jConfiguration = NULL;

    env = getJNIEnv();
    if (env == NULL) {
        ret = EINTERNAL;
        goto done;
    }
    jthr = constructNewObjectOfClass(env, &jConfiguration, HADOOP_CONF, "()V");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsConfGetStr(%s): new Configuration", key);
        goto done;
    }
    jthr = hadoopConfGetStr(env, jConfiguration, key, val);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsConfGetStr(%s): hadoopConfGetStr", key);
        goto done;
    }
    ret = 0;
done:
    destroyLocalReference(env, jConfiguration);
    if (ret)
        errno = ret;
    return ret;
}

void _dfsConfStrFree(char *val)
{
    free(val);
}

int _dfsConfGetInt(const char *key, int32_t *val)
{
    JNIEnv *env;
    int ret;
    jobject jConfiguration = NULL;
    jthrowable jthr;

    env = getJNIEnv();
    if (env == NULL) {
      ret = EINTERNAL;
      goto done;
    }
    jthr = constructNewObjectOfClass(env, &jConfiguration, HADOOP_CONF, "()V");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsConfGetInt(%s): new Configuration", key);
        goto done;
    }
    jthr = hadoopConfGetInt(env, jConfiguration, key, val);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsConfGetInt(%s): hadoopConfGetInt", key);
        goto done;
    }
    ret = 0;
done:
    destroyLocalReference(env, jConfiguration);
    if (ret)
        errno = ret;
    return ret;
}


/***************************** Connection builder **************************************/

struct fsBuilder* _dfsNewBuilder(void)
{
    struct fsBuilder* bld = calloc(1, sizeof(struct fsBuilder));
    if (!bld) {
        errno = ENOMEM;
        return NULL;
    }
    return bld;
}

void _dfsBuilderSetForceNewInstance(struct fsBuilder *bld)
{
    bld->forceNewInstance = 1;
}

void _dfsFreeBuilder(struct fsBuilder *bld)
{
    struct fsBuilderConfOpt *cur, *next;

    cur = bld->opts;
    for (cur = bld->opts; cur; ) {
        next = cur->next;
        free(cur);
        cur = next;
    }
    free(bld);
}

void _dfsBuilderSetHost(struct fsBuilder *bld, const char *host)
{
    bld->host = host;
}

void _dfsBuilderSetHostAndFilesystemType(struct fsBuilder* bld, const char* host, DFS_TYPE dfs_type){
	bld->host = host;
	bld->fs_type = dfs_type;
}

void _dfsBuilderSetPort(struct fsBuilder *bld, tPort port)
{
    bld->port = port;
}

void _dfsBuilderSetUserName(struct fsBuilder *bld, const char *userName)
{
    bld->userName = userName;
}

void _dfsBuilderSetKerbTicketCachePath(struct fsBuilder *bld,
                                       const char *kerbTicketCachePath)
{
    bld->kerbTicketCachePath = kerbTicketCachePath;
}

int _dfsBuilderConfSetStr(struct fsBuilder *bld, const char *key,
                          const char *val)
{
    struct fsBuilderConfOpt *opt, *next;

    opt = calloc(1, sizeof(struct fsBuilderConfOpt));
    if (!opt)
        return -ENOMEM;
    next = bld->opts;
    bld->opts = opt;
    opt->next = next;
    opt->key = key;
    opt->val = val;
    return 0;
}

/**
 * Calculate the effective URI to use, given a builder configuration.
 *
 * If there is not already a URI scheme, we prepend 'file://'.
 *
 * If there is not already a port specified, and a port was given to the
 * builder, we suffix that port.  If there is a port specified but also one in
 * the URI, that is an error.
 *
 * @param [in]  bld - fs builder object
 * @param [out] uri - dynamically allocated string representing the
 *                    effective URI
 *
 * @return 0 on success; error code otherwise
 */
static int calcEffectiveURI(struct fsBuilder *bld, char ** uri)
{
    const char *scheme;
    char suffix[64];
    const char *lastColon;
    char *u;
    size_t uriLen;

    if (!bld->host)
        return EINVAL;

    const char* explicitScheme;
    // check the requested scheme:
    switch(bld->fs_type){
    case HDFS:
    	explicitScheme = "hdfs://";
    	break;
    case S3:
    	explicitScheme = "s3n://";
    	break;
    case LOCAL:
    	explicitScheme = "file://";
    	break;
    default:
    	explicitScheme = "file://";
    	break;
    }
    // if there's already uri with a scheme provided, just skip changes:
    scheme = (strstr(bld->host, "://")) ? "" : explicitScheme;
    if (bld->port == 0) {
        suffix[0] = '\0';
    } else {
        lastColon = rindex(bld->host, ':');
        if (lastColon && (strspn(lastColon + 1, "0123456789") ==
                          strlen(lastColon + 1))) {
            fprintf(stderr, "port %d was given, but URI '%s' already "
                "contains a port!\n", bld->port, bld->host);
            return EINVAL;
        }
        snprintf(suffix, sizeof(suffix), ":%d", bld->port);
    }

    uriLen = strlen(scheme) + strlen(bld->host) + strlen(suffix);
    u = malloc((uriLen + 1) * (sizeof(char)));
    if (!u) {
        fprintf(stderr, "calcEffectiveURI: out of memory");
        return ENOMEM;
    }
    snprintf(u, uriLen + 1, "%s%s%s", scheme, bld->host, suffix);
    *uri = u;
    return 0;
}

static const char *maybeNull(const char *str)
{
    return str ? str : "(NULL)";
}

static const char* fsBuilderToStr(const struct fsBuilder *bld,
                                    char *buf, size_t bufLen)
{
    snprintf(buf, bufLen, "forceNewInstance=%d, host=%s, port=%d, "
             "kerbTicketCachePath=%s, userName=%s",
             bld->forceNewInstance, maybeNull(bld->host), bld->port,
             maybeNull(bld->kerbTicketCachePath), maybeNull(bld->userName));
    return buf;
}


/****************************  FileSystem/File statistics API *******************/

int _dfsFileIsOpenForRead(dfsFile file)
{
    return (file->type == INPUT);
}

int _dfsFileIsOpenForWrite(dfsFile file)
{
    return (file->type == OUTPUT);
}

int _dfsFileUsesDirectRead(dfsFile file)
{
    return !!(file->flags & DFS_FILE_SUPPORTS_DIRECT_READ);
}

void _dfsFileDisableDirectRead(dfsFile file)
{
    file->flags &= ~DFS_FILE_SUPPORTS_DIRECT_READ;
}

int _dfsDisableDomainSocketSecurity(void)
{
    jthrowable jthr;
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }
    jthr = invokeMethod(env, NULL, STATIC, NULL,
            "org/apache/hadoop/net/unix/DomainSocket",
            "disableBindPathValidation", "()V");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "DomainSocket#disableBindPathValidation");
        return -1;
    }
    return 0;
}


/****************************  Initialize and shutdown  ********************************/

int _dfsGetDefaultFsHostPortType(char* host, size_t len, struct fsBuilder *bld, int* port, DFS_TYPE* dfs_type){
    JNIEnv *env = 0;
    jvalue  jVal;
    jobject jConfiguration = NULL, jURI = NULL;

    char buffer[512], scheme[10];
    struct fsBuilderConfOpt *opt;

    jthrowable jthr     = NULL;
    jstring jHostString = NULL;
    jstring jSchemeString = NULL;
    jint jPortInt;

    int ret;

    const char *jHostChars = NULL;
    const char *jSchemeChars = NULL;

    //Get the JNIEnv* corresponding to current thread
    env = getJNIEnv();
    if (env == NULL) {
        ret = EINTERNAL;
        goto done;
    }

    //  jConfiguration = new Configuration();
    jthr = constructNewObjectOfClass(env, &jConfiguration, HADOOP_CONF, "()V");
    if (jthr) {
    	ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
    	            "_getDefaultFsHostPort(%s)", fsBuilderToStr(bld, buffer, sizeof(buffer)));
        goto done;
    }

    // set configuration values
    for (opt = bld->opts; opt; opt = opt->next) {
        jthr = hadoopConfSetStr(env, jConfiguration, opt->key, opt->val);
        if (jthr) {
            ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "_getDefaultFsHostPort(%s): error setting conf '%s' to '%s'",
                fsBuilderToStr(bld, buffer, sizeof(buffer)), opt->key, opt->val);
            goto done;
        }
    }

	// jURI = FileSystem.getDefaultUri(conf)
    jthr = invokeMethod(env, &jVal, STATIC, NULL, HADOOP_FS,
    		"getDefaultUri",
    		"(Lorg/apache/hadoop/conf/Configuration;)Ljava/net/URI;",
    		jConfiguration);
    if (jthr) {
    	ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
	                    "_getDefaultFsUri(%s)", fsBuilderToStr(bld, buffer, sizeof(buffer)));
    	goto done;
    }
    jURI = jVal.l;

    // 1. With URI, get the host:
    jthr = invokeMethod(env, &jVal, INSTANCE, jURI, JAVA_NET_URI,
                     "getHost", "()Ljava/lang/String;");
    if (jthr) {
            ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "_getDefaultFsUri: URI#getHost");
            goto done;
        }

    jHostString = jVal.l;

    jHostChars = (*env)->GetStringUTFChars(env, jHostString, NULL);
    if (!jHostChars) {
        ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "_getDefaultFsUri: GetStringUTFChars");
        goto done;
    }
    // copy host to the buffer provided:
    ret = snprintf(host, len, "%s", jHostChars);

    if (ret >= len) {
        ret = ENAMETOOLONG;
        goto done;
    }

    // reset the ret
    ret = 0;

    // 2. With URI, get the port:
    jthr = invokeMethod(env, &jVal, INSTANCE, jURI, JAVA_NET_URI,
                         "getPort", "()I");
    if (jthr) {
    	ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
    			"_getDefaultFsUri: URI#getPort");
    	goto done;
    }
    jPortInt = jVal.i;

    // got the port:
    *port = jPortInt;

    // 3. With URI, get the scheme:
    jthr = invokeMethod(env, &jVal, INSTANCE, jURI, JAVA_NET_URI,
                     "getScheme", "()Ljava/lang/String;");
    if (jthr) {
            ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "_getDefaultFsUri: URI#getScheme");
            goto done;
        }

    jSchemeString = jVal.l;

    jSchemeChars = (*env)->GetStringUTFChars(env, jSchemeString, NULL);
    if (!jSchemeChars) {
        ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "_getDefaultFsUri: GetStringUTFChars");
        goto done;
    }
    // copy host to the buffer provided:
    ret = snprintf(scheme, sizeof(scheme), "%s", jSchemeChars);

    if (ret >= sizeof(scheme)) {
        ret = ENAMETOOLONG;
        goto done;
    }

    // reset the ret
    ret = 0;

    // resolve dfs type from received scheme:
    *dfs_type = fsTypeFromScheme(scheme);

    done:
    // Release unnecessary local references
    destroyLocalReference(env, jConfiguration);
    _dfsFreeBuilder(bld);

    if (jHostChars) {
    	(*env)->ReleaseStringUTFChars(env, jHostString, jHostChars);
    }
    if (jSchemeChars) {
    	(*env)->ReleaseStringUTFChars(env, jSchemeString, jSchemeChars);
    }
    destroyLocalReference(env, jURI);
    destroyLocalReference(env, jHostString);
    destroyLocalReference(env, jSchemeString);

    if (ret) {
    	errno = ret;
    	return ret;
    }
	return 0;
}

fsBridge _dfsBuilderConnect(struct fsBuilder *bld)
{
    JNIEnv *env = 0;
    jobject jConfiguration = NULL, jFS = NULL, jURI = NULL, jCachePath = NULL;
    jstring jURIString = NULL, jUserString = NULL;
    jvalue  jVal;
    jthrowable jthr = NULL;
    char *cURI = 0, buf[512];
    int ret;
    jobject jRet = NULL;
    struct fsBuilderConfOpt *opt;

    //Get the JNIEnv* corresponding to current thread
    env = getJNIEnv();
    if (env == NULL) {
        ret = EINTERNAL;
        goto done;
    }

    //  jConfiguration = new Configuration();
    jthr = constructNewObjectOfClass(env, &jConfiguration, HADOOP_CONF, "()V");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsBuilderConnect(%s)", fsBuilderToStr(bld, buf, sizeof(buf)));
        goto done;
    }
    // set configuration values
    for (opt = bld->opts; opt; opt = opt->next) {
        jthr = hadoopConfSetStr(env, jConfiguration, opt->key, opt->val);
        if (jthr) {
            ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "_dfsBuilderConnect(%s): error setting conf '%s' to '%s'",
                fsBuilderToStr(bld, buf, sizeof(buf)), opt->key, opt->val);
            goto done;
        }
    }

    //Check what type of FileSystem the caller wants...
    if (bld->host == NULL) {
        // Get a local filesystem.
        if (bld->forceNewInstance) {
            // fs = FileSytem#newInstanceLocal(conf);
            jthr = invokeMethod(env, &jVal, STATIC, NULL, HADOOP_FS,
                    "newInstanceLocal", JMETHOD1(JPARAM(HADOOP_CONF),
                    JPARAM(HADOOP_LOCALFS)), jConfiguration);
            if (jthr) {
                ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                    "_dfsBuilderConnect(%s)",
                    fsBuilderToStr(bld, buf, sizeof(buf)));
                goto done;
            }
            jFS = jVal.l;
        } else {
            // fs = FileSytem#getLocal(conf);
            jthr = invokeMethod(env, &jVal, STATIC, NULL, HADOOP_FS, "getLocal",
                             JMETHOD1(JPARAM(HADOOP_CONF),
                                      JPARAM(HADOOP_LOCALFS)),
                             jConfiguration);
            if (jthr) {
                ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                    "fsBuilderConnect(%s)",
                    fsBuilderToStr(bld, buf, sizeof(buf)));
                goto done;
            }
            jFS = jVal.l;
        }
    } else {
        if (!strcmp(bld->host, "default")) {
            // jURI = FileSystem.getDefaultUri(conf)
            jthr = invokeMethod(env, &jVal, STATIC, NULL, HADOOP_FS,
                          "getDefaultUri",
                          "(Lorg/apache/hadoop/conf/Configuration;)Ljava/net/URI;",
                          jConfiguration);
            if (jthr) {
                ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                    "_dfsBuilderConnect(%s)",
                    fsBuilderToStr(bld, buf, sizeof(buf)));
                goto done;
            }
            jURI = jVal.l;
        } else {
            // fs = FileSystem#get(URI, conf, ugi);
            ret = calcEffectiveURI(bld, &cURI);
            if (ret)
                goto done;
            jthr = newJavaStr(env, cURI, &jURIString);
            if (jthr) {
                ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                    "_dfsBuilderConnect(%s)",
                    fsBuilderToStr(bld, buf, sizeof(buf)));
                goto done;
            }
            jthr = invokeMethod(env, &jVal, STATIC, NULL, JAVA_NET_URI,
                             "create", "(Ljava/lang/String;)Ljava/net/URI;",
                             jURIString);
            if (jthr) {
                ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                    "_dfsBuilderConnect(%s)",
                    fsBuilderToStr(bld, buf, sizeof(buf)));
                goto done;
            }
            jURI = jVal.l;
        }

        if (bld->kerbTicketCachePath) {
            jthr = hadoopConfSetStr(env, jConfiguration,
                KERBEROS_TICKET_CACHE_PATH, bld->kerbTicketCachePath);
            if (jthr) {
                ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                    "_dfsBuilderConnect(%s)",
                    fsBuilderToStr(bld, buf, sizeof(buf)));
                goto done;
            }
        }
        jthr = newJavaStr(env, bld->userName, &jUserString);
        if (jthr) {
            ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "_dfsBuilderConnect(%s)",
                fsBuilderToStr(bld, buf, sizeof(buf)));
            goto done;
        }
        if (bld->forceNewInstance) {
            jthr = invokeMethod(env, &jVal, STATIC, NULL, HADOOP_FS,
                    "newInstance", JMETHOD3(JPARAM(JAVA_NET_URI),
                        JPARAM(HADOOP_CONF), JPARAM(JAVA_STRING),
                        JPARAM(HADOOP_FS)),
                    jURI, jConfiguration, jUserString);
            if (jthr) {
                ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                    "_dfsBuilderConnect(%s)",
                    fsBuilderToStr(bld, buf, sizeof(buf)));
                goto done;
            }
            jFS = jVal.l;
        } else {
            jthr = invokeMethod(env, &jVal, STATIC, NULL, HADOOP_FS, "get",
                    JMETHOD3(JPARAM(JAVA_NET_URI), JPARAM(HADOOP_CONF),
                        JPARAM(JAVA_STRING), JPARAM(HADOOP_FS)),
                        jURI, jConfiguration, jUserString);
            if (jthr) {
                ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                    "_dfsBuilderConnect(%s)",
                    fsBuilderToStr(bld, buf, sizeof(buf)));
                goto done;
            }
            jFS = jVal.l;
        }
    }
    jRet = (*env)->NewGlobalRef(env, jFS);
    if (!jRet) {
        ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
                    "_dfsBuilderConnect(%s)",
                    fsBuilderToStr(bld, buf, sizeof(buf)));
        goto done;
    }
    ret = 0;

done:
    // Release unnecessary local references
    destroyLocalReference(env, jConfiguration);
    destroyLocalReference(env, jFS);
    destroyLocalReference(env, jURI);
    destroyLocalReference(env, jCachePath);
    destroyLocalReference(env, jURIString);
    destroyLocalReference(env, jUserString);
    free(cURI);
    _dfsFreeBuilder(bld);

    if (ret) {
        errno = ret;
        return NULL;
    }
    return (fsBridge)jRet;
}

fsBridge _dfsConnect(const char* host, int port, DFS_TYPE fs_type)
{
    struct fsBuilder *bld = _dfsNewBuilder();
    if (!bld)
        return NULL;

    if(fs_type == NON_SPECIFIED)
    	_dfsBuilderSetHost(bld, host);
    else
    	_dfsBuilderSetHostAndFilesystemType(bld, host, fs_type);

    _dfsBuilderSetPort(bld, port);

    return _dfsBuilderConnect(bld);
}

/** Always return a new FileSystem handle */
fsBridge _dfsConnectNewInstance(const char* host, tPort port, DFS_TYPE fs_type)
{
    struct fsBuilder *bld = _dfsNewBuilder();
    if (!bld)
        return NULL;

    if(fs_type == NON_SPECIFIED)
    	_dfsBuilderSetHost(bld, host);
    else
    	_dfsBuilderSetHostAndFilesystemType(bld, host, fs_type);

    _dfsBuilderSetPort(bld, port);
    _dfsBuilderSetForceNewInstance(bld);
    return _dfsBuilderConnect(bld);
}

fsBridge _dfsConnectAsUser(const char* host, tPort port, const char *user, DFS_TYPE fs_type)
{
    struct fsBuilder *bld = _dfsNewBuilder();
    if (!bld)
        return NULL;

    if(fs_type == NON_SPECIFIED)
    	_dfsBuilderSetHost(bld, host);
    else
    	_dfsBuilderSetHostAndFilesystemType(bld, host, fs_type);

    _dfsBuilderSetPort(bld, port);
    _dfsBuilderSetUserName(bld, user);
    return _dfsBuilderConnect(bld);
}

/** Always return a new FileSystem handle */
fsBridge _dfsConnectAsUserNewInstance(const char* host, tPort port,
		const char *user, DFS_TYPE fs_type)
{
    struct fsBuilder *bld = _dfsNewBuilder();
    if (!bld)
        return NULL;

    if(fs_type == NON_SPECIFIED)
    	_dfsBuilderSetHost(bld, host);
    else
    	_dfsBuilderSetHostAndFilesystemType(bld, host, fs_type);

    _dfsBuilderSetPort(bld, port);
    _dfsBuilderSetForceNewInstance(bld);
    _dfsBuilderSetUserName(bld, user);
    return _dfsBuilderConnect(bld);
}

int _dfsDisconnect(fsBridge fs)
{
    // JAVA EQUIVALENT:
    //  fs.close()

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    int ret;

    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Parameters
    jobject jFS = (jobject)fs;

    //Sanity check
    if (fs == NULL) {
        errno = EBADF;
        return -1;
    }

    jthrowable jthr = invokeMethod(env, NULL, INSTANCE, jFS, HADOOP_FS,
                     "close", "()V");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "dfsDisconnect: FileSystem#close");
    } else {
        ret = 0;
    }
    (*env)->DeleteGlobalRef(env, jFS);
    if (ret) {
        errno = ret;
        return -1;
    }
    return 0;
}


/****************************  Filesystem operations  ********************************/

int _dfsPathExists(fsBridge fs, const char *path)
{
    JNIEnv *env = getJNIEnv();
    if (env == NULL) {
        errno = EINTERNAL;
        return -1;
    }

    jobject jPath;
    jvalue  jVal;
    jobject jFS = (jobject)fs;
    jthrowable jthr;

    if (path == NULL) {
        errno = EINVAL;
        return -1;
    }
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsPathExists: constructNewObjectOfPath");
        return -1;
    }
    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
            "exists", JMETHOD1(JPARAM(HADOOP_PATH), "Z"), jPath);
    destroyLocalReference(env, jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsPathExists: invokeMethod(%s)",
            JMETHOD1(JPARAM(HADOOP_PATH), "Z"));
        return -1;
    }
    if (jVal.z) {
        return 0;
    } else {
        errno = ENOENT;
        return -1;
    }
}

char*** _dfsGetHosts(fsBridge fs, const char* path, tOffset start, tOffset length)
{
    // JAVA EQUIVALENT:
    //  fs.getFileBlockLoctions(new Path(path), start, length);
    jthrowable jthr;
    jobject jPath = NULL;
    jobject jFileStatus = NULL;
    jvalue jFSVal, jVal;
    jobjectArray jBlockLocations = NULL, jFileBlockHosts = NULL;
    jstring jHost = NULL;
    char*** blockHosts = NULL;
    int i, j, ret;
    jsize jNumFileBlocks = 0;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    jobject jFS = (jobject)fs;

    //Create an object of org.apache.hadoop.fs.Path
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsGetHosts(path=%s): constructNewObjectOfPath", path);
        goto done;
    }
    jthr = invokeMethod(env, &jFSVal, INSTANCE, jFS,
            HADOOP_FS, "getFileStatus", "(Lorg/apache/hadoop/fs/Path;)"
            "Lorg/apache/hadoop/fs/FileStatus;", jPath);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, NOPRINT_EXC_FILE_NOT_FOUND,
                "_dfsGetHosts(path=%s, start=%"PRId64", length=%"PRId64"):"
                "FileSystem#getFileStatus", path, start, length);
        destroyLocalReference(env, jPath);
        goto done;
    }
    jFileStatus = jFSVal.l;

    //org.apache.hadoop.fs.FileSystem#getFileBlockLocations
    jthr = invokeMethod(env, &jVal, INSTANCE, jFS,
                     HADOOP_FS, "getFileBlockLocations",
                     "(Lorg/apache/hadoop/fs/FileStatus;JJ)"
                     "[Lorg/apache/hadoop/fs/BlockLocation;",
                     jFileStatus, start, length);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "_dfsGetHosts(path=%s, start=%"PRId64", length=%"PRId64"):"
                "FileSystem#getFileBlockLocations", path, start, length);
        goto done;
    }
    jBlockLocations = jVal.l;

    //Figure out no of entries in jBlockLocations
    //Allocate memory and add NULL at the end
    jNumFileBlocks = (*env)->GetArrayLength(env, jBlockLocations);

    blockHosts = calloc(jNumFileBlocks + 1, sizeof(char**));
    if (blockHosts == NULL) {
        ret = ENOMEM;
        goto done;
    }
    if (jNumFileBlocks == 0) {
        ret = 0;
        goto done;
    }

    //Now parse each block to get hostnames
    for (i = 0; i < jNumFileBlocks; ++i) {
        jobject jFileBlock =
            (*env)->GetObjectArrayElement(env, jBlockLocations, i);
        if (!jFileBlock) {
            ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
                "_dfsGetHosts(path=%s, start=%"PRId64", length=%"PRId64"):"
                "GetObjectArrayElement(%d)", path, start, length, i);
            goto done;
        }

        jthr = invokeMethod(env, &jVal, INSTANCE, jFileBlock, HADOOP_BLK_LOC,
                         "getHosts", "()[Ljava/lang/String;");
        if (jthr) {
            ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "_dfsGetHosts(path=%s, start=%"PRId64", length=%"PRId64"):"
                "BlockLocation#getHosts", path, start, length);
            goto done;
        }
        jFileBlockHosts = jVal.l;
        if (!jFileBlockHosts) {
            fprintf(stderr,
                "_dfsGetHosts(path=%s, start=%"PRId64", length=%"PRId64"):"
                "BlockLocation#getHosts returned NULL", path, start, length);
            ret = EINTERNAL;
            goto done;
        }
        //Figure out no of hosts in jFileBlockHosts, and allocate the memory
        jsize jNumBlockHosts = (*env)->GetArrayLength(env, jFileBlockHosts);
        blockHosts[i] = calloc(jNumBlockHosts + 1, sizeof(char*));
        if (!blockHosts[i]) {
            ret = ENOMEM;
            goto done;
        }

        //Now parse each hostname
        const char *hostName;
        for (j = 0; j < jNumBlockHosts; ++j) {
            jHost = (*env)->GetObjectArrayElement(env, jFileBlockHosts, j);
            if (!jHost) {
                ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
                    "_dfsGetHosts(path=%s, start=%"PRId64", length=%"PRId64"): "
                    "NewByteArray", path, start, length);
                goto done;
            }
            hostName =
                (const char*)((*env)->GetStringUTFChars(env, jHost, NULL));
            if (!hostName) {
                ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
                    "_dfsGetHosts(path=%s, start=%"PRId64", length=%"PRId64", "
                    "j=%d out of %d): GetStringUTFChars",
                    path, start, length, j, jNumBlockHosts);
                goto done;
            }
            blockHosts[i][j] = strdup(hostName);
            (*env)->ReleaseStringUTFChars(env, jHost, hostName);
            if (!blockHosts[i][j]) {
                ret = ENOMEM;
                goto done;
            }
            destroyLocalReference(env, jHost);
            jHost = NULL;
        }

        destroyLocalReference(env, jFileBlockHosts);
        jFileBlockHosts = NULL;
    }
    ret = 0;

done:
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jFileStatus);
    destroyLocalReference(env, jBlockLocations);
    destroyLocalReference(env, jFileBlockHosts);
    destroyLocalReference(env, jHost);
    if (ret) {
        if (blockHosts) {
            _dfsFreeHosts(blockHosts);
        }
        return NULL;
    }

    return blockHosts;
}

void _dfsFreeHosts(char ***blockHosts)
{
    int i, j;
    for (i=0; blockHosts[i]; i++) {
        for (j=0; blockHosts[i][j]; j++) {
            free(blockHosts[i][j]);
        }
        free(blockHosts[i]);
    }
    free(blockHosts);
}

tOffset _dfsGetCapacity(fsBridge fs)
{
    // JAVA EQUIVALENT:
    //  FsStatus fss = fs.getStatus();
    //  return Fss.getCapacity();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;

    //FileSystem#getStatus
    jvalue  jVal;
    jthrowable jthr;
    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                     "getStatus", "()Lorg/apache/hadoop/fs/FsStatus;");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsGetCapacity: FileSystem#getStatus");
        return -1;
    }
    jobject fss = (jobject)jVal.l;
    jthr = invokeMethod(env, &jVal, INSTANCE, fss, HADOOP_FSSTATUS,
                     "getCapacity", "()J");
    destroyLocalReference(env, fss);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsGetCapacity: FsStatus#getCapacity");
        return -1;
    }
    return jVal.j;
}

tOffset _dfsGetUsed(fsBridge fs)
{
    // JAVA EQUIVALENT:
    //  FsStatus fss = fs.getStatus();
    //  return Fss.getUsed();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;

    //FileSystem#getStatus
    jvalue  jVal;
    jthrowable jthr;
    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                     "getStatus", "()Lorg/apache/hadoop/fs/FsStatus;");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsGetUsed: FileSystem#getStatus");
        return -1;
    }
    jobject fss = (jobject)jVal.l;
    jthr = invokeMethod(env, &jVal, INSTANCE, fss, HADOOP_FSSTATUS,
                     "getUsed", "()J");
    destroyLocalReference(env, fss);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsGetUsed: FsStatus#getUsed");
        return -1;
    }
    return jVal.j;
}

char* _dfsGetWorkingDirectory(fsBridge fs, char* buffer, size_t bufferSize)
{
    // JAVA EQUIVALENT:
    //  Path p = fs.getWorkingDirectory();
    //  return p.toString()

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    jobject jPath = NULL;
    jstring jPathString = NULL;
    jobject jFS = (jobject)fs;
    jvalue jVal;
    jthrowable jthr;
    int ret;
    const char *jPathChars = NULL;

    //FileSystem#getWorkingDirectory()
    jthr = invokeMethod(env, &jVal, INSTANCE, jFS,
                     HADOOP_FS, "getWorkingDirectory",
                     "()Lorg/apache/hadoop/fs/Path;");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsGetWorkingDirectory: FileSystem#getWorkingDirectory");
        goto done;
    }
    jPath = jVal.l;
    if (!jPath) {
        fprintf(stderr, "_dfsGetWorkingDirectory: "
            "FileSystem#getWorkingDirectory returned NULL");
        ret = -EIO;
        goto done;
    }

    //Path#toString()
    jthr = invokeMethod(env, &jVal, INSTANCE, jPath,
                     "org/apache/hadoop/fs/Path", "toString",
                     "()Ljava/lang/String;");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsGetWorkingDirectory: Path#toString");
        goto done;
    }
    jPathString = jVal.l;
    jPathChars = (*env)->GetStringUTFChars(env, jPathString, NULL);
    if (!jPathChars) {
        ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "_dfsGetWorkingDirectory: GetStringUTFChars");
        goto done;
    }

    //Copy to user-provided buffer
    ret = snprintf(buffer, bufferSize, "%s", jPathChars);
    if (ret >= bufferSize) {
        ret = ENAMETOOLONG;
        goto done;
    }
    ret = 0;

done:
    if (jPathChars) {
        (*env)->ReleaseStringUTFChars(env, jPathString, jPathChars);
    }
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jPathString);

    if (ret) {
        errno = ret;
        return NULL;
    }
    return buffer;
}

int _dfsSetWorkingDirectory(fsBridge fs, const char* path)
{
    // JAVA EQUIVALENT:
    //  fs.setWorkingDirectory(Path(path));

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;
    jthrowable jthr;
    jobject jPath;

    //Create an object of org.apache.hadoop.fs.Path
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsSetWorkingDirectory(%s): constructNewObjectOfPath",
            path);
        return -1;
    }

    //FileSystem#setWorkingDirectory()
    jthr = invokeMethod(env, NULL, INSTANCE, jFS, HADOOP_FS,
                     "setWorkingDirectory",
                     "(Lorg/apache/hadoop/fs/Path;)V", jPath);
    destroyLocalReference(env, jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, NOPRINT_EXC_ILLEGAL_ARGUMENT,
            "_dfsSetWorkingDirectory(%s): FileSystem#setWorkingDirectory",
            path);
        return -1;
    }
    return 0;
}

int _dfsCopy(fsBridge srcFS, const char* src, fsBridge dstFS, const char* dst)
{
    return fsCopyImpl(srcFS, src, dstFS, dst, 0);
}

int _dfsMove(fsBridge srcFS, const char* src, fsBridge dstFS, const char* dst)
{
    return fsCopyImpl(srcFS, src, dstFS, dst, 1);
}

int _dfsDelete(fsBridge fs, const char* path, int recursive)
{
    // JAVA EQUIVALENT:
    //  Path p = new Path(path);
    //  bool retval = fs.delete(p, recursive);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;
    jthrowable jthr;
    jobject jPath;
    jvalue jVal;

    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsDelete(path=%s): constructNewObjectOfPath", path);
        return -1;
    }
    jboolean jRecursive = recursive ? JNI_TRUE : JNI_FALSE;
    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                     "delete", "(Lorg/apache/hadoop/fs/Path;Z)Z",
                     jPath, jRecursive);
    destroyLocalReference(env, jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsDelete(path=%s, recursive=%d): "
            "FileSystem#delete", path, recursive);
        return -1;
    }
    if (!jVal.z) {
        errno = EIO;
        return -1;
    }
    return 0;
}

int _dfsRename(fsBridge fs, const char* oldPath, const char* newPath)
{
    // JAVA EQUIVALENT:
    //  Path old = new Path(oldPath);
    //  Path new = new Path(newPath);
    //  fs.rename(old, new);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;
    jthrowable jthr;
    jobject jOldPath = NULL, jNewPath = NULL;
    int ret = -1;
    jvalue jVal;

    jthr = constructNewObjectOfPath(env, oldPath, &jOldPath );
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsRename: constructNewObjectOfPath(%s)", oldPath);
        goto done;
    }
    jthr = constructNewObjectOfPath(env, newPath, &jNewPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsRename: constructNewObjectOfPath(%s)", newPath);
        goto done;
    }

    // Rename the file
    // TODO: use rename2 here?  (See HDFS-3592)
    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS, "rename",
                     JMETHOD2(JPARAM(HADOOP_PATH), JPARAM(HADOOP_PATH), "Z"),
                     jOldPath, jNewPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsRename(oldPath=%s, newPath=%s): FileSystem#rename",
            oldPath, newPath);
        goto done;
    }
    if (!jVal.z) {
        errno = EIO;
        goto done;
    }
    ret = 0;

done:
    destroyLocalReference(env, jOldPath);
    destroyLocalReference(env, jNewPath);
    return ret;
}

int _dfsCreateDirectory(fsBridge fs, const char* path)
{
    // JAVA EQUIVALENT:
    //  fs.mkdirs(new Path(path));

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;
    jobject jPath;
    jthrowable jthr;

    //Create an object of org.apache.hadoop.fs.Path
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsCreateDirectory(%s): constructNewObjectOfPath", path);
        return -1;
    }

    //Create the directory
    jvalue jVal;
    jVal.z = 0;
    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                     "mkdirs", "(Lorg/apache/hadoop/fs/Path;)Z",
                     jPath);
    destroyLocalReference(env, jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr,
            NOPRINT_EXC_ACCESS_CONTROL | NOPRINT_EXC_FILE_NOT_FOUND |
            NOPRINT_EXC_UNRESOLVED_LINK | NOPRINT_EXC_PARENT_NOT_DIRECTORY,
            "_dfsCreateDirectory(%s): FileSystem#mkdirs", path);
        return -1;
    }
    if (!jVal.z) {
        // It's unclear under exactly which conditions FileSystem#mkdirs
        // is supposed to return false (as opposed to throwing an exception.)
        // It seems like the current code never actually returns false.
        // So we're going to translate this to EIO, since there seems to be
        // nothing more specific we can do with it.
        errno = EIO;
        return -1;
    }
    return 0;
}

int _dfsSetReplication(fsBridge fs, const char* path, int16_t replication)
{
    // JAVA EQUIVALENT:
    //  fs.setReplication(new Path(path), replication);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;
    jthrowable jthr;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath;
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsSetReplication(path=%s): constructNewObjectOfPath", path);
        return -1;
    }

    //Create the directory
    jvalue jVal;
    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                     "setReplication", "(Lorg/apache/hadoop/fs/Path;S)Z",
                     jPath, replication);
    destroyLocalReference(env, jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsSetReplication(path=%s, replication=%d): "
            "FileSystem#setReplication", path, replication);
        return -1;
    }
    if (!jVal.z) {
        // setReplication returns false "if file does not exist or is a
        // directory."  So the nearest translation to that is ENOENT.
        errno = ENOENT;
        return -1;
    }

    return 0;
}

dfsFileInfo* _dfsListDirectory(fsBridge fs, const char* path, int *numEntries)
{
    // JAVA EQUIVALENT:
    //  Path p(path);
    //  Path []pathList = fs.listPaths(p)
    //  foreach path in pathList
    //    getFileInfo(path)
    jthrowable jthr;
    jobject jPath = NULL;
    dfsFileInfo *pathList = NULL;
    jobjectArray jPathList = NULL;
    jvalue jVal;
    jsize jPathListSize = 0;
    int ret;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    jobject jFS = (jobject)fs;

    //Create an object of org.apache.hadoop.fs.Path
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsListDirectory(%s): constructNewObjectOfPath", path);
        goto done;
    }

    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_DFS, "listStatus",
                     JMETHOD1(JPARAM(HADOOP_PATH), JARRPARAM(HADOOP_STAT)),
                     jPath);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr,
            NOPRINT_EXC_ACCESS_CONTROL | NOPRINT_EXC_FILE_NOT_FOUND |
            NOPRINT_EXC_UNRESOLVED_LINK,
            "_dfsListDirectory(%s): FileSystem#listStatus", path);
        goto done;
    }
    jPathList = jVal.l;

    //Figure out the number of entries in that directory
    jPathListSize = (*env)->GetArrayLength(env, jPathList);
    if (jPathListSize == 0) {
        ret = 0;
        goto done;
    }

    //Allocate memory
    pathList = calloc(jPathListSize, sizeof(dfsFileInfo));
    if (pathList == NULL) {
        ret = ENOMEM;
        goto done;
    }

    //Save path information in pathList
    jsize i;
    jobject tmpStat;
    for (i=0; i < jPathListSize; ++i) {
        tmpStat = (*env)->GetObjectArrayElement(env, jPathList, i);
        if (!tmpStat) {
            ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
                "_dfsListDirectory(%s): GetObjectArrayElement(%d out of %d)",
                path, i, jPathListSize);
            goto done;
        }
        jthr = getFileInfoFromStat(env, tmpStat, &pathList[i]);
        destroyLocalReference(env, tmpStat);
        if (jthr) {
            ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "_dfsListDirectory(%s): getFileInfoFromStat(%d out of %d)",
                path, i, jPathListSize);
            goto done;
        }
    }
    ret = 0;

done:
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jPathList);

    if (ret) {
    	_dfsFreeFileInfo(pathList, jPathListSize);
        errno = ret;
        return NULL;
    }
    *numEntries = jPathListSize;
    return pathList;
}

dfsFileInfo* _dfsGetPathInfo(fsBridge fs, const char* path)
{
    // JAVA EQUIVALENT:
    //  File f(path);
    //  fs.isDirectory(f)
    //  fs.lastModified() ??
    //  fs.getLength(f)
    //  f.getPath()

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    jobject jFS = (jobject)fs;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath;
    jthrowable jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsGetPathInfo(%s): constructNewObjectOfPath", path);
        return NULL;
    }
    dfsFileInfo *fileInfo;
    jthr = getFileInfo(env, jFS, jPath, &fileInfo);
    destroyLocalReference(env, jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr,
            NOPRINT_EXC_ACCESS_CONTROL | NOPRINT_EXC_FILE_NOT_FOUND |
            NOPRINT_EXC_UNRESOLVED_LINK,
            "_dfsGetPathInfo(%s): getFileInfo", path);
        return NULL;
    }
    if (!fileInfo) {
        errno = ENOENT;
        return NULL;
    }
    return fileInfo;
}

void _dfsFreeFileInfo(dfsFileInfo *dfsFileInfo, int numEntries)
{
    //Free the mName, mOwner, and mGroup
    int i;
    for (i=0; i < numEntries; ++i) {
        fsFreeFileInfoEntry(dfsFileInfo + i);
    }
    //Free entire block
    free(dfsFileInfo);
}

tOffset _dfsGetDefaultBlockSize(fsBridge fs)
{
    // JAVA EQUIVALENT:
    //  fs.getDefaultBlockSize();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;

    //FileSystem#getDefaultBlockSize()
    jvalue jVal;
    jthrowable jthr;
    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                     "getDefaultBlockSize", "()J");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsGetDefaultBlockSize: FileSystem#getDefaultBlockSize");
        return -1;
    }
    return jVal.j;
}

tOffset _dfsGetDefaultBlockSizeAtPath(fsBridge fs, const char *path)
{
    // JAVA EQUIVALENT:
    //  fs.getDefaultBlockSize(path);

    jthrowable jthr;
    jobject jFS = (jobject)fs;
    jobject jPath;
    tOffset blockSize;
    JNIEnv* env = getJNIEnv();

    if (env == NULL) {
        errno = EINTERNAL;
        return -1;
    }
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsGetDefaultBlockSizeAtPath(path=%s): constructNewObjectOfPath",
            path);
        return -1;
    }
    jthr = getDefaultBlockSize(env, jFS, jPath, &blockSize);
    (*env)->DeleteLocalRef(env, jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsGetDefaultBlockSizeAtPath(path=%s): "
            "FileSystem#getDefaultBlockSize", path);
        return -1;
    }
    return blockSize;
}

int _dfsChown(fsBridge fs, const char* path, const char *owner, const char *group)
{
    // JAVA EQUIVALENT:
    //  fs.setOwner(path, owner, group)

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    if (owner == NULL && group == NULL) {
      return 0;
    }

    jobject jFS = (jobject)fs;
    jobject jPath = NULL;
    jstring jOwner = NULL, jGroup = NULL;
    jthrowable jthr;
    int ret;

    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsChown(path=%s): constructNewObjectOfPath", path);
        goto done;
    }

    jthr = newJavaStr(env, owner, &jOwner);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsChown(path=%s): newJavaStr(%s)", path, owner);
        goto done;
    }
    jthr = newJavaStr(env, group, &jGroup);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsChown(path=%s): newJavaStr(%s)", path, group);
        goto done;
    }

    //Create the directory
    jthr = invokeMethod(env, NULL, INSTANCE, jFS, HADOOP_FS,
            "setOwner", JMETHOD3(JPARAM(HADOOP_PATH),
                    JPARAM(JAVA_STRING), JPARAM(JAVA_STRING), JAVA_VOID),
            jPath, jOwner, jGroup);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr,
            NOPRINT_EXC_ACCESS_CONTROL | NOPRINT_EXC_FILE_NOT_FOUND |
            NOPRINT_EXC_UNRESOLVED_LINK,
            "_dfsChown(path=%s, owner=%s, group=%s): "
            "FileSystem#setOwner", path, owner, group);
        goto done;
    }
    ret = 0;

done:
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jOwner);
    destroyLocalReference(env, jGroup);

    if (ret) {
        errno = ret;
        return -1;
    }
    return 0;
}

int _dfsChmod(fsBridge fs, const char* path, short mode)
{
    int ret;
    // JAVA EQUIVALENT:
    //  fs.setPermission(path, FsPermission)

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jthrowable jthr;
    jobject jPath = NULL, jPermObj = NULL;
    jobject jFS = (jobject)fs;

    // construct jPerm = FsPermission.createImmutable(short mode);
    jshort jmode = mode;
    jthr = constructNewObjectOfClass(env, &jPermObj,
                HADOOP_FSPERM,"(S)V",jmode);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "constructNewObjectOfClass(%s)", HADOOP_FSPERM);
        return -1;
    }

    //Create an object of org.apache.hadoop.fs.Path
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsChmod(%s): constructNewObjectOfPath", path);
        goto done;
    }

    //Create the directory
    jthr = invokeMethod(env, NULL, INSTANCE, jFS, HADOOP_FS,
            "setPermission",
            JMETHOD2(JPARAM(HADOOP_PATH), JPARAM(HADOOP_FSPERM), JAVA_VOID),
            jPath, jPermObj);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr,
            NOPRINT_EXC_ACCESS_CONTROL | NOPRINT_EXC_FILE_NOT_FOUND |
            NOPRINT_EXC_UNRESOLVED_LINK,
            "_dfsChmod(%s): FileSystem#setPermission", path);
        goto done;
    }
    ret = 0;

done:
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jPermObj);

    if (ret) {
        errno = ret;
        return -1;
    }
    return 0;
}

int _dfsUtime(fsBridge fs, const char* path, tTime mtime, tTime atime)
{
    // JAVA EQUIVALENT:
    //  fs.setTimes(src, mtime, atime)
    jthrowable jthr;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath;
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsUtime(path=%s): constructNewObjectOfPath", path);
        return -1;
    }

    const tTime NO_CHANGE = -1;
    jlong jmtime = (mtime == NO_CHANGE) ? -1 : (mtime * (jlong)1000);
    jlong jatime = (atime == NO_CHANGE) ? -1 : (atime * (jlong)1000);

    jthr = invokeMethod(env, NULL, INSTANCE, jFS, HADOOP_FS,
            "setTimes", JMETHOD3(JPARAM(HADOOP_PATH), "J", "J", JAVA_VOID),
            jPath, jmtime, jatime);
    destroyLocalReference(env, jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr,
            NOPRINT_EXC_ACCESS_CONTROL | NOPRINT_EXC_FILE_NOT_FOUND |
            NOPRINT_EXC_UNRESOLVED_LINK,
            "_dfsUtime(path=%s): FileSystem#setTimes", path);
        return -1;
    }
    return 0;
}


/**********************************  Operations with org.apache.hadoop.fs.FSData(Output|Input)Stream and file objects **/

dfsFile _dfsOpenFile(fsBridge fs, const char* path, int flags,
                      int bufferSize, short replication, tSize blockSize)
{
    /*
      JAVA EQUIVALENT:
       File f = new File(path);
       FSData{Input|Output}Stream f{is|os} = fs.create(f);
       return f{is|os};
    */
    /* Get the JNIEnv* corresponding to current thread */
    JNIEnv* env = getJNIEnv();
    int accmode = flags & O_ACCMODE;

    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    jstring jStrBufferSize = NULL, jStrReplication = NULL;
    jobject jConfiguration = NULL, jPath = NULL, jFile = NULL;
    jobject jFS = (jobject)fs;
    jthrowable jthr;
    jvalue jVal;
    dfsFile file = NULL;
    int ret;

    if (accmode == O_RDONLY || accmode == O_WRONLY) {
	/* yay */
    } else if (accmode == O_RDWR) {
      fprintf(stderr, "ERROR: cannot open an hdfs file in O_RDWR mode\n");
      errno = ENOTSUP;
      return NULL;
    } else {
      fprintf(stderr, "ERROR: cannot open an hdfs file in mode 0x%x\n", accmode);
      errno = EINVAL;
      return NULL;
    }

    if ((flags & O_CREAT) && (flags & O_EXCL)) {
      fprintf(stderr, "WARN: hdfs does not truly support O_CREATE && O_EXCL\n");
    }

    /* The hadoop java api/signature */
    const char* method = NULL;
    const char* signature = NULL;

    if (accmode == O_RDONLY) {
	method = "open";
        signature = JMETHOD2(JPARAM(HADOOP_PATH), "I", JPARAM(HADOOP_ISTRM));
    } else if (flags & O_APPEND) {
	method = "append";
	signature = JMETHOD1(JPARAM(HADOOP_PATH), JPARAM(HADOOP_OSTRM));
    } else {
	method = "create";
	signature = JMETHOD2(JPARAM(HADOOP_PATH), "ZISJ", JPARAM(HADOOP_OSTRM));
    }

    /* Create an object of org.apache.hadoop.fs.Path */
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsOpenFile(%s): constructNewObjectOfPath", path);
        goto done;
    }

    /* Get the Configuration object from the FileSystem object */
    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                     "getConf", JMETHOD1("", JPARAM(HADOOP_CONF)));
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsOpenFile(%s): FileSystem#getConf", path);
        goto done;
    }
    jConfiguration = jVal.l;

    jint jBufferSize = bufferSize;
    jshort jReplication = replication;
    jStrBufferSize = (*env)->NewStringUTF(env, "io.file.buffer.size");
    if (!jStrBufferSize) {
        ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL, "OOM");
        goto done;
    }
    jStrReplication = (*env)->NewStringUTF(env, "dfs.replication");
    if (!jStrReplication) {
        ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL, "OOM");
        goto done;
    }

    if (!bufferSize) {
        jthr = invokeMethod(env, &jVal, INSTANCE, jConfiguration,
                         HADOOP_CONF, "getInt", "(Ljava/lang/String;I)I",
                         jStrBufferSize, 4096);
        if (jthr) {
            ret = printExceptionAndFree(env, jthr, NOPRINT_EXC_FILE_NOT_FOUND |
                NOPRINT_EXC_ACCESS_CONTROL | NOPRINT_EXC_UNRESOLVED_LINK,
                "_dfsOpenFile(%s): Configuration#getInt(io.file.buffer.size)",
                path);
            goto done;
        }
        jBufferSize = jVal.i;
    }

    if ((accmode == O_WRONLY) && (flags & O_APPEND) == 0) {
        if (!replication) {
            jthr = invokeMethod(env, &jVal, INSTANCE, jConfiguration,
                             HADOOP_CONF, "getInt", "(Ljava/lang/String;I)I",
                             jStrReplication, 1);
            if (jthr) {
                ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                    "_dfsOpenFile(%s): Configuration#getInt(dfs.replication)",
                    path);
                goto done;
            }
            jReplication = jVal.i;
        }
    }

    /* Create and return either the FSDataInputStream or
       FSDataOutputStream references jobject jStream */

    // READ?
    if (accmode == O_RDONLY) {
        jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                       method, signature, jPath, jBufferSize);
    }  else if ((accmode == O_WRONLY) && (flags & O_APPEND)) {
        // WRITE/APPEND?
       jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                       method, signature, jPath);
    } else {
        // WRITE/CREATE
        jboolean jOverWrite = 1;
        jlong jBlockSize = blockSize;

        if (jBlockSize == 0) {
            jthr = getDefaultBlockSize(env, jFS, jPath, &jBlockSize);
            if (jthr) {
                ret = EIO;
                goto done;
            }
        }
        jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                         method, signature, jPath, jOverWrite,
                         jBufferSize, jReplication, jBlockSize);
    }
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsOpenFile(%s): FileSystem#%s(%s)", path, method, signature);
        goto done;
    }
    jFile = jVal.l;

    file = calloc(1, sizeof(struct dfsFile_internal));
    if (!file) {
        fprintf(stderr, "_dfsOpenFile(%s): OOM create hdfsFile\n", path);
        ret = ENOMEM;
        goto done;
    }
    file->file = (*env)->NewGlobalRef(env, jFile);
    if (!file->file) {
        ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "_dfsOpenFile(%s): NewGlobalRef", path);
        goto done;
    }
    file->type = (((flags & O_WRONLY) == 0) ? INPUT : OUTPUT);
    file->flags = 0;

    if ((flags & O_WRONLY) == 0) {
        // Try a test read to see if we can do direct reads
        char buf;
        if (readDirect(fs, file, &buf, 0) == 0) {
            // Success - 0-byte read should return 0
            file->flags |= DFS_FILE_SUPPORTS_DIRECT_READ;
        } /*else if (errno != ENOTSUP) {
            // Unexpected error. Clear it, don't set the direct flag.
            fprintf(stderr,
                  "_dfsOpenFile(%s): WARN: Unexpected error %d when testing "
                  "for direct read compatibility\n", path, errno);
        }*/
    }
    ret = 0;

done:
    destroyLocalReference(env, jStrBufferSize);
    destroyLocalReference(env, jStrReplication);
    destroyLocalReference(env, jConfiguration);
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jFile);
    if (ret) {
        if (file) {
            if (file->file) {
                (*env)->DeleteGlobalRef(env, file->file);
            }
            free(file);
        }
        errno = ret;
        return NULL;
    }
    return file;
}

int _dfsCloseFile(fsBridge fs, dfsFile file)
{
    int ret;
    // JAVA EQUIVALENT:
    //  file.close

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    if (env == NULL) {
        errno = EINTERNAL;
        return -1;
    }

    //Caught exception
    jthrowable jthr;

    //Sanity check
    if (!file || file->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    //The interface whose 'close' method to be called
    const char* interface = (file->type == INPUT) ?
        HADOOP_ISTRM : HADOOP_OSTRM;

    jthr = invokeMethod(env, NULL, INSTANCE, file->file, interface,
                     "close", "()V");
    if (jthr) {
        const char *interfaceShortName = (file->type == INPUT) ?
            "FSDataInputStream" : "FSDataOutputStream";
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "%s#close", interfaceShortName);
    } else {
        ret = 0;
    }

    //De-allocate memory
    (*env)->DeleteGlobalRef(env, file->file);
    free(file);

    if (ret) {
        errno = ret;
        return -1;
    }
    return 0;
}

tOffset _dfsTell(fsBridge fs, dfsFile f)
{
    // JAVA EQUIVALENT
    //  pos = f.getPos();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Sanity check
    if (!f || f->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    //Parameters
    jobject jStream = f->file;
    const char* interface = (f->type == INPUT) ?
        HADOOP_ISTRM : HADOOP_OSTRM;
    jvalue jVal;
    jthrowable jthr = invokeMethod(env, &jVal, INSTANCE, jStream,
                     interface, "getPos", "()J");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsTell: %s#getPos",
            ((f->type == INPUT) ? "FSDataInputStream" :
                                 "FSDataOutputStream"));
        return -1;
    }
    return jVal.j;
}

int _dfsSeek(fsBridge fs, dfsFile f, tOffset desiredPos)
{
    // JAVA EQUIVALENT
    //  fis.seek(pos);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Sanity check
    if (!f || f->type != INPUT) {
        errno = EBADF;
        return -1;
    }

    jobject jInputStream = f->file;
    jthrowable jthr = invokeMethod(env, NULL, INSTANCE, jInputStream,
            HADOOP_ISTRM, "seek", "(J)V", desiredPos);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsSeek(desiredPos=%" PRId64 ")"
            ": FSDataInputStream#seek", desiredPos);
        return -1;
    }
    return 0;
}

tSize _dfsRead(fsBridge fs, dfsFile f, void* buffer, tSize length)
{
    if (length == 0) {
        return 0;
    } else if (length < 0) {
        errno = EINVAL;
        return -1;
    }
    if (f->flags & DFS_FILE_SUPPORTS_DIRECT_READ) {
      return readDirect(fs, f, buffer, length);
    }

    // JAVA EQUIVALENT:
    //  byte [] bR = new byte[length];
    //  fis.read(bR);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Parameters
    jobject jInputStream;
    if (readPrepare(env, fs, f, &jInputStream) == -1) {
      return -1;
    }

    jbyteArray jbRarray;
    jint noReadBytes = length;
    jvalue jVal;
    jthrowable jthr;

    //Read the requisite bytes
    jbRarray = (*env)->NewByteArray(env, length);
    if (!jbRarray) {
        errno = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "_dfsRead: NewByteArray");
        return -1;
    }

    jthr = invokeMethod(env, &jVal, INSTANCE, jInputStream, HADOOP_ISTRM,
                               "read", "([B)I", jbRarray);
    if (jthr) {
        destroyLocalReference(env, jbRarray);
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsRead: FSDataInputStream#read");
        return -1;
    }
    if (jVal.i < 0) {
        // EOF
        destroyLocalReference(env, jbRarray);
        return 0;
    } else if (jVal.i == 0) {
        destroyLocalReference(env, jbRarray);
        errno = EINTR;
        return -1;
    }
    (*env)->GetByteArrayRegion(env, jbRarray, 0, noReadBytes, buffer);
    destroyLocalReference(env, jbRarray);
    if ((*env)->ExceptionCheck(env)) {
        errno = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "_dfsRead: GetByteArrayRegion");
        return -1;
    }
    return jVal.i;
}

tSize _dfsPread(fsBridge filesystem, dfsFile file, tOffset position,
                void* buffer, tSize length)
{
    JNIEnv* env;
    jbyteArray jbRarray;
    jvalue jVal;
    jthrowable jthr;

    if (length == 0) {
        return 0;
    } else if (length < 0) {
        errno = EINVAL;
        return -1;
    }
    if (!file || file->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Error checking... make sure that this file is 'readable'
    if (file->type != INPUT) {
        fprintf(stderr, "Cannot read from a non-InputStream object!\n");
        errno = EINVAL;
        return -1;
    }

    // JAVA EQUIVALENT:
    //  byte [] bR = new byte[length];
    //  fis.read(pos, bR, 0, length);
    jbRarray = (*env)->NewByteArray(env, length);
    if (!jbRarray) {
        errno = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "_dfsPread: NewByteArray");
        return -1;
    }
    jthr = invokeMethod(env, &jVal, INSTANCE, file->file, HADOOP_ISTRM,
                     "read", "(J[BII)I", position, jbRarray, 0, length);
    if (jthr) {
        destroyLocalReference(env, jbRarray);
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsPread: FSDataInputStream#read");
        return -1;
    }
    if (jVal.i < 0) {
        // EOF
        destroyLocalReference(env, jbRarray);
        return 0;
    } else if (jVal.i == 0) {
        destroyLocalReference(env, jbRarray);
        errno = EINTR;
        return -1;
    }
    (*env)->GetByteArrayRegion(env, jbRarray, 0, jVal.i, buffer);
    destroyLocalReference(env, jbRarray);
    if ((*env)->ExceptionCheck(env)) {
        errno = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "_dfsPread: GetByteArrayRegion");
        return -1;
    }
    return jVal.i;
}

tSize _dfsWrite(fsBridge fs, dfsFile f, const void* buffer, tSize length)
{
    // JAVA EQUIVALENT
    // byte b[] = str.getBytes();
    // fso.write(b);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Sanity check
    if (!f || f->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    jobject jOutputStream = f->file;
    jbyteArray jbWarray;
    jthrowable jthr;

    if (length < 0) {
    	errno = EINVAL;
    	return -1;
    }

    //Error checking... make sure that this file is 'writable'
    if (f->type != OUTPUT) {
        fprintf(stderr, "Cannot write into a non-OutputStream object!\n");
        errno = EINVAL;
        return -1;
    }

    if (length < 0) {
        errno = EINVAL;
        return -1;
    }
    if (length == 0) {
        return 0;
    }
    //Write the requisite bytes into the file
    jbWarray = (*env)->NewByteArray(env, length);
    if (!jbWarray) {
        errno = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "hdfsWrite: NewByteArray");
        return -1;
    }
    (*env)->SetByteArrayRegion(env, jbWarray, 0, length, buffer);
    if ((*env)->ExceptionCheck(env)) {
        destroyLocalReference(env, jbWarray);
        errno = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "_dfsWrite(length = %d): SetByteArrayRegion", length);
        return -1;
    }
    jthr = invokeMethod(env, NULL, INSTANCE, jOutputStream,
            HADOOP_OSTRM, "write", "([B)V", jbWarray);
    destroyLocalReference(env, jbWarray);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsWrite: FSDataOutputStream#write");
        return -1;
    }
    // Unlike most Java streams, FSDataOutputStream never does partial writes.
    // If we succeeded, all the data was written.
    return length;
}

int _dfsFlush(fsBridge fs, dfsFile f)
{
    // JAVA EQUIVALENT
    //  fos.flush();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Sanity check
    if (!f || f->type != OUTPUT) {
        errno = EBADF;
        return -1;
    }
    jthrowable jthr = invokeMethod(env, NULL, INSTANCE, f->file,
                     HADOOP_OSTRM, "flush", "()V");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsFlush: FSDataInputStream#flush");
        return -1;
    }
    return 0;
}

int _dfsHFlush(fsBridge fs, dfsFile f)
{
    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Sanity check
    if (!f || f->type != OUTPUT) {
        errno = EBADF;
        return -1;
    }

    jobject jOutputStream = f->file;
    jthrowable jthr = invokeMethod(env, NULL, INSTANCE, jOutputStream,
                     HADOOP_OSTRM, "hflush", "()V");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsHFlush: FSDataOutputStream#hflush");
        return -1;
    }
    return 0;
}

int _dfsHSync(fsBridge fs, dfsFile f)
{
    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Sanity check
    if (!f || f->type != OUTPUT) {
        errno = EBADF;
        return -1;
    }

    jobject jOutputStream = f->file;
    jthrowable jthr = invokeMethod(env, NULL, INSTANCE, jOutputStream,
                     HADOOP_OSTRM, "hsync", "()V");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsHSync: FSDataOutputStream#hsync");
        return -1;
    }
    return 0;
}

int _dfsAvailable(fsBridge fs, dfsFile f)
{
    // JAVA EQUIVALENT
    //  fis.available();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Sanity check
    if (!f || f->type != INPUT) {
        errno = EBADF;
        return -1;
    }

    //Parameters
    jobject jInputStream = f->file;
    jvalue jVal;
    jthrowable jthr = invokeMethod(env, &jVal, INSTANCE, jInputStream,
                     HADOOP_ISTRM, "available", "()I");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "_dfsAvailable: FSDataInputStream#available");
        return -1;
    }
    return jVal.i;
}


















