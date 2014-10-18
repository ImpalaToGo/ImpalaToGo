/*
 * @file  replacement-for-hdfs-inherited.cc
 * @brief just emulate the libhdfs facilities related to acquiring JNI Env
 *
 * @date Oct 13, 2014
 * @author elenav
 */

#include <string.h>
#include "util/replacement-for-hdfs-inherited.h"

#define threadID_SIZE 32

#define GET_threadID(threadID, key, keySize) \
    snprintf(key, keySize, "__hdfs_threadID__%u", (unsigned)(threadID));

/**
 * getJNIEnv: A helper function to get the JNIEnv* for the given thread.
 * @param: None.
 * @return The JNIEnv* corresponding to the thread.
 */
JNIEnv* getJNIEnv()
{
    char threadID[threadID_SIZE];

    const jsize vmBufLength = 1;

    // container for JavaVM
    JavaVM* vmBuf[vmBufLength];
    JNIEnv *env;
    jint rv = 0;
    jint noVMs = 0;

    //Get the threadID and stringize it
    GET_threadID(pthread_self(), threadID, sizeof(threadID));

    //See if you already have the JNIEnv* cached...
    env = (JNIEnv*)searchEntryFromTable(threadID);
    if (env != NULL) {
        return env;
    }

    //All right... some serious work required here!
    //1. Initialize the HashTable
    //2. LOCK!
    //3. Check if any JVMs have been created here
    //      Yes: Use it (we should only have 1 VM)
    //      No: Create the JVM
    //4. UNLOCK

    hashTableInit();

    LOCK_HASH_TABLE();

    rv = JNI_GetCreatedJavaVMs(&(vmBuf[0]), vmBufLength, &noVMs);
    if (rv != 0) {
        fprintf(stderr,
                "Call to JNI_GetCreatedJavaVMs failed with error: %d\n", rv);
        exit(1);
    }

    if (noVMs == 0) { // no running Java machines found
        // Get the environment variables for initializing the JVM
        char *classPath = getenv("CLASSPATH");
        if (classPath == NULL) {
        		fprintf(stderr, "Please set the environment variable $CLASSPATH!\n");
        		exit(-1);
        }
        char *classPathVMArg = "-Djava.class.path=";
        size_t optClassPathLen = strlen(classPath) +
        								strlen(classPathVMArg) + 1;
        char *optClassPath = (char*)malloc(sizeof(char) * optClassPathLen);
        snprintf(optClassPath, optClassPathLen,
        	"%s%s", classPathVMArg, classPath);

        //Create the VM
        JavaVMInitArgs vm_args;
        JavaVMOption options[1];
        JavaVM *vm;

        // User classes
        options[0].optionString = optClassPath;
        // Print JNI-related messages
        //options[2].optionString = "-verbose:jni";

        vm_args.version = JNI_VERSION_1_2;
        vm_args.options = options;
        vm_args.nOptions = 1;
        vm_args.ignoreUnrecognized = 1;

        // run new Java Machine
        rv = JNI_CreateJavaVM(&vm, (void**)&env, &vm_args);
        if (rv != 0) {
            fprintf(stderr,
                    "Call to JNI_CreateJavaVM failed with error: %d\n", rv);
            exit(1);
        }

        free(optClassPath);
    } else {
        // Attach this thread to the VM
        JavaVM* vm = vmBuf[0];
        // rv = (*vm)->AttachCurrentThread(vm, (void**)&env, 0);
        rv = vm->AttachCurrentThread((void**)&env, NULL);
        if (rv != 0) {
            fprintf(stderr,
                    "Call to AttachCurrentThread failed with error: %d\n", rv);
            exit(1);
        }
    }

    //Save the threadID -> env mapping
    ENTRY e, *ep;
    e.key = threadID;
    e.data = (void*)(env);
    if ((ep = hsearch(e, ENTER)) == NULL) {
        fprintf(stderr, "Call to hsearch(ENTER) failed\n");
        exit(1);
    }

    UNLOCK_HASH_TABLE();

    return env;
}


