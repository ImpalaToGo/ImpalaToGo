/*
 * @file replacement-for-hdfs-inherited.h
 * @brief declare some utility methods (basically, currently only for getting JNI Env reference from current runtime)
 * that were inherited from libhdfs which is now unbound from Impala directly, at least, from util layer.
 *
 * @date   Oct 12, 2014
 * @author elenav
 */

#ifndef REPLACEMENT_FOR_HDFS_INHERITED_H_
#define REPLACEMENT_FOR_HDFS_INHERITED_H_

#include <jni.h>
#include <stdio.h>

#include <stdlib.h>
#include <stdarg.h>
#include <search.h>

#include <pthread.h>

#pragma GCC diagnostic ignored "-Wwrite-strings"

/** @namespace replacement_for_hdfs
 *  @brief we need to put borrowed from hdfs stuuf into namespace and decorate usage
 *  as we do not want conflicts when hdfs pulgin will be really linked in
 */

pthread_mutex_t hashTableMutex = PTHREAD_MUTEX_INITIALIZER;

#define LOCK_HASH_TABLE() pthread_mutex_lock(&hashTableMutex)
#define UNLOCK_HASH_TABLE() pthread_mutex_unlock(&hashTableMutex)

/**
 * MAX_HASH_TABLE_ELEM: The maximum no. of entries in the hashtable.
 * It's set to 4096 to account for (classNames + No. of threads)
 */
#define MAX_HASH_TABLE_ELEM 4096

void hashTableInit()
{
    static int hash_table_inited = 0;
    LOCK_HASH_TABLE();
    if(!hash_table_inited) {
        if (hcreate(MAX_HASH_TABLE_ELEM) == 0) {
            fprintf(stderr,"hcreate returned error\n");
            exit(1);
        }
        hash_table_inited = 1;
    }
    UNLOCK_HASH_TABLE();
}

void *searchEntryFromTable(const char *key)
{
    ENTRY e,*ep;
    if(key == NULL) {
        return NULL;
    }
    hashTableInit();
    e.key = (char*)key;
    LOCK_HASH_TABLE();
    ep = hsearch(e, FIND);
    UNLOCK_HASH_TABLE();
    if(ep != NULL) {
        return ep->data;
    }
    return NULL;
}



#endif /* REPLACEMENT_FOR_HDFS_INHERITED_H_ */
