#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "hashmap.h"
#include "mapreduce.h"

#define MAPS_NUM 1000
#define CHECK_MALLOC(ptr) \
    do { \
        if ((ptr) == NULL) { \
            perror("ERROR: Failed to allocate memory.\n"); \
            exit(EXIT_FAILURE); \
        } \
    } while(0)

// global arrays
HashMap *maps[MAPS_NUM];
KVList *hashKVP[MAPS_NUM];
pthread_mutex_t locks[MAPS_NUM];

// global memory
size_t *kvl_count;
int map_workers = 0;
int reduce_workers = 0;
Partitioner partition_func; // pointer to partition function

KVList* init_kvlist(size_t size) {
    KVList *list = (KVList*)malloc(sizeof(KVList));

    CHECK_MALLOC(list);

    list->kvp = (KVpair**) malloc(size * sizeof(KVpair*));

    if (list->kvp == NULL) {
        free(list);
        CHECK_MALLOC(list->kvp);
    }

    list->capacity = 0;
    list->size = size;

    return list;
} // init_kvlist()


void add(KVList *list, KVpair* kvp) {
    // resize the list if capacity is reached
    if (list->capacity == list-> size) {
        list->size *= 2;
        list->kvp = realloc(list->kvp, list->size * sizeof(KVpair*));
    }

    list->kvp[list->capacity++] = kvp;
} // add()


char* get(char* key, int partition_num) {
    KVList *list = hashKVP[partition_num];

    size_t cur_count = kvl_count[partition_num];

    // if current count ahs reached max capacity, key is not inside
    if (cur_count == list->capacity) 
        return NULL;

    KVpair *cur_kvp = list->kvp[cur_count];

    // if current key-value pair does not match
    if (strcmp(cur_kvp->key, key) != 0)
        return NULL;
    
    kvl_count[partition_num] += 1;
    return cur_kvp->value;
} // get()


void MR_Emit(char *key, char *value) {
    KVpair *temp = (KVpair*) malloc(sizeof(KVpair));
    CHECK_MALLOC(temp);

    int partition_num = (*partition_func)(key, reduce_workers);

    char* key_temp = strdup(key);
    // if (key_temp == NULL) {
    //     free(temp);
    //     CHECK_MALLOC(key_temp);
    // }

    char* value_temp = strdup(value);
    // if (value_temp == NULL) {
    //     free(key_temp);
    //     free(temp);
    //     CHECK_MALLOC(value_temp);
    // }

    temp->key = key_temp;
    temp->value = value_temp;

    pthread_mutex_lock(&locks[partition_num]);
    add(hashKVP[partition_num], temp);
    pthread_mutex_unlock(&locks[partition_num]);
} // MR_Emit()


// Uses the djb2 hash algorithm
unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;

    while ((c = *key++) != '\0') {
        hash = hash*33 + c;
    }

    return (hash % num_partitions);
} // MR_DefaultHashPartition()


void reduce_partition(getterParams *params) {
    Reducer reduce = params->reduceFunc;

    pthread_mutex_lock(&locks[params->partNum]);

    KVList *partition = hashKVP[params->partNum];
    char *prev = " ";
    int keys_found = 0, i=0;

    // iterate through all key-value pairs in current partition
    for (; i<partition->capacity; i++) {
        if (strcmp(partition->kvp[i]->key, prev) == 0) {
            continue;
        }
        keys_found++;

        // reduce current key
        (*reduce)(partition->kvp[i]->key, params->getFunc, params->partNum);
        prev = partition->kvp[i]->key;
    }
    kvl_count[params->partNum] = 0;

    pthread_mutex_unlock(&locks[params->partNum]);
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, 
	    Reducer reduce, int num_reducers, Partitioner partition) {
    int i=0, l=0;

    for (; i<num_reducers; i++) {
        
    }
} // MR_Run()