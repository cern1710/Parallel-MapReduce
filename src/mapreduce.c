#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "hashmap.h"
#include "mapreduce.h"

#define MAPS_NUM 100
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
pthread_t *map_threads;
pthread_t *reduce_threads;

// global memory
size_t *kvl_count;
int map_workers = 0;
int reduce_workers = 0;
Partitioner partition_func; // pointer to partition function

KVList* initKVlist(size_t size) {
    KVList *list = (KVList*)malloc(sizeof(KVList));

    CHECK_MALLOC(list);

    list->kvp = (KVpair**) malloc(size * sizeof(KVpair*));

    if (list->kvp == NULL) {
        free(list);
        CHECK_MALLOC(list->kvp);
    }

    list->num_kvp = 0;
    list->size = size;

    return list;
}


void initParams(int num_mappers, int num_reducers, Partitioner partition) {
    int i;

    // hash KVP
    for (i=0; i<num_reducers; i++) {
        hashKVP[i] = initKVlist(MAPS_NUM/10);
        pthread_mutex_init(&locks[i], NULL);
    }

    // kvl counter
    kvl_count = malloc(sizeof(size_t) * num_reducers);
    for (i=0; i<num_reducers; i++)
        kvl_count[i] = 0;
    
    // map threads and reduce threads
    map_threads = malloc(sizeof(pthread_t) * num_mappers);
    reduce_threads = malloc(sizeof(pthread_t) * num_reducers);
    for (i=0; i<num_mappers; i++) {
        map_threads[i] = -1;
    }

    // partition function
    partition_func = (partition == NULL)
                        ? &MR_DefaultHashPartition : partition;
    
    // reduce workers
    reduce_workers = num_reducers;
}


void add(KVList *list, KVpair* kvp) {
    // resize the list if number of KVPs is reached
    if (list->num_kvp == list-> size) {
        list->size *= 2;
        list->kvp = realloc(list->kvp, list->size * sizeof(KVpair*));
    }

    list->kvp[list->num_kvp++] = kvp;
}


char* get(char* key, int partition_num) {
    KVList *list = hashKVP[partition_num];

    size_t cur_count = kvl_count[partition_num];

    // if current count has reached max number of KVPs, key is not inside
    if (cur_count == list->num_kvp) 
        return NULL;

    KVpair *cur_kvp = list->kvp[cur_count];

    // if current key-value pair does not match
    if (strcmp(cur_kvp->key, key) != 0)
        return NULL;
    
    kvl_count[partition_num] += 1;
    return cur_kvp->value;
}


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


void reducePartition(getterParams *params) {
    Reducer reduce = params->reduceFunc;

    pthread_mutex_lock(&locks[params->partNum]);

    KVList *partition = hashKVP[params->partNum];
    char *prev = " ";
    int keys_found = 0, i=0;

    // iterate through all key-value pairs in current partition
    for (; i<partition->num_kvp; i++) {
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


void createMapThreads(char *argv[], int start, int end, Mapper map) {
    int i;
    for (i=start; i<end; i++) {
        if (strstr(argv[i], ".") == NULL) continue;
        pthread_create(&map_threads[i-start], NULL, (void*)(*map), argv[i]);
    }
    for (i=start; i<end; i++) {
        if (map_threads[i-start] != -1) {
            pthread_join(map_threads[i-start], NULL);
        }
    }
}


int cmp(const void* a, const void* b) {
    char *str1, *str2;
    str1 = (*(KVpair**)a)->key;
    str2 = (*(KVpair**)b)->key;
    return strcmp(str1, str2);
}


void sortHashKVP(int num_reducers) {
    int i;
    KVList *cur_kvp;
    for (i=0; i<num_reducers; i++) {
        cur_kvp = hashKVP[i];
        if (cur_kvp != NULL && cur_kvp->num_kvp > 0) {
            qsort(cur_kvp->kvp, cur_kvp->num_kvp, sizeof(KVpair*), cmp);
        }
    }
}


void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, 
	    Reducer reduce, int num_reducers, Partitioner partition) {
    int i, num_files = argc-1, files_processed = 0;
    int start, end;
    pthread_t thread_id;

    initParams(num_mappers, num_reducers, partition);

    if (num_files <= num_mappers) { // each file has its own mapper thread
        createMapThreads(argv, 1, argc, map);
    }
    else {    // imbalance workload
        files_processed = 0;
        while (files_processed < num_files) {
            end = num_mappers + (start = files_processed+1);

            if (end > argc)
                end = argc;
            
            createMapThreads(argv, start, end, map);
            files_processed += end - start;
        }
    }

    sortHashKVP(num_reducers);
} // MR_Run()