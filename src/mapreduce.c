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

// context struct to store global variables
typedef struct {
    HashMap *maps[MAPS_NUM];
    KVList *hashKVP[MAPS_NUM];
    pthread_mutex_t locks[MAPS_NUM];
    pthread_t *map_threads;
    pthread_t *reduce_threads;
    getterParams **paramsList;
    int *paramsIndex;
    size_t *kvl_count;
    int map_workers;            // automatically initialised to 0
    int reduce_workers;         // automatically initialised to 0
    Partitioner partition_func; // pointer to partition function
} MR_Context;


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


void initParams(MR_Context context, int num_mappers, int num_reducers, 
                Partitioner partition) {
    int i;

    // hash KVP
    for (i=0; i<num_reducers; i++) {
        context.hashKVP[i] = initKVlist(MAPS_NUM/10);
        pthread_mutex_init(&context.locks[i], NULL);
    }

    // kvl counter
    context.kvl_count = malloc(sizeof(size_t) * num_reducers);
    for (i=0; i<num_reducers; i++)
        context.kvl_count[i] = 0;
    
    // map threads and reduce threads
    context.map_threads = malloc(sizeof(pthread_t) * num_mappers);
    context.reduce_threads = malloc(sizeof(pthread_t) * num_reducers);
    for (i=0; i<num_mappers; i++)
        context.map_threads[i] = -1;

    // partition function
    context.partition_func = (partition == NULL)
                        ? &MR_DefaultHashPartition : partition;
    
    // getter params
    // paramsList = malloc(sizeof(getterParams) * num_reducers);
    context.paramsIndex = malloc(sizeof(int) * num_reducers);
    for (i=0; i<num_reducers; i++)
        context.paramsIndex[i] = -1;
    
    // reduce workers
    context.reduce_workers = num_reducers;
}


void add(KVList *list, KVpair* kvp) {
    // resize the list if number of KVPs is reached
    if (list->num_kvp == list-> size) {
        list->size *= 2;
        list->kvp = realloc(list->kvp, list->size * sizeof(KVpair*));
    }

    list->kvp[list->num_kvp++] = kvp;
}


char* get(MR_Context context, char* key, int partition_num) {
    KVList *list = context.hashKVP[partition_num];

    size_t cur_count = context.kvl_count[partition_num];

    // if current count has reached max number of KVPs, key is not inside
    if (cur_count == list->num_kvp) 
        return NULL;

    KVpair *cur_kvp = list->kvp[cur_count];

    // if current key-value pair does not match
    if (strcmp(cur_kvp->key, key) != 0)
        return NULL;
    
    context.kvl_count[partition_num] += 1;
    return cur_kvp->value;
}


void MR_Emit(MR_Context context, char *key, char *value) {
    KVpair *temp = (KVpair*) malloc(sizeof(KVpair));
    CHECK_MALLOC(temp);

    int partition_num = (*context.partition_func)(key, context.reduce_workers);

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

    pthread_mutex_lock(&context.locks[partition_num]);
    add(context.hashKVP[partition_num], temp);
    pthread_mutex_unlock(&context.locks[partition_num]);
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


int cmp(const void* a, const void* b) {
    char *str1, *str2;

    str1 = (*(KVpair**)a)->key;
    str2 = (*(KVpair**)b)->key;
    return strcmp(str1, str2);
}


void sortHashKVP(KVList *hashKVP[], int num_reducers) {
    int i;
    KVList *cur_kvp;

    for (i=0; i<num_reducers; i++) {
        cur_kvp = hashKVP[i];
        if (cur_kvp != NULL && cur_kvp->num_kvp > 0) {
            qsort(cur_kvp->kvp, cur_kvp->num_kvp, sizeof(KVpair*), cmp);
        }
    }
}


void createMapThreads(MR_Context context, char *argv[], 
                        int start, int end, Mapper map) {
    int i;

    for (i=start; i<end; i++) {
        if (strstr(argv[i], ".") == NULL) 
            continue;
        pthread_create(&context.map_threads[i-start], 
                            NULL, (void*)(*map), argv[i]);
    }
    for (i=start; i<end; i++) {
        if (context.map_threads[i-start] != -1)
            pthread_join(context.map_threads[i-start], NULL);
    }
} // createMapThreads()


void mapThreads(MR_Context context, int argc, char *argv[], Mapper map,
                int num_mappers, int num_reducers) {
    int num_files = argc-1;
    int files_processed=0, start, end;

    if (num_files <= num_mappers) { // each file has its own mapper thread
        createMapThreads(context, argv, 1, argc, map);
    } else {    // imbalance workload
        files_processed = 0;
        while (files_processed < num_files) {
            end = num_mappers + (start = files_processed+1);

            if (end > argc)
                end = argc;
            
            createMapThreads(context, argv, start, end, map);
            files_processed += end - start;
        }
    }

    sortHashKVP(context.hashKVP ,num_reducers);
} // mapThreads()


void reducePartition(MR_Context context, getterParams *params) {
    Reducer reduce = params->reduceFunc;

    pthread_mutex_lock(&context.locks[params->partNum]);
    char *prev = " ";
    int keys_found = 0, i;
    KVList *partition = context.hashKVP[params->partNum];

    // iterate through all key-value pairs in current partition
    for (i=0; i<partition->num_kvp; i++) {
        if (strcmp(partition->kvp[i]->key, prev) == 0) {
            continue;
        }
        keys_found++;

        // reduce current key
        (*reduce)(partition->kvp[i]->key, params->getFunc, params->partNum);
        prev = partition->kvp[i]->key;
    }

    context.kvl_count[params->partNum] = 0;
    pthread_mutex_unlock(&context.locks[params->partNum]);
}


void reduceThreads(MR_Context context, Reducer reduce, int num_reducers) {
    pthread_t thread_id;
    int i, hashKVP_count = 0;
    getterParams *temp = (getterParams*)malloc(sizeof(getterParams));

    for (i=0; i<num_reducers; i++) {
        if (context.hashKVP[i] != NULL && context.hashKVP[i]->num_kvp > 0) {
            temp->getFunc = get;
            temp->reduceFunc = reduce;
            temp->partNum = (*context.partition_func)
                (context.hashKVP[i]->kvp[0]->key, context.reduce_workers);
            context.paramsList[i] = temp;
            context.paramsIndex[i] = 1;
        }
    }

    for (i=0; i<num_reducers; i++) {
        pthread_mutex_lock(&context.locks[i]);
        if (context.hashKVP[i] == NULL || context.hashKVP[i]->num_kvp <= 0)
            continue;
        
        pthread_create(&thread_id, NULL, (void*)(*reducePartition), 
                        context.paramsList[i]);
        context.reduce_threads[hashKVP_count++] = thread_id;
        ptread_mutex_unlock(&context.locks[i]);
    }

    for (i=0; i<hashKVP_count; i++) {
        pthread_join(context.reduce_threads[i], NULL);
    }
} // reduceThreads()


void freeMR(MR_Context context, int num_reducers) {
    int i, j;

    free(context.kvl_count);

    for (i=0; i<MAPS_NUM; i++) {
        if (context.hashKVP[i] != NULL) {
            for (j=0; j<context.hashKVP[i]->num_kvp; j++) {
                free(context.hashKVP[i]->kvp[j]->key);
                free(context.hashKVP[i]->kvp[j]->value);
                free(context.hashKVP[i]->kvp[j]);
            }
            free(context.hashKVP[i]->kvp);
            free(context.hashKVP[i]);
        }
    }

    for (i=0; i<num_reducers; i++) {
        if (context.paramsIndex[i] != -1)
            free(context.paramsIndex[i]);
    }
}


void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, 
	    Reducer reduce, int num_reducers, Partitioner partition) {
    MR_Context context;
    initParams(context, num_mappers, num_reducers, partition);
    mapThreads(context, argc, argv, map, num_mappers, num_reducers);
    reduceThreads(context, reduce, num_reducers);
    freeMR(context, num_reducers);
} // MR_Run()