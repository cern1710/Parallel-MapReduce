#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include "mapreduce.h"

HashMap *maps[MAPS_NUM];
KVList *hashKVP[MAPS_NUM];
pthread_mutex_t locks[MAPS_NUM];

Partitioner partition_func; // pointer to partition function
int map_workers = 0;
int reduce_workers = 0;

pthread_t *map_threads;
char *thread_used;
pthread_t *reduce_threads;
size_t *kvl_count;

#define CHECK_MALLOC(ptr) \
    do { \
        if ((ptr) == NULL) { \
            perror("ERROR: Failed to allocate memory.\n"); \
            exit(EXIT_FAILURE); \
        } \
    } while(0)


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


void initParams(int num_mappers, int num_reducers, 
                Partitioner partition) {
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
    thread_used = malloc(sizeof(char) * num_mappers);
    reduce_threads = malloc(sizeof(pthread_t) * num_reducers);
    for (i=0; i<num_mappers; i++) {
        thread_used[i] = '0';
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

    // assumes this does not crash
    temp->key = strdup(key);
    temp->value = strdup(value);

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


void createThread(pthread_t *thread_id, Mapper map, char *file_name) {
    if (strstr(file_name, ".") != NULL)
        pthread_create(thread_id, NULL, (void *)(*map), file_name);
}


void joinThreads(pthread_t map_threads[], int num_threads) {
    int i;
    for (i=0; i<num_threads; i++) {
        if (thread_used[i] == '1') {
            pthread_join(map_threads[i], NULL);
            thread_used[i] = '0';
        }
    }
}


void mapThreads(int argc, char *argv[], Mapper map, 
                int num_mappers, int num_reducers) {
    int num_files = argc - 1, file_index = 0, loop, i;

    if (num_files <= num_mappers) {
        for (i=1; i<argc; i++) {
            if (strstr(argv[i], ".") == NULL)
                continue;
            createThread(&map_threads[i-1], map, argv[i]);
            thread_used[i-1] = '1';
            joinThreads(map_threads, i);
        }
    } else {
        while (num_files > 0) {
            loop = (num_files >= num_mappers) ? 
                    num_mappers : num_files;

            for (i=0; i<loop; i++) {
                createThread(&map_threads[i], map, argv[file_index]);
                thread_used[i] = '1';
                file_index++;
            }

            joinThreads(map_threads, loop);
            num_files -= num_mappers;
        }
    }

    sortHashKVP(hashKVP, num_reducers);
}


void reducePartition(getterParams *params) {
    Reducer red = params->reduceFunc;

    pthread_mutex_lock(&locks[params->partNum]);
    char *prev = " ";
    int keys_found = 0, i;
    KVList *partition = hashKVP[params->partNum];

    // iterate through all key-value pairs in current partition
    for (i=0; i<partition->num_kvp; i++) {
        if (strcmp(partition->kvp[i]->key, prev) == 0) {
            continue;
        }
        keys_found++;

        // reduce current key
        (*red)(partition->kvp[i]->key, params->getFunc, params->partNum);
        prev = partition->kvp[i]->key;
    }

    kvl_count[params->partNum] = 0;
    pthread_mutex_unlock(&locks[params->partNum]);
}


void reduceThreads(Reducer reduce, int num_reducers) {
    pthread_t thread_id;
    int i, hashKVP_count = 0, paramsIndex[num_reducers];
    getterParams *paramsList[num_reducers];

    for (i=0;i<num_reducers;i++)
        paramsIndex[i] = -1;

    for (i=0; i<num_reducers; i++) {
        if (hashKVP[i] != NULL && hashKVP[i]->num_kvp > 0) {
            getterParams *temp = (getterParams*)malloc(sizeof(getterParams));
            CHECK_MALLOC(temp);
            temp->getFunc = get;
            temp->reduceFunc = reduce;
            temp->partNum = (*partition_func)
                (hashKVP[i]->kvp[0]->key, reduce_workers);
            paramsList[i] = temp;
            paramsIndex[i] = 1;
        }
    }

    for (i=0; i<num_reducers; i++) {
        pthread_mutex_lock(&locks[i]);
        if (hashKVP[i] != NULL && hashKVP[i]->num_kvp > 0) {
            pthread_create(&thread_id, NULL, (void*)(*reducePartition), 
                            paramsList[i]);
            reduce_threads[hashKVP_count++] = thread_id;
        }
        pthread_mutex_unlock(&locks[i]);
    }

    for (i=0; i<hashKVP_count; i++)
        pthread_join(reduce_threads[i], NULL);
} // reduceThreads()


void freeMR(int num_mappers, int num_reducers) {
    int i, j;

    free(kvl_count);

    for (i=0; i<MAPS_NUM; i++) {
        if (hashKVP[i] != NULL) {
            for (j=0; j<hashKVP[i]->num_kvp; j++) {
                free(hashKVP[i]->kvp[j]->key);
                free(hashKVP[i]->kvp[j]->value);
                free(hashKVP[i]->kvp[j]);
            }
            free(hashKVP[i]->kvp);
            free(hashKVP[i]);
        }
    }

    for (i=0; i<num_reducers; i++) {
        pthread_mutex_destroy(&locks[i]);
    }

    // need to free map_threads, reduce_threads, paramsList
}


void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, 
	    Reducer reduce, int num_reducers, Partitioner partition) {
    initParams(num_mappers, num_reducers, partition);
    mapThreads(argc, argv, map, num_mappers, num_reducers);
    reduceThreads(reduce, num_reducers);
    freeMR(num_mappers, num_reducers);
} // MR_Run()