#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include "mapreduce.h"

#define CHECK_MALLOC(ptr) \
    do { \
        if ((ptr) == NULL) { \
            perror("ERROR: Failed to allocate memory.\n"); \
            exit(EXIT_FAILURE); \
        } \
    } while(0)


MRContext* getContext(void) {
    static MRContext *instance = NULL;

    if (instance == NULL)
        instance = (MRContext*)malloc(sizeof(MRContext));

    return instance;
}


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
    MRContext *ctx = getContext();
    // hash KVP
    for (i=0; i<num_reducers; i++) {
        ctx->hashKVP[i] = initKVlist(MAPS_NUM/10);
        pthread_mutex_init(&ctx->locks[i], NULL);
    }

    // kvl counter
    ctx->kvl_count = malloc(sizeof(size_t) * num_reducers);
    for (i=0; i<num_reducers; i++)
        ctx->kvl_count[i] = 0;
    
    // map threads and reduce threads
    ctx->map_threads = malloc(sizeof(pthread_t) * num_mappers);
    ctx->thread_used = malloc(sizeof(int) * num_mappers);
    ctx->reduce_threads = malloc(sizeof(pthread_t) * num_reducers);
    for (i=0; i<num_mappers; i++)
        ctx->thread_used[i] = 0;

    // partition function
    ctx->partition_func = (partition == NULL)
                        ? &MR_DefaultHashPartition : partition;
    
    // reduce workers
    ctx->reduce_workers = num_reducers;
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
    MRContext *ctx = getContext();
    KVList *list = ctx->hashKVP[partition_num];

    size_t cur_count = ctx->kvl_count[partition_num];

    // if current count has reached max number of KVPs, key is not inside
    if (cur_count == list->num_kvp) 
        return NULL;

    KVpair *cur_kvp = list->kvp[cur_count];

    // if current key-value pair does not match
    if (strcmp(cur_kvp->key, key) != 0)
        return NULL;
    
    ctx->kvl_count[partition_num] += 1;
    return cur_kvp->value;
}


void MR_Emit(char *key, char *value) {
    MRContext *ctx = getContext();
    KVpair *temp = (KVpair*) malloc(sizeof(KVpair));
    CHECK_MALLOC(temp);

    int partition_num = (*ctx->partition_func)(key, ctx->reduce_workers);

    char *temp_key = strdup(key);
    char *temp_val = strdup(value);
    temp->key = temp_key;
    temp->value = temp_val;

    pthread_mutex_lock(&ctx->locks[partition_num]);
    add(ctx->hashKVP[partition_num], temp);
    pthread_mutex_unlock(&ctx->locks[partition_num]);
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


void shuffleAndSort(KVList *hashKVP[], int num_reducers) {
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


void joinThreads(MRContext *ctx, int num_threads) {
    pthread_t *map_threads = ctx->map_threads;
    int i;
    for (i=0; i<num_threads; i++) {
        if (ctx->thread_used[i] == 1) {
            pthread_join(map_threads[i], NULL);
            ctx->thread_used[i] = 0;
        }
    }
}


void mapThreads(int argc, char *argv[], Mapper map, 
                int num_mappers, int num_reducers) {
    MRContext *ctx = getContext();
    int num_files = argc - 1, file_index = 0, loop, i;

    if (num_files <= num_mappers) {
        for (i=1; i<argc; i++) {
            if (strstr(argv[i], ".") == NULL)
                continue;
            createThread(&ctx->map_threads[i-1], map, argv[i]);
            ctx->thread_used[i-1] = 1;
            joinThreads(ctx, i);
        }
    } else {
        while (num_files > 0) {
            loop = (num_files >= num_mappers) ? 
                    num_mappers : num_files;

            for (i=0; i<loop; i++) {
                createThread(&ctx->map_threads[i], 
                                map, argv[file_index]);
                ctx->thread_used[i] = 1;
                file_index++;
            }

            joinThreads(ctx, loop);
            num_files -= num_mappers;
        }
    }

    shuffleAndSort(ctx->hashKVP, num_reducers);
}


void reducePartition(getterParams *params) {
    MRContext *ctx = getContext();
    Reducer red = params->reduce_func;

    pthread_mutex_lock(&ctx->locks[params->partition_num]);
    char *prev = " ";
    int keys_found = 0, i;
    KVList *partition = ctx->hashKVP[params->partition_num];

    // iterate through all key-value pairs in current partition
    for (i=0; i<partition->num_kvp; i++) {
        if (strcmp(partition->kvp[i]->key, prev) == 0) {
            continue;
        }
        keys_found++;

        // reduce current key
        (*red)(partition->kvp[i]->key, params->get_func, params->partition_num);
        prev = partition->kvp[i]->key;
    }

    ctx->kvl_count[params->partition_num] = 0;
    pthread_mutex_unlock(&ctx->locks[params->partition_num]);
}


void reduceThreads(Reducer reduce, int num_reducers) {
    MRContext *ctx = getContext();
    pthread_t thread_id;
    int i, hashKVP_count = 0, paramsIndex[num_reducers];
    getterParams *paramsList[num_reducers];

    for (i=0;i<num_reducers;i++)
        paramsIndex[i] = -1;

    for (i=0; i<num_reducers; i++) {
        if (ctx->hashKVP[i] != NULL && ctx->hashKVP[i]->num_kvp > 0) {
            getterParams *temp = (getterParams*)malloc(sizeof(getterParams));
            CHECK_MALLOC(temp);
            temp->get_func = get;
            temp->reduce_func = reduce;
            temp->partition_num = (*ctx->partition_func)
                (ctx->hashKVP[i]->kvp[0]->key, ctx->reduce_workers);
            paramsList[i] = temp;
            paramsIndex[i] = 1;
        }
    }

    for (i=0; i<num_reducers; i++) {
        pthread_mutex_lock(&ctx->locks[i]);
        if (ctx->hashKVP[i] != NULL && ctx->hashKVP[i]->num_kvp > 0) {
            pthread_create(&thread_id, NULL, (void*)(*reducePartition), 
                            paramsList[i]);
            ctx->reduce_threads[hashKVP_count++] = thread_id;
        }
        pthread_mutex_unlock(&ctx->locks[i]);
    }

    for (i=0; i<hashKVP_count; i++)
        pthread_join(ctx->reduce_threads[i], NULL);

    for (i=0; i<num_reducers; i++)
        if (paramsIndex[i] != -1)
            free(paramsList[i]);

} // reduceThreads()


void freeMR(int num_mappers, int num_reducers) {
    MRContext *ctx = getContext();
    int i, j;

    free(ctx->kvl_count);

    for (i=0; i<MAPS_NUM; i++) {
        if (ctx->hashKVP[i] != NULL) {
            for (j=0; j<ctx->hashKVP[i]->num_kvp; j++) {
                free(ctx->hashKVP[i]->kvp[j]->key);
                free(ctx->hashKVP[i]->kvp[j]->value);
                free(ctx->hashKVP[i]->kvp[j]);
            }
            free(ctx->hashKVP[i]->kvp);
            free(ctx->hashKVP[i]);
        }
    }

    for (i=0; i<num_reducers; i++)
        pthread_mutex_destroy(&ctx->locks[i]);

    free(ctx->map_threads);
    free(ctx->reduce_threads);
    free(ctx->thread_used);
    free(ctx);
}


void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, 
	    Reducer reduce, int num_reducers, Partitioner partition) {
    initParams(num_mappers, num_reducers, partition);
    mapThreads(argc, argv, map, num_mappers, num_reducers);
    reduceThreads(reduce, num_reducers);
    freeMR(num_mappers, num_reducers);
} // MR_Run()