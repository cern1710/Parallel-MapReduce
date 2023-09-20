#ifndef MAPREDUCE_H
#define MAPREDUCE_H

#include <pthread.h>
#include "hashmap.h"
#define MAPS_NUM 100

// different function pointer types used by MR
typedef char *(*Getter)(char *key, int partition_number);
typedef void (*Mapper)(char *file_name);
typedef void (*Reducer)(char *key, Getter get_func, int partition_number);
typedef unsigned long (*Partitioner)(char *key, int num_partitions);

typedef struct KVpair_ {
    char *key;
    char *value;
} KVpair;

typedef struct KVList_ {
    KVpair** kvp;
    size_t num_kvp;
    size_t size;
} KVList;

typedef struct getterParams_ {
    Getter get_func;
    Reducer reduce_func;
    int partition_num;
} getterParams;

typedef struct MRContext_ {
    HashMap *maps[MAPS_NUM];
    KVList *hashKVP[MAPS_NUM];
    pthread_t *map_threads;
    pthread_t *reduce_threads;
    pthread_mutex_t locks[MAPS_NUM];
    Partitioner partition_func; // pointer to partition function
    int map_workers;
    int reduce_workers;
    char *thread_used;
    size_t *kvl_count;
} MRContext;

// external functions
void MR_Emit(char *key, char *value);

unsigned long MR_DefaultHashPartition(char *key, int num_partitions);

void MR_Run(int argc, char *argv[], 
	    Mapper map, int num_mappers, 
	    Reducer reduce, int num_reducers, 
	    Partitioner partition);

#endif // MAPREDUCE_H