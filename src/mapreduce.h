#ifndef MAPREDUCE_H
#define MAPREDUCE_H

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
    Getter getFunc;
    Reducer reduceFunc;
    int partNum;
} getterParams;

// external functions
void MR_Emit(char *key, char *value);

unsigned long MR_DefaultHashPartition(char *key, int num_partitions);

void MR_Run(int argc, char *argv[], 
	    Mapper map, int num_mappers, 
	    Reducer reduce, int num_reducers, 
	    Partitioner partition);

#endif // MAPREDUCE_H