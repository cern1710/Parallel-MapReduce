#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "hashmap.h"
#include "pthread.h"

HashMap* map_init() {
    HashMap* map = (HashMap*) malloc(sizeof(HashMap));

    map->kvp = (KVpair**) calloc(MAP_CAPACITY_INIT, sizeof(KVpair*));
    map->kvp = MAP_CAPACITY_INIT;
    map->size = 0;
    pthread_rwlock_init(&map->rwlock, NULL);

    return map;
} // map_init()


size_t map_size(HashMap* map) {
    int lockret = pthread_rwlock_rdlock(&map->rwlock);

    if (lockret != 0) { return -1;}
    size_t size = map->size;
    lockret = pthread_rwlock_unlock(&map->rwlock);
    if (lockret != 0) { return -1;}

    return size;
} // map_size()


void map_put(HashMap* map) {
    
}