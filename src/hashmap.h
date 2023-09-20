#ifndef HASHMAP_H
#define HASHMAP_H

#include <pthread.h>

typedef struct MapPair_ {
    char* key;
    void* value;
} MapPair;

typedef struct HashMap_ {
    MapPair** kvp;
    size_t capacity;
    size_t size;
    pthread_rwlock_t rwlock;
} HashMap;

HashMap* initMap();
size_t mapSize(HashMap* map);
void mapPut(HashMap* map, char* key, void* value, size_t value_size);
char* mapGet(HashMap* map, char* key);
void mapRemove(HashMap* map, char* key);
void freeMap(HashMap* map);

int mapResize(HashMap* map, size_t size);

#endif
