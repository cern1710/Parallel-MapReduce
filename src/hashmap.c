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
    int lock = pthread_rwlock_rdlock(&map->rwlock);
    // if (lock != 0) { return -1; }
    
    size_t size = map->size;

    lock = pthread_rwlock_unlock(&map->rwlock);
    if (lock != 0) { return -1; }

    return size;
} // map_size()


void map_put(HashMap* map, char* key, void* value, size_t value_size) {
    int lock = pthread_rwlock_rdlock(&map->rwlock);

} // map_put()


char* map_get(HashMap* map, char* key) {

} // map_get()

void map_remove(HashMap* map, char* key) {

} // map_remove()


void map_free(HashMap* map) {

} // map_free()


int map_resize(HashMap* map, size_t size) {
    pthread_rwlock_rdlock(&map->rwlock);

    if (size <= map->size) {
        size = map->capacity * 2;
    }

    KVpair** temp = (KVpair**) calloc(size, sizeof(KVpair*));
    if (temp == NULL) {
        pthread_rwlock_unlock(&map->rwlock);
        return -1; 
    }

    size_t i, hash_val;
    KVpair* entry;

    // rehashing existing keys
    for (i=0; i<map->capacity; i++) {
        if (map->kvp[i] != NULL) {
            entry = map->kvp[i];
        } else continue;
        
        hash_val = hash(entry->key, size);
        while (temp[hash_val] != NULL) {
            if (++hash_val == size) {
                hash_val = 0;
            }
        }
        
        temp[hash_val] = entry;
    }
    
    // free old kvp and replace with new one
    free(map->kvp);
    map->kvp = temp;
    map->capacity = size;

    pthread_rwlock_unlock(&map->rwlock);

    return 0;
} // map_resize()


size_t hash(char* key, size_t capacity) {

} // hash()
