#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "pthread.h"
#include "hashmap.h"
#include "hash.h"

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
    int lock = pthread_rwlock_rdlock(&map->rwlock), hashVal;
    
    if (map->size > (map->capacity / 2)) {
        if (map_resize(map, 0) < 0) {
            lock = pthread_rwlock_unlock(&map->rwlock);
            fprtinf(stderr, "Error: map_put() failed.\n");
            exit(0);
        }
    }

    KVpair* pair = (KVpair*) malloc(sizeof(KVpair));
    pair->value = (void*) malloc(value_size);
    pair->key = strdup(key);
    memcpy(pair->value, value, value_size);
    hashVal = hash(key, map->capacity);

    while (map->kvp[hashVal] != NULL) {
        // update if keys are equal
        if (!strcmp(key, map->kvp[hashVal]->key)) {
            free(map->kvp[hashVal]);
            map->kvp[hashVal] = pair;
            pthread_rwlock_unlock(&map->rwlock);
            return;
        }
        
        if (++hashVal == map->capacity) { hashVal = 0; }        
    }

    // key not found in hashmap
    map->kvp[hashVal] = pair;
    map->size += 1;

    pthread_rwlock_unlock(&map->rwlock);
} // map_put()


char* map_get(HashMap* map, char* key) {

} // map_get()

void map_remove(HashMap* map, char* key) {

} // map_remove()


void kvp_free(KVpair* kvp) {
    if (kvp) {
        if (kvp->key) free(kvp->key);
        if (kvp->value) free(kvp->value);
        free (kvp);
    }
} // kvp_free()


void map_free(HashMap* map) {
    if (map) {
        if (map->kvp) {
            for (size_t i=0; i < map->capacity; i++) {
                kvp_free(map->kvp[i]);
            }
        }
        pthread_rwlock_destroy(&map->rwlock);
        free(map);
    }
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

    size_t i, hashVal;
    KVpair* entry;

    // rehashing existing keys
    for (i=0; i<map->capacity; i++) {
        if (map->kvp[i] != NULL) {
            entry = map->kvp[i];
        } else continue;
        
        hashVal = bucket_hash(entry->key, size);
        while (temp[hashVal] != NULL) {
            if (++hashVal == size) {
                hashVal = 0;
            }
        }
        
        temp[hashVal] = entry;
    }
    
    // free old kvp and replace with new one
    free(map->kvp);
    map->kvp = temp;
    map->capacity = size;

    pthread_rwlock_unlock(&map->rwlock);

    return 0;
} // map_resize()
