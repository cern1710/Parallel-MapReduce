#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pthread.h"
#include "hashmap.h"
#include "hash.h"

HashMap* initMap() {
    HashMap* map = (HashMap*) malloc(sizeof(HashMap));

    map->kvp = (MapPair**) calloc(MAP_CAPACITY_INIT, sizeof(MapPair*));
    map->kvp = MAP_CAPACITY_INIT;
    map->size = 0;
    pthread_rwlock_init(&map->rwlock, NULL);

    return map;
}


size_t mapSize(HashMap* map) {
    int lock = pthread_rwlock_rdlock(&map->rwlock);
    // if (lock != 0) { return -1; }
    
    size_t size = map->size;

    lock = pthread_rwlock_unlock(&map->rwlock);
    if (lock != 0) { return -1; }

    return size;
}


void mapPut(HashMap* map, char* key, void* value, size_t value_size) {
    int lock = pthread_rwlock_rdlock(&map->rwlock), hashVal;
    
    if (map->size > (map->capacity / 2)) {
        if (mapResize(map, -1) < 0) {
            lock = pthread_rwlock_unlock(&map->rwlock);
            fprintf(stderr, "Error: map_put() failed.\n");
            exit(0);
        }
    }

    MapPair* pair = (MapPair*) malloc(sizeof(MapPair));
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
}


char* mapGet(HashMap* map, char* key) {
    int lock = pthread_rwlock_rdlock(&map->rwlock);
    int hashVal = hash(key, map->capacity);

    while (map->kvp[hashVal] != NULL) {
        // update if keys are equal
        if (!strcmp(key, map->kvp[hashVal]->key)) {
            lock = pthread_rwlock_unlock(&map->rwlock);
            return map->kvp[hashVal]->value;
        }
        
        if (++hashVal == map->capacity) { hashVal = 0; }        
    }

    pthread_rwlock_unlock(&map->rwlock);
    return NULL;
} // map_get()

void mapRemove(HashMap* map, char* key) {
    int lock = pthread_rwlock_rdlock(&map->rwlock), hashVal;

    while (map->kvp[hashVal] != NULL) {
        // remove if keys are equal
        if (!strcmp(key, map->kvp[hashVal]->key)) {
            free(map->kvp[hashVal]);

            for (size_t i=hashVal; i<map->capacity-1; i++) {
                map->kvp[hashVal]->key = map->kvp[hashVal+1]->key;
                map->kvp[hashVal]->value = map->kvp[hashVal+1]->value;
            }

            map->size--;
            pthread_rwlock_unlock(&map->rwlock);
            return;
        }
        
        if (++hashVal == map->capacity) { hashVal = 0; }        
    }

    pthread_rwlock_unlock(&map->rwlock);
}


void freeKVP(MapPair* kvp) {
    if (kvp) {
        if (kvp->key) free(kvp->key);
        if (kvp->value) free(kvp->value);
        free (kvp);
    }
}


void freeMap(HashMap* map) {
    if (map) {
        if (map->kvp) {
            for (size_t i=0; i < map->capacity; i++) {
                freeKVP(map->kvp[i]);
            }
        }
        pthread_rwlock_destroy(&map->rwlock);
        free(map);
    }
}


int mapResize(HashMap* map, size_t size) {
    pthread_rwlock_rdlock(&map->rwlock);

    if (size == -1) {
        size = map->capacity * 2;
    }

    MapPair** temp = (MapPair**) calloc(size, sizeof(MapPair*));
    if (temp == NULL) {
        pthread_rwlock_unlock(&map->rwlock);
        return -1; 
    }

    size_t i, hashVal;
    MapPair* entry;

    // rehashing existing keys
    for (i=0; i<map->capacity; i++) {
        if (map->kvp[i] != NULL) {
            entry = map->kvp[i];
        } else continue;
        
        hashVal = bucketHash(entry->key, size);
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
}
