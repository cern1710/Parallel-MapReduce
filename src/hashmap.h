#ifndef __hashmap_h__
#define __hashmap_h__

#define MAP_CAPACITY_INIT 23    // or maybe 31

typedef struct KVpair_ {
    char* key;
    void* value;
} KVpair;

typedef struct Map_ {
    KVpair** kvp;
    size_t capacity;
    size_t size;
    pthread_rwlock_t rwlock;
} HashMap;

HashMap* map_init();
size_t map_size(HashMap* map);
void map_put(HashMap* map);

#endif
