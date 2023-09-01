#ifndef __hashmap_h__
#define __hashmap_h__

typedef struct KVpair_ {
    char* key;
    void* value;
} KVpair;

typedef struct HashMap_ {
    KVpair** kvp;
    size_t capacity;
    size_t size;
    pthread_rwlock_t rwlock;
} HashMap;

HashMap* map_init();
size_t map_size(HashMap* map);
void map_put(HashMap* map, char* key, void* value, size_t value_size);
char* map_get(HashMap* map, char* key);
void map_remove(HashMap* map, char* key);
void map_free(HashMap* map);

int map_resize(HashMap* map, size_t size);

#endif
