#ifndef HASHMAP_H
#define HASHMAP_H

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

HashMap* map_init();
size_t map_size(HashMap* map);
void map_put(HashMap* map, char* key, void* value, size_t value_size);
char* map_get(HashMap* map, char* key);
void map_remove(HashMap* map, char* key);
void map_free(HashMap* map);

int map_resize(HashMap* map, size_t size);

#endif
