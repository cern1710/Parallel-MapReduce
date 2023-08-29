#ifndef __hashmap_h__
#define __hashmap_h__

#include "kvp.h"

typedef struct Map {
    KVpair** kvp;
    size_t capacity;
    size_t size;
} Map;

#endif
