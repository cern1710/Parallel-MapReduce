#ifndef HASH_H
#define HASH_H

#include <stddef.h> // for size_t

#define FNV_OFFSET 14695981039346656037UL
#define FNV_PRIME 1099511628211UL
#define MAP_CAPACITY_INIT 23    // or maybe 31
#define KMER_SIZE 3

size_t hash(char* str, size_t length);

size_t minHash(char* key, size_t capacity);
size_t bucketHash(char* key, size_t capacity);
size_t fnv1aHash(char* key, size_t capacity);

#endif // HASH_H
