#include "hash.h"

size_t hash(char* str, size_t length) {
    size_t hashVal = 0, i = 0;
    for (; i<length; i++) {
        hashVal = hashVal * 31 + str[i];
    }
    return hashVal;
} //hash()


size_t minHash(char* key, size_t capacity) {
    size_t minHashVal = __SIZE_MAX__;
    size_t sequenceLen = strlen(key);
    size_t i = 0, hashVal = 0;

    for (; i <= sequenceLen - KMER_SIZE; i++) {
        hashVal = hash(key+i, KMER_SIZE);
        if (hashVal < minHashVal)
            minHashVal = hashVal;
    }

    return (minHashVal % capacity);
} //min_hash()


size_t bucketHash(char* key, size_t capacity) {
    int i = 0, len = strlen(key);
    size_t hashVal = 0;

    for (; i<len; i++)
        hashVal = (hashVal*31 + key[i]) % capacity;

    return hashVal;
} //bucket_hash()


size_t fnv1aHash(char* key, size_t capacity) {
    size_t hashVal = FNV_OFFSET;
    for (const char *p = key; *p; p++) {
        hashVal ^= (size_t)(unsigned char)(*p);
        hashVal *= FNV_PRIME;
    }
    return (hashVal % capacity);
} //fnv1a_hash()
