// #ifndef KVLIST_H
// #define KVLIST_H
#pragma once
#include <stdbool.h>
typedef struct kvp_t {
    char *key;
    char *value;
} kvp;

typedef struct kvl_node_t {
    kvp *kvp;
    struct kvl_node *next;
} kvl_node;

typedef struct kvl_t {
    kvl_node *head;
    kvl_node *tail;
} kvl;

typedef struct kvl_iter_t {
    kvl_node *node;
} kvl_iter;

kvp *initKVP(char *key, char *val);
void updateKVP(kvp *kv, char *val);
void freeKVP(kvp **kv);

kvl_node *initKVL(kvp *kv);
bool isEmptyKVL(kvl *list);
void appendKVL(kvl *list, kvp *kv);
void extendKVL(kvl *list1, kvl *list2);
void kvlNodeSplit(kvl_node *node, kvl_node **head, kvl_node **tail);
void mergeNode(kvl_node **node, kvl_node **result, kvl_node **current);
kvl_node *kvlNodeMerge(kvl_node *node1, kvl_node *node2);
void kvlNodeMergesort(kvl_node **node);
void kvlSort(kvl *list);
void freeKVL(kvl **list);

kvl_iter *initIter(kvl *list);
kvl_iter *iterNext(kvl_iter *iter);
void freeIter(kvl_iter **iter);
// #endif // KVLIST_H
