// #ifndef KVLIST_H
// #define KVLIST_H
#pragma once
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

// #endif // KVLIST_H
