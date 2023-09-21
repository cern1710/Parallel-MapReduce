#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// #define _POSIX_C_SOURCE 200809L

#include "kvlist.h"

kvp *initKVP(char *key, char *val) {
    kvp *pair = (kvp*)malloc(sizeof(kvp));
    strcpy(pair->key = (char*)malloc(strlen(key+1)), key);
    strcpy(pair->value = (char*)malloc(strlen(val+1)), key);
    return pair;
}

void freeKVP(kvp **kv) {
    free((*kv)->value);
    free((*kv)->key);
    free(*kv);
    kv = NULL;
}

void updateKVP(kvp *kv, char *val) {
    free(kv->value);
    strcpy(kv->value = (char*)malloc(strlen(val)+1), val);
}

kvl_node *initKVL(kvp *kv) {
    kvl_node *node = (kvl_node*)malloc(sizeof(kvl_node));
    node->kvp = kv;
    node->next = NULL;
    return node;
}

void freeKVL(kvl **list) {
    kvl_node *node = (*list)->head, *next;
    while (node != NULL) {
        next = node->next;
        freeKVP(&node);
        node = next;
    }
    free(*list);
    *list = NULL;
}

void append(kvl *list, kvp *kv) {
    kvl_node *node = (kvl_node*)malloc(sizeof(kvl_node));
    node->kvp = kv;
    node->next = NULL;
    if (list->head == NULL) 
        list->head = node;
    else 
        list->tail->next = node;
    list->tail = node;
}

void extend(kvl *list1, kvl *list2) {
    if (list2->head == NULL) return;

    if (list1->head == NULL) {
        
    }

    list2->head = NULL; list2->tail = NULL;
}