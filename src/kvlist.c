#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kvlist.h"

//*************** Key-valued Pairs ***************//
kvp *initKVP(char *key, char *val) {
    kvp *pair = (kvp*)malloc(sizeof(kvp));
    strcpy(pair->key = (char*)malloc(strlen(key+1)), key);
    strcpy(pair->value = (char*)malloc(strlen(val+1)), key);
    return pair;
}

void updateKVP(kvp *kv, char *val) {
    free(kv->value);
    strcpy(kv->value = (char*)malloc(strlen(val)+1), val);
}

void freeKVP(kvp **kv) {
    free((*kv)->value);
    free((*kv)->key);
    free(*kv);
    kv = NULL;
}

//*************** KVP Lists ***************//
kvl_node *initKVL(kvp *kv) {
    kvl_node *node = (kvl_node*)malloc(sizeof(kvl_node));
    node->kvp = kv;
    node->next = NULL;
    return node;
}

bool isEmptyKVL(kvl *list) {
    return (list->head == NULL) ? true : false;
}

void appendKVL(kvl *list, kvp *kv) {
    kvl_node *node = (kvl_node*)malloc(sizeof(kvl_node));
    node->kvp = kv;
    node->next = NULL;
    if (list->head == NULL) 
        list->head = node;
    else 
        list->tail->next = node;
    list->tail = node;
}

void extendKVL(kvl *list1, kvl *list2) {
    if (list2->head == NULL) return;

    if (list1->head == NULL) {
        list1->head = list2->head;
        list1->tail = list2->tail;
    } else {
        list1->tail->next = list2->head;
        list1->tail = list2->tail;
    }

    list2->head = NULL; 
    list2->tail = NULL;
}

void kvlNodeSplit(kvl_node *node, kvl_node **head, kvl_node **tail) {
    kvl_node *node1 = node->next;
    kvl_node *node2 = node;

    while (node1 != NULL) {
       if ((node1 = node1->next) != NULL) {
            node1 = node1->next;
            node2 = node2->next;
       }
    }

    *head = node;
    *tail = node2->next;
    node2->next = NULL;
}

void mergeNode(kvl_node **node, kvl_node **result, kvl_node **current) {
    if (*result == NULL) {
        *result = *node;
        *current = *node;
    } else {
        (*current)->next = *node;
        *current = (*current)->next;
    }
    *node = (*node)->next;
}

kvl_node *kvlNodeMerge(kvl_node *node1, kvl_node *node2) {
    if (node1 == NULL) return node2;
    if (node2 == NULL) return node1;

    kvl_node *result = NULL;
    kvl_node *current = NULL;

    while (node1 != NULL && node2 != NULL) {
        if (strcmp(node1->kvp->key, node2->kvp->key) <= 0) {
            mergeNode(&node1, &result, &current);
        } else {
            mergeNode(&node2, &result, &current);
        }
    }

    current->next = (node1 != NULL) ? node1 : node2;
    return result;
}

void kvlNodeMergesort(kvl_node **node) {
    kvl_node *head = *node;
    kvl_node *node1, *node2;
    if (head == NULL || head->next == NULL) return;

    kvlNodeSplit(head, &node1, &node2);
    kvlNodeMergesort(&node1);
    kvlNodeMergesort(&node2);
    *node = kvlNodeMerge(node1, node2);
}

void kvlSort(kvl *list) {
    kvlNodeMergesort(&list->head);

    kvl_node *node = list->head;
    while (node != NULL && node->next != NULL)
        node = node->next;
    
    list->tail = node;
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

//*************** KVL iterator ***************//
kvl_iter *initIter(kvl *list) {
    kvl_iter *iter = (kvl_iter*)malloc(sizeof(kvl_iter));
    iter->node = list->head;
    return iter;
}

kvl_iter *iterNext(kvl_iter *iter) {
    if (iter->node == NULL) return NULL;
    kvp *kv = iter->node->kvp;
    iter->node = iter->node->next;
    return kv;
}

void freeIter(kvl_iter **iter) {
    free(*iter);
    *iter = NULL;
}