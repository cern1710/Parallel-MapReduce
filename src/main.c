#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "mapreduce.h"
#include "hashmap.h"

HashMap* hashmap;

void Map(char *file_name) {
    FILE *fp = fopen(file_name, "r");
    assert(fp != NULL);

    char *line = NULL, *token, *dummy;
    size_t size = 0;

    while (getline(&line, &size, fp) != -1) {
        dummy = line;
        while ((token = strsep(&dummy, " \t\n\r")) != NULL) {
	        if (!strcmp(token, ""))
		        break;
            MR_Emit(token, "1");
        }
    }

    free(line);
    fclose(fp);
}

void Reduce(char *key, Getter get_next, int partition_number) {
    int *count = (int*)malloc(sizeof(int));
    *count = 0;
    char *value;
    
    while ((value = get_next(key, partition_number)) != NULL)
        (*count)++;

    mapPut(hashmap, key, count, sizeof(int));
    free(count);
}


int main(int argc, char *argv[]) {
    if (argc < 3) {
	    printf("Invalid usage: ./hashmap <filename> ... <searchterm>\n");
	    return 1;
    }
    
    hashmap = initMap();
    // save the searchterm
    char* search_term = argv[argc - 1];
    argc -= 1;

    // run mapreduce
    MR_Run(argc, argv, Map, 2, Reduce, 1, MR_DefaultHashPartition);
    // get the number of occurrences and print
    char *result;
    if ((result = mapGet(hashmap, search_term)) != NULL)
	    printf("Found \"%s\" %d times!\n", search_term, *(int*)result);
    else
	    printf("Word not found!\n");
   
    return 0;
}
