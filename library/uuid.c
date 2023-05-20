#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <uuid/uuid.h>

#define UUID_STR_LEN 37

#define lowercaseuuid true

char **fill_array (uint32_t size, bool use_v4) {
    char **arr = calloc(size, sizeof(char *));
    uuid_t binuuid;
    if (!arr) {
        return NULL;
    }
    if (use_v4) {
        for (uint32_t i = 0; i < size; i++) {
            arr[i] = calloc(UUID_STR_LEN, sizeof(char));
            uuid_generate_random(binuuid);
            uuid_unparse_lower(binuuid, arr[i]);
        }
    } else {
        for (uint32_t i = 0; i < size; i++) {
            arr[i] = calloc(UUID_STR_LEN, sizeof(char));
            uuid_generate_time(binuuid);
            uuid_unparse_lower(binuuid, arr[i]);
        }
    }
    return arr;
}
