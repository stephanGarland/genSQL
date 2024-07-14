#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/time.h>
#include <uuid/uuid.h>

#define MSEC_PER_SEC 1000
#define USEC_PER_MSEC 1000
#define UUID_STR_LEN 37

void uuid_generate_v7(uuid_t out) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    uint64_t timestamp = ((uint64_t)tv.tv_sec * MSEC_PER_SEC) + (tv.tv_usec / USEC_PER_MSEC);

    out[0] = (timestamp >> 40) & 0xFF;
    out[1] = (timestamp >> 32) & 0xFF;
    out[2] = (timestamp >> 24) & 0xFF;
    out[3] = (timestamp >> 16) & 0xFF;
    out[4] = (timestamp >> 8) & 0xFF;
    out[5] = timestamp & 0xFF;

    out[6] = (out[6] & 0x0F) | 0x70;
    arc4random_buf(out + 7, 9);
    out[8] = (out[8] & 0x3F) | 0x80;
}

void uuid_generate_v4(uuid_t out) {
    arc4random_buf(out, 16);
    out[6] = (out[6] & 0x0f) | 0x40;
    out[8] = (out[8] & 0x3f) | 0x80;
}
char **fill_array (uint32_t count, int uuid_version) {
    uuid_t uuid;
    char **arr = calloc(count, sizeof(char *));
    if (!arr) {
        return NULL;
    }

    for (uint32_t i = 0; i < count; i++) {
        arr[i] = calloc(UUID_STR_LEN, sizeof(char));
        if (!arr[i]) {
            for (uint32_t j = 0; j < i; j++) {
                free(arr[j]);
            }
            free(arr);
            return NULL;
        }

        if (uuid_version == 4) {
            uuid_generate_v4(uuid);
        } else if (uuid_version == 7) {
            uuid_generate_v7(uuid);
        } else {
            for (uint32_t j = 0; j <= i; j++) {
                free(arr[j]);
            }
            free(arr);
            return NULL;
        }
        uuid_unparse_lower(uuid, arr[i]);
    }
    return arr;
}
