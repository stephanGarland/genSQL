#include <fcntl.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <unistd.h>
#include <uuid/uuid.h>

#define MSEC_PER_SEC 1000
#define USEC_PER_MSEC 1000
#define UUID_STR_LEN 37

void uuid_generate_v7(uuid_t out, uint64_t given_time) {
    uint64_t timestamp = 0;
    if (given_time == 0) {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        timestamp = ((uint64_t)ts.tv_sec * MSEC_PER_SEC) +
                    (ts.tv_nsec / (USEC_PER_MSEC * 1000));
    } else {
        timestamp = given_time * MSEC_PER_SEC;
    }
    out[0] = (timestamp >> 40) & 0xFF;
    out[1] = (timestamp >> 32) & 0xFF;
    out[2] = (timestamp >> 24) & 0xFF;
    out[3] = (timestamp >> 16) & 0xFF;
    out[4] = (timestamp >> 8) & 0xFF;
    out[5] = timestamp & 0xFF;

    arc4random_buf(out + 6, 10);
    out[6] = (out[6] & 0x0F) | 0x70;
    out[8] = (out[8] & 0x3F) | 0x80;
}

void uuid_generate_v4(uuid_t out) {
    arc4random_buf(out, 16);
    out[6] = (out[6] & 0x0f) | 0x40;
    out[8] = (out[8] & 0x3f) | 0x80;
}

char* fill_array(uint32_t count,
                 int uuid_version,
                 const char* shmem_name,
                 uint32_t epoch_count) {
    uuid_t uuid;
    char* arr = calloc(count * UUID_STR_LEN, sizeof(char));
    if (!arr) {
        return NULL;
    }

    bool provided_epochs = false;
    int shmem_fd = 0;
    void* shmem_addr = 0;
    size_t epoch_index = 0;
    uint64_t given_time = 0;
    int64_t* epochs = NULL;

    if (strlen(shmem_name) > 0 || epoch_count > 0) {
        provided_epochs = true;
        shmem_fd = shm_open(shmem_name, O_RDONLY, 0666);
        if (shmem_fd == -1) {
            free(arr);
            return NULL;
        }
        shmem_addr = mmap(NULL, epoch_count * sizeof(int64_t), PROT_READ,
                          MAP_SHARED, shmem_fd, 0);
        if (shmem_addr == MAP_FAILED) {
            close(shmem_fd);
            free(arr);
            return NULL;
        }
        epochs = (int64_t*)shmem_addr;
    }

    for (uint32_t i = 0; i < count; i++) {
        char* current_uuid = arr + (i * UUID_STR_LEN);

        if (uuid_version == 4) {
            uuid_generate_v4(uuid);
        } else if (uuid_version == 7) {
            if (epoch_count > 0 && epoch_index < epoch_count) {
                given_time = epochs[epoch_index++];
            }
            uuid_generate_v7(uuid, given_time);
        } else {
            free(arr);
            if (provided_epochs) {
                munmap(shmem_addr, epoch_count * sizeof(int64_t));
                close(shmem_fd);
            }
            return NULL;
        }

        uuid_unparse_lower(uuid, current_uuid);
    }

    if (provided_epochs) {
        if (munmap(shmem_addr, epoch_count * sizeof(int64_t)) == -1) {
            free(arr);
            return NULL;
        }
        close(shmem_fd);
    }

    return arr;
}

void free_array(uint32_t* arr) {
    free(arr);
}
