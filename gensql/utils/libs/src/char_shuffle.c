#include <fcntl.h>
#include <sys/mman.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// TODO: sync this with Python
const int SH_MEM_SZ = (1 << 20);

char* get_shared_mem_ptr(const char *shm_name) {
    int shm_fd;
    char *ptr;

    shm_fd = shm_open(shm_name, O_RDWR, 0666);
    if (shm_fd == -1) {
        fprintf(stderr, "FAILURE: couldn't open shared memory for %s\n", shm_name);
        exit(EXIT_FAILURE);
    }
    ptr = mmap(NULL, SH_MEM_SZ, PROT_WRITE | PROT_READ, MAP_SHARED, shm_fd, 0);
    if (ptr == MAP_FAILED) {
        fprintf(stderr, "FAILURE: couldn't map shared memory for %s\n", shm_name);
        exit(EXIT_FAILURE);
    }
    return ptr;
}

static int cmpstringp(const void *p1, const void *p2) {
    return strcmp((char*)p1, (char*) p2);
}

void shuffle(char *array, int n, int size, unsigned int seed) {
    srand(seed);

    if (n > 1) {
        for (int i = 0; i < n - 1; i++) {
            int j = i + rand() % (n - i);
            if ((j * size >= SH_MEM_SZ) || (i * size >= SH_MEM_SZ)) {
                fprintf(stderr, "Index out of bounds: i=%d, j=%d, size=%d, SH_MEM_SZ=%d\n", i, j, size, SH_MEM_SZ);
                exit(EXIT_FAILURE);
            }
            char temp[size];
            memcpy(temp, array + j * size, size);
            memcpy(array + j * size, array + i * size, size);
            memcpy(array + i * size, temp, size);
        }
    }
}

void shuffle_data(int rowcount, int max_word_len, const char *shm_name, unsigned int seed) {
    char *ptr = get_shared_mem_ptr(shm_name);
    if (ptr == MAP_FAILED) {
        fprintf(stderr, "FAILURE: couldn't call get_ptr function\n");
        exit(EXIT_FAILURE);
    }
    shuffle(ptr, rowcount, max_word_len, seed);
}

void sort(char *array, int n, int size) {
    qsort(&array[0], n - 1, size, cmpstringp);

}

void sort_data(int rowcount, int max_word_len, const char *shm_name) {
    char *ptr = get_shared_mem_ptr(shm_name);
    if (ptr == MAP_FAILED) {
        fprintf(stderr, "FAILURE: couldn't call get_ptr function\n");
        exit(EXIT_FAILURE);
    }
    sort(ptr, rowcount, max_word_len);
}
