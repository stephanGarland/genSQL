#include <fcntl.h>
#include <sys/mman.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

const char *SH_MEM_NAME_FNAME = "/SHM_FNAME";
const char *SH_MEM_NAME_LNAME = "/SHM_LNAME";
const int SH_MEM_SZ = (1 << 20);

char* get_fname_ptr() {
    int shm_fname_fd;
    char *p_fname;

    shm_fname_fd = shm_open(SH_MEM_NAME_FNAME, O_RDWR, 0666);
    if (shm_fname_fd == -1) {
        fprintf(stderr, "%s\n", "FAILURE: couldn't open shm_fname");
        exit(EXIT_FAILURE);
    }
    p_fname = mmap(NULL, SH_MEM_SZ, PROT_WRITE | PROT_READ, MAP_SHARED, shm_fname_fd, 0);
    if (p_fname == MAP_FAILED) {
        fprintf(stderr, "%s\n", "FAILURE: couldn't map fname");
        exit(EXIT_FAILURE);
    }
    return p_fname;
}

char* get_lname_ptr() {
    int shm_lname_fd = shm_open(SH_MEM_NAME_LNAME, O_RDWR);
    char *p_lname = mmap(NULL, SH_MEM_SZ, PROT_WRITE | PROT_READ, MAP_SHARED, shm_lname_fd, 0);
    if (shm_lname_fd == -1) {
        fprintf(stderr, "%s\n", "FAILURE: couldn't open shm_lname");
        exit(EXIT_FAILURE);
    }
    if (p_lname == MAP_FAILED) {
        fprintf(stderr, "%s\n", "FAILURE: couldn't map lname");
        exit(EXIT_FAILURE);
    }
    return p_lname;
}

void shuffle(char *array, int n, int size) {
    srand(time(NULL));
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

void shuffle_fname(int rowcount, int max_word_len) {
    char *ptr = get_fname_ptr();
    if (ptr == MAP_FAILED) {
        fprintf(stderr, "%s\n", "FAILURE: couldn't call get_fname_ptr");
        exit(EXIT_FAILURE);
    }
    shuffle((char *)ptr, rowcount, max_word_len);
}

void shuffle_lname(int rowcount, int max_word_len) {
    char *ptr = get_lname_ptr();
    shuffle((char *)ptr, rowcount, max_word_len);
}
