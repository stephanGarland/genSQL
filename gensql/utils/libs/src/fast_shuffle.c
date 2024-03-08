// Taken from https://github.com/lemire/Code-used-on-Daniel-Lemire-s-blog/blob/master/2016/06/29/shuffle.c

#include <stdint.h>
#include <stdlib.h>

uint32_t *fill_array(uint32_t size) {
    uint32_t *arr = calloc(size, sizeof(uint32_t));
    if (!arr) {
        return NULL;
    }
    for (uint32_t i = 0; i < size; i++) {
        arr[i] = i + 1;
    }
    return arr;
}

uint32_t *fill_array_range(uint32_t start, uint32_t end) {
    uint32_t size = end - start;
    uint32_t *arr = calloc(size, sizeof(uint32_t));
    if (!arr) {
        return NULL;
    }
    for (uint32_t i = 0; i < size; i++) {
        arr[i] = start++;
    }
    return arr;
}

uint32_t right_shift(uint32_t range, uint32_t *seed) {
    uint64_t random32bit, multiresult;
    uint32_t leftover, threshold;
    random32bit = rand_r(seed);
    multiresult = random32bit * range;
    leftover = (uint32_t) multiresult;
    if (leftover < range ) {
        threshold = -range % range;
        while (leftover < threshold) {
            random32bit = rand_r(seed);
            multiresult = random32bit * range;
            leftover = (uint32_t) multiresult;
        }
    }
    return multiresult >> 32;
}

void shuf(uint32_t *arr, uint32_t size, uint32_t seed) {
    for (uint32_t i = size; i > 0; i--) {
        uint32_t nextpos = right_shift(i, &seed);
        uint32_t tmp = arr[i-1];
        uint32_t val = arr[nextpos];
        arr[i-1] = val;
        arr[nextpos] = tmp;
    }
}

