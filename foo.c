//#define PY_SSIZE_T_CLEAN
//#include <Python.h>

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>

void fill_array(uint32_t arr[], uint32_t len) {
    for (uint32_t i = 0; i < len; i++) {
        arr[i] = i;
    }
}

uint32_t right_shift(uint32_t range) {
    uint64_t random32bit, multiresult;
    uint32_t leftover, threshold;
    random32bit =  random();
    multiresult = random32bit * range;
    leftover = (uint32_t) multiresult;
    if (leftover < range ) {
        threshold = -range % range ;
        while (leftover < threshold) {
            random32bit =  random();
            multiresult = random32bit * range;
            leftover = (uint32_t) multiresult;
        }
    }
    return multiresult >> 32;
}

void shuf(uint32_t *arr, uint32_t size) {
    for (uint32_t i = size; i > 0; i--) {
        uint32_t nextpos = right_shift(i);
        uint32_t tmp = arr[i-1];
        uint32_t val = arr[nextpos];
        arr[i-1] = val;
        arr[nextpos] = tmp;
    }
}


int main(int argc, char *argv[]) {
    uint32_t len = atoi(argv[1]);
    uint32_t arr[len];
    fill_array(arr, len);
    /*for (uint32_t i = 0; i < sizeof(arr) / sizeof(uint32_t); i++) {
        printf("%d ", arr[i]);
    }
    printf("\n");
    */
    shuf(arr, len);

    /*for (uint32_t i = 0; i < sizeof(arr) / sizeof(uint32_t); i++) {
        printf("%d ", arr[i]);
    }
    printf("\n");
    */
}
