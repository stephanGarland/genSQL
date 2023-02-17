#include <stdlib.h>
#include <string.h>

#define ARR_ELEM_SIZE 10
#define ARR_LEN 3628800

int *generate_permutations() {
    int *arr = calloc(ARR_ELEM_SIZE, sizeof(int));
    if (!arr) {
        return NULL;
    }
    int digits[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    int *d = digits;
    int n = 10;
    int i, j, k, l;
    int tmp;

    for (i = 1; i < ARR_LEN; i++) {
        for (k = n - 2; k >= 0; k--) {
            if (digits[k] < digits[k+1]) {
                break;
            }
        }
        if (k < 0) {
            break;
        }
        for (l = n - 1; l > k; l--) {
            if (digits[k] < digits[l]) {
                break;
            }
        }
        tmp = digits[k];
        digits[k] = digits[l];
        digits[l] = tmp;
        for (j = k+1, l = n-1; j < l; j++, l--) {
            tmp = digits[j];
            digits[j] = digits[l];
            digits[l] = tmp;
        }
    }
    memcpy(arr, d, sizeof(int) * n);
    return arr;
}

