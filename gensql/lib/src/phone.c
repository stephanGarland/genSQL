#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include "fast_div.c"

#define AREA_CODE_STR_LEN 3
#define PHONE_STR_LEN 7
#define SEP_STR_LEN 2

/* TODO: support other number formats */
char* fill_array(uint64_t* numbers, uint32_t count, bool generate_area_code) {
    int phone_str_len;
    if (!generate_area_code) {
        phone_str_len = PHONE_STR_LEN + SEP_STR_LEN;
    } else {
        phone_str_len = PHONE_STR_LEN + AREA_CODE_STR_LEN + SEP_STR_LEN;
    }
    char* output = calloc(count * phone_str_len, sizeof(char));
    if (!output) {
        return NULL;
    }

    uint64_t div1 = 10000000;
    uint64_t div2 = 10000;
    __uint128_t precomputed1 = precompute_mod_u64(div1);
    __uint128_t precomputed2 = precompute_mod_u64(div2);

    for (uint32_t i = 0; i < count; i++) {
        uint64_t num = numbers[i];
        uint64_t area_code = fastdiv_u64(num, precomputed1);
        num = fastmod_u64(num, precomputed1, div1);
        uint64_t prefix = fastdiv_u64(num, precomputed2);
        uint64_t line_number = fastmod_u64(num, precomputed2, div2);

        if (!generate_area_code) {
            snprintf(output + i * phone_str_len, phone_str_len, "%03llu-%04llu",
                     prefix, line_number);
        } else {
            snprintf(output + i * phone_str_len, phone_str_len,
                     "%03llu-%03llu-%04llu", area_code, prefix, line_number);
        }
    }

    return output;
}

void free_array(char* arr) {
    free(arr);
}
