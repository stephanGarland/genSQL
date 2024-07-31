#include <stdint.h>
#include <stdlib.h>
#include <time.h>

static uint64_t s[4];

static inline uint64_t rotl(const uint64_t x, int k) {
    return (x << k) | (x >> (64 - k));
}

static uint64_t next(void) {
    const uint64_t result = rotl(s[1] * 5, 7) * 9;
    const uint64_t t = s[1] << 17;
    s[2] ^= s[0];
    s[3] ^= s[1];
    s[1] ^= s[2];
    s[0] ^= s[3];
    s[2] ^= t;
    s[3] = rotl(s[3], 45);
    return result;
}

void seed_rng() {
    srand(time(NULL));
    for (int i = 0; i < 4; i++) {
        s[i] = ((uint64_t)rand() << 32) | rand();
    }
}

uint32_t* fill_array(uint32_t count, uint32_t min_value, uint32_t max_value) {
    if (min_value >= max_value) {
        return NULL;
    }

    uint32_t* arr = calloc(count, sizeof(uint32_t));
    if (arr == NULL) {
        return NULL;
    }

    uint64_t range = (uint64_t)max_value - min_value + 1;

    for (uint32_t i = 0; i < count; i++) {
        uint64_t rand_val = next();
        arr[i] = (uint32_t)((rand_val % range) + min_value);
    }

    return arr;
}

void free_array(uint32_t* arr) {
    free(arr);
}
