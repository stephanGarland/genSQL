// Modified from https://github.com/lemire/fastmod/blob/master/include/fastmod.h

#include <stdbool.h>
#include <stdint.h>

uint64_t mul128_u32(uint64_t lowbits, uint32_t d) {
  return ((__uint128_t)lowbits * d) >> 64;
}

uint64_t precompute_M_u32(uint32_t d) {
  return UINT64_C(0xFFFFFFFFFFFFFFFF) / d + 1;
}

uint32_t fastmod_u32(uint32_t a, uint64_t M, uint32_t d) {
  uint64_t lowbits = M * a;
  return (uint32_t)(mul128_u32(lowbits, d));
}

bool is_divisible(uint32_t n, uint64_t M) {
  return n * M <= M - 1;
}

