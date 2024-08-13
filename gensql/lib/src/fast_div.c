// Modified from https://github.com/lemire/fastmod/blob/master/include/fastmod.h

#include <stdbool.h>
#include <stdint.h>

uint64_t mul128_u32(uint64_t lowbits, uint32_t d) {
    return ((__uint128_t)lowbits * d) >> 64;
}

uint64_t mul128_u64(__uint128_t lowbits, uint64_t d) {
  __uint128_t bottom_half =
      (lowbits & UINT64_C(0xFFFFFFFFFFFFFFFF)) * d;
  bottom_half >>=
      64;
  __uint128_t top_half = (lowbits >> 64) * d;
  __uint128_t both_halves =
      bottom_half + top_half;
  both_halves >>= 64;
  return (uint64_t)both_halves;
}

uint64_t precompute_mod_u32(uint32_t d) {
    return UINT64_C(0xFFFFFFFFFFFFFFFF) / d + 1;
}

uint32_t fastmod_u32(uint32_t a, uint64_t M, uint32_t d) {
    uint64_t lowbits = M * a;
    return (uint32_t)(mul128_u32(lowbits, d));
}

uint32_t fastdiv_u32(uint32_t a, uint64_t M) {
  return (uint32_t)(mul128_u32(M, a));
}

bool is_divisible(uint32_t n, uint64_t M) {
    return n * M <= M - 1;
}

__uint128_t precompute_mod_u64(uint64_t d) {
  __uint128_t M = UINT64_C(0xFFFFFFFFFFFFFFFF);
  M <<= 64;
  M |= UINT64_C(0xFFFFFFFFFFFFFFFF);
  M /= d;
  M += 1;
  return M;
}

uint64_t fastmod_u64(uint64_t a, __uint128_t M, uint64_t d) {
  __uint128_t lowbits = M * a;
  return mul128_u64(lowbits, d);
}

uint64_t fastdiv_u64(uint64_t a, __uint128_t M) {
  return mul128_u64(M, a);
}
