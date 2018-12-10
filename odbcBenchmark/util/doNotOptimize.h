#pragma once

// see: https://github.com/google/benchmark/blob/master/include/benchmark/benchmark.h
template<class Tp>
inline __attribute__((always_inline)) void DoNotOptimize(Tp const &value) {
#if defined(__clang__)
    asm volatile("" : : "g"(value) : "memory");
#else
    asm volatile("" : : "i,r,m"(value) : "memory");
#endif
}
