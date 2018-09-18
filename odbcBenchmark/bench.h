#ifndef ODBCBENCHMARK_BENCH_H
#define ODBCBENCHMARK_BENCH_H

#include <chrono>

template<typename T>
auto bench(T &&fun) {
    const auto start = std::chrono::high_resolution_clock::now();

    fun();

    const auto end = std::chrono::high_resolution_clock::now();

    return std::chrono::duration<double>(end - start).count();
}

#endif //ODBCBENCHMARK_BENCH_H
