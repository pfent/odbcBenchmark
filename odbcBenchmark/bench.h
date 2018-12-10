#pragma once

#include <chrono>

template<typename T>
auto bench(T &&fun) {
    const auto start = std::chrono::high_resolution_clock::now();

    fun();

    const auto end = std::chrono::high_resolution_clock::now();

    return std::chrono::duration<double>(end - start).count();
}
