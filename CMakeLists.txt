cmake_minimum_required(VERSION 3.8)

project("odbcBenchmark")
if (CMAKE_CXX_COMPILER_ID MATCHES "MSVC")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /O2")
endif ()

add_subdirectory("odbcBenchmark")
