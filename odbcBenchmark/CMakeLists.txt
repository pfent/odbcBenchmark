cmake_minimum_required(VERSION 3.8)
set(CMAKE_CXX_STANDARD 17)

add_executable(odbcBenchmark "odbcBenchmark.cpp" "odbcBenchmark.h")
if (CMAKE_CXX_COMPILER_ID MATCHES "MSVC")
    target_link_libraries(odbcBenchmark odbc32)
else ()
    target_link_libraries(odbcBenchmark odbc)
endif ()
