# CMakeList.txt: CMake-Projektdatei der obersten Ebene. Führen Sie eine globale Konfiguration durch,
# und schließen Sie hier Unterprojekte ein.
#
cmake_minimum_required (VERSION 3.8)

project ("odbcBenchmark")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Profile")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Ox")

# Schließen Sie Unterprojekte ein.
add_subdirectory ("odbcBenchmark")
