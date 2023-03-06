cmake_minimum_required(VERSION 3.2)

project(kuzu_benchmark)

set(CMAKE_CXX_STANDARD 20)

include_directories(
        ../../src/include
        ./include
        ../../test/include/test_helper)
link_directories(../../build/release/src)

add_executable(kuzu_benchmark
        benchmark.cpp
        benchmark_runner.cpp
        main.cpp)

target_link_libraries(kuzu_benchmark kuzu test_helper)
