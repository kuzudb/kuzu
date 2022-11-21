cmake_minimum_required(VERSION 3.2)

project(kuzu_benchmark)

set(CMAKE_CXX_STANDARD 20)

include_directories(
        ../../src/include
        ./include
        ../../test/test_helper/include
        ../../tools/join_order_pick/include)
link_directories(../../build/release/src)

add_executable(kuzu_benchmark
        benchmark.cpp
        benchmark_runner.cpp
        main.cpp)

target_link_libraries(kuzu_benchmark kuzu join_order_pick test_helper)
