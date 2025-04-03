cmake_minimum_required(VERSION 3.11)
set(CMAKE_CXX_STANDARD 20)

project(kuzu_microbench VERSION 0.0.1 LANGUAGES CXX)

find_package(Threads REQUIRED)

include(FetchContent)
find_package(benchmark)
if (NOT benchmark_FOUND)
    FetchContent_Declare(
            benchmark
            GIT_REPOSITORY https://github.com/google/benchmark.git
            GIT_TAG v1.7.1
    )
    FetchContent_MakeAvailable(benchmark)
endif()

#FetchContent_Declare(
#    FastPFor
#    GIT_REPOSITORY https://github.com/lemire/FastPFor
#    GIT_TAG v0.1.8
#)
#FetchContent_MakeAvailable(FastPFor)

#FetchContent_Declare(
#    TurboPFor
#    GIT_REPOSITORY https://github.com/powturbo/TurboPFor-Integer-Compression
#    GIT_TAG 2023.06
#)
#FetchContent_MakeAvailable(TurboPFor)

include_directories(../src/include)
include_directories(../third_party/nlohmann_json)
include_directories(smhasher)
include_directories(${CMAKE_BINARY_DIR}/src/include/)
#include_directories(${FastPFor_SOURCE_DIR}/headers)
#include_directories(Oroch)
#include_directories(TurboPFor-Integer-Compression/include)
#include_directories(SIMDCompressionAndIntersection/include)

add_executable(kuzu_benchmark benchmark.cpp)
target_link_libraries(kuzu_benchmark benchmark::benchmark kuzu)
