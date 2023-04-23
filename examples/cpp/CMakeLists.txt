cmake_minimum_required(VERSION 3.11)
project(example-cpp)

set(CMAKE_CXX_STANDARD 20)

include_directories(../../src/include)
link_directories(../../build/release/src)

add_executable(example-cpp main.cpp)
target_link_libraries(example-cpp kuzu)
