cmake_minimum_required(VERSION 3.11)
project(example-c)

include_directories(../../src/include)
link_directories(../../build/release/src)

add_executable(example-c main.c)
target_link_libraries(example-c kuzu)
