cmake_minimum_required(VERSION 3.11)

project(examples VERSION 0.0.3.1 LANGUAGES CXX)

include_directories(../src/include)
link_directories(../build/release/src)

add_executable(example main.cpp)
target_link_libraries(example PRIVATE kuzu)
