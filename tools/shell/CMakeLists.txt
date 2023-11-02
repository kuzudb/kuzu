cmake_minimum_required(VERSION 3.2)

project(kuzu_shell)

set(CMAKE_CXX_STANDARD 20)

include_directories(../../src/include ./include ../../third_party/taywee_args/include)
link_directories(../../build/release/src)

add_executable(kuzu_shell
        embedded_shell.cpp
        linenoise.cpp
        shell_runner.cpp)

target_link_libraries(kuzu_shell kuzu)
if (MSVC)
        target_link_libraries(kuzu_shell ws2_32)
endif()
