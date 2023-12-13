cmake_minimum_required(VERSION 2.8.12)

if(POLICY CMP0048)
    cmake_policy(SET CMP0048 NEW)
endif()

if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

if(POLICY CMP0069)
    cmake_policy(SET CMP0069 NEW)
endif()

project(LIBURING C)

set(CMAKE_C_STANDARD 11)

set(EXTRA_TARGET_LINK_LIBRARIES)

include_directories(src
        src/include)

set(LIBURING_SOURCES
        src/include/liburing.h
        src/include/liburing/barrier.h
        src/include/liburing/io_uring.h
        src/ffi.c
        src/int_flags.h
        src/lib.h
        src/queue.c
        src/register.c
        src/setup.c
        src/syscall.c
        src/syscall.h
        src/version.c)

add_library(liburing STATIC ${LIBURING_SOURCES})
add_compile_definitions(liburing LIBURING_API=) # Don't expose symbols to public API.
target_include_directories(
        liburing
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)