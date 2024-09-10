# Copyright 2011-2020 David Robillard <d@drobilla.net>
# SPDX-License-Identifier: ISC

cmake_minimum_required(VERSION 3.15)

if(POLICY CMP0048)
    cmake_policy(SET CMP0048 NEW)
endif()

if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

if(POLICY CMP0069)
    cmake_policy(SET CMP0069 NEW)
endif()

project(serd C)

set(CMAKE_C_STANDARD 11)

set(EXTRA_TARGET_LINK_LIBRARIES)

include_directories(include)
include_directories(include/serd)
include_directories(src)

set(SERD_SOURCES
        include/serd.h
        src/attributes.h
        src/base64.c
        src/base64.h
        src/byte_sink.h
        src/byte_source.c
        src/byte_source.h
        src/env.c
        src/n3.c
        src/node.c
        src/node.h
        src/reader.c
        src/reader.h
        src/serd_config.h
        src/serd_internal.h
        src/stack.h
        src/string.c
        src/string_utils.h
        src/system.c
        src/system.h
        src/try.h
        src/uri.c
        src/uri_utils.h
        src/writer.c)

add_library(serd STATIC ${SERD_SOURCES})
add_compile_definitions(serd SERD_API=) # Don't expose symbols to public API.
target_include_directories(
        serd
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
