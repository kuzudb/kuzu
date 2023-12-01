cmake_minimum_required(VERSION 3.11)

if(POLICY CMP0048)
    cmake_policy(SET CMP0048 NEW)
endif()

if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

# Certain Clang compiler versions throw "deprecated declarations" warnings for
# Thrift.h which comes from the Apache thrift library.
# The workaround, for now, is to disable this warning if the compiler is Clang.
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    add_compile_options(-Wno-deprecated-declarations)
endif()

project(miniparquet CXX)

include_directories(src/parquet
        src/snappy
        src/thrift)

add_library(miniparquet STATIC
        src/parquet/parquet_constants.cpp
        src/parquet/parquet_types.cpp
        src/thrift/protocol/TProtocol.cpp
        src/thrift/transport/TTransportException.cpp
        src/thrift/transport/TBufferTransports.cpp
        src/snappy/snappy.cc
        src/snappy/snappy-sinksource.cc)

target_compile_definitions(miniparquet PUBLIC HAVE_STDINT_H)
target_include_directories(
        miniparquet
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
