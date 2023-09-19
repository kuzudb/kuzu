cmake_minimum_required(VERSION 2.8.7)

if(POLICY CMP0048)
    cmake_policy(SET CMP0048 NEW)
endif()

if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
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
        src/snappy/snappy-sinksource.cc
        src/miniparquet.cpp)

target_include_directories(
        miniparquet
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
