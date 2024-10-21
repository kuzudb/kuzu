if(NOT MSVC)
    add_compile_options(-Wno-extra)
endif()

add_subdirectory(antlr4_cypher)
add_subdirectory(antlr4_runtime)
set(BROTLI_BUNDLED_MODE ON)
add_subdirectory(brotli)
add_subdirectory(fast_float)
add_subdirectory(alp)
add_subdirectory(fastpfor)
add_subdirectory(glob)
add_subdirectory(mbedtls)
add_subdirectory(miniz)
if(${BUILD_PYTHON})
    add_subdirectory(pybind11)
endif()
add_subdirectory(re2)
add_subdirectory(serd)
add_subdirectory(utf8proc)
add_subdirectory(zstd)
add_subdirectory(lz4)

include_directories(parquet
    snappy
    thrift)

add_library(parquet STATIC
    parquet/parquet_constants.cpp
    parquet/parquet_types.cpp
    thrift/thrift/protocol/TProtocol.cpp
    thrift/thrift/transport/TTransportException.cpp
    thrift/thrift/transport/TBufferTransports.cpp
    snappy/snappy.cc
    snappy/snappy-sinksource.cc)

target_compile_definitions(parquet PUBLIC HAVE_STDINT_H)
target_include_directories(
    parquet
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
