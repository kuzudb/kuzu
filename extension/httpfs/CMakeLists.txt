cmake_minimum_required(VERSION 3.15)

SET(POSITION_INDEPENDENT_CODE ON)

project(HTTPFSExtension)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED FALSE)

if (NOT WIN32)

SET(CMAKE_FIND_PACKAGE_SORT_ORDER NATURAL)
SET(CMAKE_FIND_PACKAGE_SORT_DIRECTION DEC)
SET(OPENSSL_USE_STATIC_LIBS TRUE)

find_package(OpenSSL REQUIRED)
include_directories(${OPENSSL_INCLUDE_DIR})
add_compile_definitions(CPPHTTPLIB_OPENSSL_SUPPORT)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_subdirectory(third_party/mbedtls)

include_directories(src/include ../../third_party/httplib ../../src/include third_party/mbedtls/include)

add_library(httpfs
        SHARED
        src/httpfs.cpp
        src/httpfs_extension.cpp
        src/s3fs.cpp
        src/crypto.cpp)

target_link_libraries(httpfs
        PRIVATE
        dl  # For dlopen, dlsym, etc.
        mbedtls
)

set_target_properties(httpfs PROPERTIES
        OUTPUT_NAME httpfs
        SUFFIX ".kuzu_extension"
)

target_link_libraries(httpfs
        PRIVATE
        ${OPENSSL_LIBRARIES})
endif ()

if (APPLE)
    set_target_properties(httpfs PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
endif ()
