cmake_minimum_required(VERSION 3.15)

project(HTTPFSExtension)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED FALSE)

include_directories(include ../../third_party/httplib ../../src/include)

if (NOT WIN32)
add_library(httpfs
        SHARED
        src/httpfs.cpp
        src/httpfs_extension.cpp)

target_link_libraries(httpfs
        PRIVATE
        dl  # For dlopen, dlsym, etc.
)

set_target_properties(httpfs PROPERTIES
        OUTPUT_NAME httpfs
        SUFFIX ".kuzu_extension"
)
endif ()

if (APPLE)
    set_target_properties(httpfs PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
endif ()
