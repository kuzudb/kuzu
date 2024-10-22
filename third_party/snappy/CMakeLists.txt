add_library(snappy
        STATIC
        snappy.cc
        snappy-sinksource.cc)

target_include_directories(snappy PUBLIC ./include)
