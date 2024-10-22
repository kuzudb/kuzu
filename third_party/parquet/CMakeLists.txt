add_library(parquet
        STATIC
        parquet_constants.cpp
        parquet_types.cpp)

target_include_directories(parquet PUBLIC ./include)
