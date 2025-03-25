if (WIN32 OR BUILD_STATIC_EXTENSION)
    set(DuckDB_USE_STATIC_LIBS ON)
else ()
    set(DuckDB_USE_STATIC_LIBS OFF)
endif ()
find_package(DuckDB REQUIRED)

include_directories(
        ${PROJECT_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/src/include
        ${PROJECT_SOURCE_DIR}/extension/httpfs/src/include  # For S3 configuration
        src/include
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/include
        ${DuckDB_INCLUDE_DIRS})

add_subdirectory(src/connector)
add_subdirectory(src/storage)
add_subdirectory(src/main)
add_subdirectory(src/installer)
add_subdirectory(src/function)

build_extension_lib(${BUILD_STATIC_EXTENSION} "sqlite")

target_link_libraries(kuzu_${EXTENSION_LIB_NAME}_extension
        PRIVATE
        ${DuckDB_LIBRARIES})

if (NOT WIN32 AND NOT BUILD_STATIC_EXTENSION)
    add_library(sqlite_loader
            SHARED
            ${PROJECT_SOURCE_DIR}/extension/duckdb/src/loader/duckdb_loader.cpp)
    set_extension_properties(sqlite_loader sqlite_loader sqlite)
endif ()

if (APPLE AND NOT BUILD_STATIC_EXTENSION)
    set_apple_dynamic_lookup(sqlite_loader)
endif ()
