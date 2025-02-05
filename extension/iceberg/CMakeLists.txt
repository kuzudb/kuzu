if (WIN32)
    set(DuckDB_USE_STATIC_LIBS ON)
else ()
    set(DuckDB_USE_STATIC_LIBS OFF)
endif ()
find_package(DuckDB REQUIRED)

include_directories(
        ${PROJECT_SOURCE_DIR}/src/include
        src/include
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/include
        ${PROJECT_SOURCE_DIR}/extension/httpfs/src/include
        ${PROJECT_SOURCE_DIR}/extension/delta/src/include
        ${DuckDB_INCLUDE_DIRS}
)

add_subdirectory(src/function)
add_subdirectory(src/connector)
add_subdirectory(src/installer)

add_library(iceberg_extension
        SHARED
        ${ICEBERG_OBJECT_FILES}
        src/iceberg_extension.cpp
)

add_library(delta_connector
    ${PROJECT_SOURCE_DIR}/extension/delta/src/connector/delta_connector.cpp
    ${PROJECT_SOURCE_DIR}/extension/duckdb/src/connector/duckdb_secret_manager.cpp
    ${PROJECT_SOURCE_DIR}/extension/duckdb/src/connector/duckdb_connector.cpp
    ${PROJECT_SOURCE_DIR}/extension/duckdb/src/function/duckdb_scan.cpp
    ${PROJECT_SOURCE_DIR}/extension/httpfs/src/s3_download_options.cpp
)

target_include_directories(delta_connector PRIVATE
    ${PROJECT_SOURCE_DIR}/extension/delta/src/include
    ${PROJECT_SOURCE_DIR}/extension/duckdb/src/include
    ${PROJECT_SOURCE_DIR}/extension/httpfs/src/include
)

set_extension_properties(iceberg_extension iceberg iceberg)

target_link_libraries(iceberg_extension
        PRIVATE
        ${DuckDB_LIBRARIES}
        delta_connector)

if (NOT WIN32)
    add_library(iceberg_loader
            SHARED
            ../duckdb/src/loader/duckdb_loader.cpp)
    set_extension_properties(iceberg_loader iceberg_loader iceberg)
endif ()

if (WIN32)
    # See comments in extension/httpfs/CMakeLists.txt.
    target_link_libraries(iceberg_extension PRIVATE kuzu)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(iceberg_extension)
    set_apple_dynamic_lookup(iceberg_loader)
endif ()
