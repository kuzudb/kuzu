if (WIN32)
    set(DuckDB_USE_STATIC_LIBS ON)
else ()
    set(DuckDB_USE_STATIC_LIBS OFF)
endif ()
find_package(DuckDB REQUIRED)

include_directories(
        ${PROJECT_SOURCE_DIR}/src/include
        src/include
        ../duckdb/src/include
        ${DuckDB_INCLUDE_DIRS})

add_library(sqlite_extension
        SHARED
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/duckdb_scan.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/duckdb_catalog.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/duckdb_table_catalog_entry.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/duckdb_type_converter.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/duckdb_functions.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/connector/duckdb_connector.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/connector/duckdb_result_converter.cpp
        src/sqlite_extension.cpp
        src/sqlite_storage.cpp
        src/sqlite_connector.cpp)

set_extension_properties(sqlite_extension sqlite)

target_link_libraries(sqlite_extension
        PRIVATE
        ${DuckDB_LIBRARIES})

if (NOT WIN32)
    add_library(sqlite_installer
            SHARED
            src/sqlite_install_func.cpp
            ../duckdb/src/duckdb_installer.cpp)
    add_library(sqlite_loader
            SHARED
            ../duckdb/src/duckdb_loader.cpp)
    set_extension_properties(sqlite_installer sqlite_installer)
    set_extension_properties(sqlite_loader sqlite_loader)
endif ()

if (WIN32)
    # See comments in extension/httpfs/CMakeLists.txt.
    target_link_libraries(sqlite_extension PRIVATE kuzu)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(sqlite_extension)
    set_apple_dynamic_lookup(sqlite_loader)
    set_apple_dynamic_lookup(sqlite_installer)
endif ()
