if (WIN32)
    set(DuckDB_USE_STATIC_LIBS ON)
else ()
    set(DuckDB_USE_STATIC_LIBS OFF)
endif ()

find_package(DuckDB REQUIRED)

add_library(postgres_extension
        SHARED
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/duckdb_scan.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/duckdb_catalog.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/duckdb_table_catalog_entry.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/duckdb_type_converter.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/duckdb_functions.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/connector/duckdb_connector.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/connector/duckdb_result_converter.cpp
        src/postgres_extension.cpp
        src/postgres_storage.cpp
        src/postgres_connector.cpp)

include_directories(
        src/include
        ../duckdb/src/include
        ${DuckDB_INCLUDE_DIRS}
        ${PROJECT_SOURCE_DIR}/src/include)

set_extension_properties(postgres_extension postgres)

target_link_libraries(postgres_extension
        PRIVATE
        ${DuckDB_LIBRARIES})

if (NOT WIN32)
    add_library(postgres_installer
            SHARED
            src/postgres_install_func.cpp
            ../duckdb/src/duckdb_installer.cpp)
    add_library(postgres_loader
            SHARED
            ../duckdb/src/duckdb_loader.cpp)
    set_extension_properties(postgres_installer postgres_installer)
    set_extension_properties(postgres_loader postgres_loader)
endif ()

if (WIN32)
    # See comments in extension/httpfs/CMakeLists.txt.
    target_link_libraries(postgres_extension PRIVATE kuzu)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(postgres_extension)
    set_apple_dynamic_lookup(postgres_loader)
    set_apple_dynamic_lookup(postgres_installer)
endif ()
