if (WIN32)
    set(DuckDB_USE_STATIC_LIBS ON)
else ()
    set(DuckDB_USE_STATIC_LIBS OFF)
endif ()
find_package(DuckDB REQUIRED)

include_directories(
        ${PROJECT_SOURCE_DIR}/src/include
        src/include
        ${DuckDB_INCLUDE_DIRS}
        ${PROJECT_SOURCE_DIR}/extension/httpfs/src/include)

add_subdirectory(src/connector)

add_library(duckdb_extension
        SHARED
        src/duckdb_extension.cpp
        src/duckdb_storage.cpp
        src/duckdb_scan.cpp
        src/duckdb_type_converter.cpp
        src/duckdb_catalog.cpp
        src/duckdb_table_catalog_entry.cpp
        src/duckdb_functions.cpp
        src/duckdb_secret_manager.cpp
        ${PROJECT_SOURCE_DIR}/extension/httpfs/src/s3_download_options.cpp
        ${DUCKDB_OBJECT_FILES})

set_extension_properties(duckdb_extension duckdb)

target_link_libraries(duckdb_extension
        PRIVATE
        ${DuckDB_LIBRARIES})

if (NOT WIN32)
    add_library(duckdb_installer
            SHARED
            src/duckdb_installer.cpp
            src/duckdb_install_func.cpp)
    add_library(duckdb_loader
            SHARED
            src/duckdb_loader.cpp)
    set_extension_properties(duckdb_installer duckdb_installer)
    set_extension_properties(duckdb_loader duckdb_loader)
endif ()

if (WIN32)
    # See comments in extension/httpfs/CMakeLists.txt.
    target_link_libraries(duckdb_extension PRIVATE kuzu)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(duckdb_extension)
    set_apple_dynamic_lookup(duckdb_installer)
    set_apple_dynamic_lookup(duckdb_loader)
endif ()
