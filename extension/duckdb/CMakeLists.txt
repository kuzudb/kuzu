if (WIN32)
    set(DuckDB_USE_STATIC_LIBS ON)
else ()
    set(DuckDB_USE_STATIC_LIBS OFF)
endif ()
find_package(DuckDB REQUIRED)

include_directories(
        ${PROJECT_SOURCE_DIR}/src/include
        src/include
        ${DuckDB_INCLUDE_DIRS})

add_library(duckdb_extension
        SHARED
        src/duckdb_extension.cpp
        src/duckdb_storage.cpp
        src/duckdb_scan.cpp
        src/duckdb_type_converter.cpp
        src/duckdb_catalog.cpp
        src/duckdb_table_catalog_entry.cpp
        src/duckdb_functions.cpp
        src/duckdb_s3_auth.cpp
        src/duckdb_connector.cpp)

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
    # On windows, there is no dynamic lookup available, so it's not
    # possible to generically look for symbols on library load. There are
    # two main alternatives to statically linking kuzu, neither of which is
    # appealing:
    #       1. Link against the shared library. This works well assuming
    #       the DLL is locatable, but this assumption isn't valid for users
    #       of kuzu.exe.
    #       2. Link against the executable (kuzu.exe). This is
    #       strange but works well for kuzu.exe. However, it forces
    #       users who are embedding kuzu in their application to recompile
    #       the extension _and_ export the symbols for the extension to
    #       locate on load.
    # We choose the simplest option. Windows isn't known
    # for its small libraries anyways...
    # Future work could make it possible to embed extension into kuzu,
    # which would help fix this problem.
    target_link_libraries(duckdb_extension PRIVATE kuzu)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(duckdb_extension)
    set_apple_dynamic_lookup(duckdb_installer)
    set_apple_dynamic_lookup(duckdb_loader)
endif ()
