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
        ../duckdb/src/duckdb_scan.cpp
        ../duckdb/src/duckdb_catalog.cpp
        ../duckdb/src/duckdb_table_catalog_entry.cpp
        ../duckdb/src/duckdb_type_converter.cpp
        ../duckdb/src/duckdb_functions.cpp
        ../duckdb/src/duckdb_connector.cpp
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
    target_link_libraries(sqlite_extension PRIVATE kuzu)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(sqlite_extension)
    set_apple_dynamic_lookup(sqlite_loader)
    set_apple_dynamic_lookup(sqlite_installer)
endif ()
