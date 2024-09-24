if (WIN32)
    set(DuckDB_USE_STATIC_LIBS ON)
else ()
    set(DuckDB_USE_STATIC_LIBS OFF)
endif ()

find_package(DuckDB REQUIRED)

add_library(postgres_extension
        SHARED
        ../duckdb/src/duckdb_scan.cpp
        ../duckdb/src/duckdb_catalog.cpp
        ../duckdb/src/duckdb_table_catalog_entry.cpp
        ../duckdb/src/duckdb_type_converter.cpp
        ../duckdb/src/duckdb_functions.cpp
        ../duckdb/src/duckdb_connector.cpp
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
    target_link_libraries(postgres_extension PRIVATE kuzu)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(postgres_extension)
    set_apple_dynamic_lookup(postgres_loader)
    set_apple_dynamic_lookup(postgres_installer)
endif ()
