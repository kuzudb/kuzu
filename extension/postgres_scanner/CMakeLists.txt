find_package(DuckDB REQUIRED)

add_library(postgres_scanner
        SHARED
        ../duckdb_scanner/src/duckdb_scan.cpp
        ../duckdb_scanner/src/duckdb_catalog.cpp
        ../duckdb_scanner/src/duckdb_table_catalog_entry.cpp
        ../duckdb_scanner/src/duckdb_type_converter.cpp
        src/postgres_scanner_extension.cpp
        src/postgres_storage.cpp
        src/postgres_catalog.cpp)

include_directories(
        src/include
        ../duckdb_scanner/src/include
        ${DuckDB_INCLUDE_DIRS}
        ${PROJECT_SOURCE_DIR}/src/include)

set_target_properties(postgres_scanner PROPERTIES
        OUTPUT_NAME postgres_scanner
        PREFIX "lib"
        SUFFIX ".kuzu_extension"
)

set_target_properties(postgres_scanner
    PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build"
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build"
)

target_link_libraries(postgres_scanner
        PRIVATE
        ${DuckDB_LIBRARIES})

if (WIN32)
        # On windows, there is no dynamic lookup available, so it's not
        # possible to generically look for symbols on library load. There are
        # two main alternatives to statically linking kuzu, neither of which is
        # appealing:
        #       1. Link against the shared library. This works well assuming
        #       the DLL is locatable, but this assumption isn't valid for users
        #       of kuzu_shell.exe.
        #       2. Link against the executable (kuzu_shell.exe). This is
        #       strange but works well for kuzu_shell.exe. However, it forces
        #       users who are embedding kuzu in their application to recompile
        #       the extension _and_ export the symbols for the extension to
        #       locate on load.
        # We choose the simplest option. Windows isn't known
        # for its small libraries anyways...
        # Future work could make it possible to embed extension into kuzu,
        # which would help fix this problem.
        target_link_libraries(postgres_scanner PRIVATE kuzu)
endif()

if (APPLE)
    set_target_properties(postgres_scanner PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
endif ()
