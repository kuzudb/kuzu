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

set_target_properties(duckdb_extension
    PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build"
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build"
)

target_link_libraries(duckdb_extension
        PRIVATE
        ${DuckDB_LIBRARIES})

set_target_properties(duckdb_extension PROPERTIES
        OUTPUT_NAME duckdb
        PREFIX "lib"
        SUFFIX ".kuzu_extension"
)

if (NOT WIN32)
    add_library(duckdb_extension_installer
            SHARED
            src/duckdb_installer.cpp
            src/duckdb_install_func.cpp)

    add_library(duckdb_extension_loader
            SHARED
            src/duckdb_loader.cpp)

    set_target_properties(duckdb_extension_installer
            PROPERTIES
            ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build"
            LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build"
            RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build"
    )

    set_target_properties(duckdb_extension_loader
            PROPERTIES
            ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build"
            LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build"
            RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build"
    )

    set_target_properties(duckdb_extension_installer PROPERTIES
            OUTPUT_NAME duckdb_installer
            PREFIX "lib"
            SUFFIX ".kuzu_extension"
    )

    set_target_properties(duckdb_extension_loader PROPERTIES
            OUTPUT_NAME duckdb_loader
            PREFIX "lib"
            SUFFIX ".kuzu_extension"
    )
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
endif()

if (APPLE)
    set_target_properties(duckdb_extension PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
    set_target_properties(duckdb_extension_installer PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
    set_target_properties(duckdb_extension_loader PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
endif ()
