if (NOT WIN32)
    add_library(sqlite_installer
            SHARED
            sqlite_install_func.cpp
            ${PROJECT_SOURCE_DIR}/extension/duckdb/src/installer/duckdb_installer.cpp)
    set_extension_properties(sqlite_installer sqlite_installer sqlite)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(sqlite_installer)
endif ()
