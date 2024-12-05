if (NOT WIN32)
    add_library(duckdb_installer
            SHARED
            duckdb_installer.cpp
            duckdb_install_func.cpp)
    set_extension_properties(duckdb_installer duckdb_installer duckdb)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(duckdb_installer)
endif ()
