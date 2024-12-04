if (NOT WIN32)
    add_library(duckdb_loader
            SHARED
            duckdb_loader.cpp)
    set_extension_properties(duckdb_loader duckdb_loader duckdb)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(duckdb_loader)
endif ()
