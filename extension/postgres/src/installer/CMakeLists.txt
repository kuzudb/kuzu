if (NOT WIN32)
    add_library(postgres_installer
            SHARED
            postgres_install_func.cpp
            ${PROJECT_SOURCE_DIR}/extension/duckdb/src/installer/duckdb_installer.cpp)
    set_extension_properties(postgres_installer postgres_installer postgres)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(postgres_installer)
endif ()
