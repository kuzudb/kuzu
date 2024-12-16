if (NOT WIN32)
    add_library(iceberg_installer
            SHARED
            iceberg_install_func.cpp
            ${PROJECT_SOURCE_DIR}/extension/duckdb/src/installer/duckdb_installer.cpp)
    set_extension_properties(iceberg_installer iceberg_installer iceberg)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(iceberg_installer)
endif ()
