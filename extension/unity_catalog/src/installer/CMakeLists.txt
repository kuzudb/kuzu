if (NOT WIN32)
    add_library(unity_catalog_installer
            SHARED
            unity_catalog_install_func.cpp
            ${PROJECT_SOURCE_DIR}/extension/duckdb/src/installer/duckdb_installer.cpp)
    set_extension_properties(unity_catalog_installer unity_catalog_installer unity_catalog)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(unity_catalog_installer)
endif ()
