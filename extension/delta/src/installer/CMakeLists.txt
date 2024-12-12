if (NOT WIN32)
    add_library(delta_installer
            SHARED
            delta_install_func.cpp
            ${PROJECT_SOURCE_DIR}/extension/duckdb/src/installer/duckdb_installer.cpp)
    set_extension_properties(delta_installer delta_installer delta)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(delta_installer)
endif ()
