if (NOT WIN32)
    add_library(azure_installer
            SHARED
            azure_install_func.cpp
            ${PROJECT_SOURCE_DIR}/extension/duckdb/src/installer/duckdb_installer.cpp)
    set_extension_properties(azure_installer azure_installer azure)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(azure_installer)
endif ()
