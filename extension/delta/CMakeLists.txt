if (WIN32)
    set(DuckDB_USE_STATIC_LIBS ON)
else ()
    set(DuckDB_USE_STATIC_LIBS OFF)
endif ()
find_package(DuckDB REQUIRED)

include_directories(
        ${PROJECT_SOURCE_DIR}/src/include
        src/include
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/include
        ${PROJECT_SOURCE_DIR}/extension/httpfs/src/include
        ${DuckDB_INCLUDE_DIRS}
)

add_subdirectory(src/connector)
add_subdirectory(src/function)

add_library(delta_extension
        SHARED
        ${DELTA_OBJECT_FILES}
        src/delta_extension.cpp
        ${PROJECT_SOURCE_DIR}/extension/httpfs/src/s3_download_options.cpp)

set_extension_properties(delta_extension delta)

target_link_libraries(delta_extension
        PRIVATE
        ${DuckDB_LIBRARIES})

if (NOT WIN32)
    add_library(delta_installer
            SHARED
            src/delta_install_func.cpp
            ../duckdb/src/duckdb_installer.cpp)
    add_library(delta_loader
            SHARED
            ../duckdb/src/duckdb_loader.cpp)
    set_extension_properties(delta_installer delta_installer)
    set_extension_properties(delta_loader delta_loader)
endif ()

if (WIN32)
    # See comments in extension/httpfs/CMakeLists.txt.
    target_link_libraries(delta_extension PRIVATE kuzu)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(delta_extension)
    set_apple_dynamic_lookup(delta_loader)
    set_apple_dynamic_lookup(delta_installer)
endif ()
