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
add_subdirectory(src/installer)
add_subdirectory(src/main)

add_library(delta_extension
        SHARED
        ${DELTA_EXTENSION_OBJECT_FILES})

set_extension_properties(delta_extension delta delta)

target_link_libraries(delta_extension
        PRIVATE
        ${DuckDB_LIBRARIES})

if (NOT WIN32)
    add_library(delta_loader
            SHARED
            ${PROJECT_SOURCE_DIR}/extension/duckdb/src/loader/duckdb_loader.cpp)
    set_extension_properties(delta_loader delta_loader delta)
endif ()

if (WIN32)
    # See comments in extension/httpfs/CMakeLists.txt.
    target_link_libraries(delta_extension PRIVATE kuzu)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(delta_extension)
    set_apple_dynamic_lookup(delta_loader)
endif ()
