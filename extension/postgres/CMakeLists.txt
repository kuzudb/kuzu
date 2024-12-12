if (WIN32)
    set(DuckDB_USE_STATIC_LIBS ON)
else ()
    set(DuckDB_USE_STATIC_LIBS OFF)
endif ()

find_package(DuckDB REQUIRED)

include_directories(
        src/include
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/include
        ${DuckDB_INCLUDE_DIRS}
        ${PROJECT_SOURCE_DIR}/src/include)

add_subdirectory(src/connector)
add_subdirectory(src/storage)
add_subdirectory(src/main)
add_subdirectory(src/installer)

add_library(postgres_extension
        SHARED
        ${POSTGRES_EXTENSION_OBJECT_FILES})

set_extension_properties(postgres_extension postgres postgres)

target_link_libraries(postgres_extension
        PRIVATE
        ${DuckDB_LIBRARIES})

if (NOT WIN32)
    add_library(postgres_loader
            SHARED
            ${PROJECT_SOURCE_DIR}/extension/duckdb/src/loader/duckdb_loader.cpp)
    set_extension_properties(postgres_loader postgres_loader postgres)
endif ()

if (WIN32)
    # See comments in extension/httpfs/CMakeLists.txt.
    target_link_libraries(postgres_extension PRIVATE kuzu)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(postgres_extension)
    set_apple_dynamic_lookup(postgres_loader)
endif ()
