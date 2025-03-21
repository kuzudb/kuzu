# TODO(Ziyi): This is a temporary workaround otherwise we have to explicitly export symbols in extensions.
set(CMAKE_CXX_VISIBILITY_PRESET default)
set(CMAKE_C_VISIBILITY_PRESET default)

SET(STATICALLY_LINKED_EXTENSIONS "" PARENT_SCOPE)
function(add_static_link_extension extension_name)
    list(APPEND STATICALLY_LINKED_EXTENSIONS ${extension_name})
    set(STATICALLY_LINKED_EXTENSIONS "${STATICALLY_LINKED_EXTENSIONS}" PARENT_SCOPE)
endfunction()

include(extension_config.cmake)
set(STATICALLY_LINKED_EXTENSIONS "${STATICALLY_LINKED_EXTENSIONS}" PARENT_SCOPE)

function(set_extension_properties target_name output_name extension_name)
    set_target_properties(${target_name}
            PROPERTIES
            ARCHIVE_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/extension/${extension_name}/build"
            LIBRARY_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/extension/${extension_name}/build"
            RUNTIME_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/extension/${extension_name}/build"
            OUTPUT_NAME ${output_name}
            PREFIX "lib"
            SUFFIX ".kuzu_extension"
    )
endfunction()

function(set_apple_dynamic_lookup target_name)
    set_target_properties(${target_name} PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
endfunction()

if (${BUILD_EXTENSION_TESTS})
    enable_testing()
endif ()

if ("httpfs" IN_LIST BUILD_EXTENSIONS OR "httpfs" IN_LIST STATICALLY_LINKED_EXTENSIONS)
    add_subdirectory(httpfs)
endif ()

if ("duckdb" IN_LIST BUILD_EXTENSIONS OR "duckdb" IN_LIST STATICALLY_LINKED_EXTENSIONS)
    if (NOT __32BIT__)
        # DuckDB does not officially support 32-bit builds, so we disable the
        # extension for 32-bit builds
        add_subdirectory(duckdb)
    endif ()
endif ()

if ("json" IN_LIST BUILD_EXTENSIONS OR "json" IN_LIST STATICALLY_LINKED_EXTENSIONS)
    add_subdirectory(json)
endif ()

if ("postgres" IN_LIST BUILD_EXTENSIONS OR "postgres" IN_LIST STATICALLY_LINKED_EXTENSIONS)
    if (NOT __32BIT__)
        # DuckDB does not officially support 32-bit builds, so we disable the
        # extension for 32-bit builds
        add_subdirectory(postgres)
    endif ()
endif ()

if ("sqlite" IN_LIST BUILD_EXTENSIONS OR "sqlite" IN_LIST STATICALLY_LINKED_EXTENSIONS)
    if (NOT __32BIT__)
        # DuckDB does not officially support 32-bit builds, so we disable the
        # extension for 32-bit builds
        add_subdirectory(sqlite)
    endif ()
endif ()

if ("fts" IN_LIST BUILD_EXTENSIONS OR "fts" IN_LIST STATICALLY_LINKED_EXTENSIONS)
    add_subdirectory(fts)
endif ()

if ("delta" IN_LIST BUILD_EXTENSIONS OR "delta" IN_LIST STATICALLY_LINKED_EXTENSIONS)
    add_subdirectory(delta)
endif ()

if ("iceberg" IN_LIST BUILD_EXTENSIONS OR "iceberg" IN_LIST STATICALLY_LINKED_EXTENSIONS)
    add_subdirectory(iceberg)
endif ()

if ("unity_catalog" IN_LIST BUILD_EXTENSIONS OR "unity_catalog" IN_LIST STATICALLY_LINKED_EXTENSIONS)
    add_subdirectory(unity_catalog)
endif ()

if ("vector" IN_LIST BUILD_EXTENSIONS OR "vector" IN_LIST STATICALLY_LINKED_EXTENSIONS)
    add_subdirectory(vector)
endif()
