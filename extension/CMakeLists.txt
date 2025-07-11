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

function(build_extension_lib build_static ext_name)
    string(TOUPPER "${ext_name}" ext_name_upper_case)
    if (build_static)
        add_library(kuzu_${ext_name}_static_extension
                STATIC
                ${${ext_name_upper_case}_EXTENSION_OBJECT_FILES})
        SET(EXTENSION_LIB_NAME "${ext_name}_static" PARENT_SCOPE)
        SET(EXTENSION_LIB_NAME "${ext_name}_static")
    else ()
        add_library(kuzu_${ext_name}_extension
                SHARED
                ${${ext_name_upper_case}_EXTENSION_OBJECT_FILES})
        SET(EXTENSION_LIB_NAME "${ext_name}" PARENT_SCOPE)
        SET(EXTENSION_LIB_NAME "${ext_name}")
    endif ()
    set_extension_properties(kuzu_${EXTENSION_LIB_NAME}_extension ${EXTENSION_LIB_NAME} ${ext_name})
    if (WIN32 OR build_static)
        # See comments in extension/httpfs/CMakeLists.txt.
        target_link_libraries(kuzu_${EXTENSION_LIB_NAME}_extension PRIVATE kuzu)
    endif ()
    if (APPLE AND NOT build_static)
        set_apple_dynamic_lookup(kuzu_${EXTENSION_LIB_NAME}_extension)
    endif ()
endfunction()

if (BUILD_EXTENSION_TESTS)
    enable_testing()
    if (NOT "${STATICALLY_LINKED_EXTENSIONS}" STREQUAL "")
        add_compile_definitions(__STATIC_LINK_EXTENSION_TEST__)
    endif()
endif ()

list(LENGTH STATICALLY_LINKED_EXTENSIONS LIST_SIZE)
if (NOT LIST_SIZE EQUAL 0)
    SET(BUILD_STATIC_EXTENSION ON)
else ()
    SET(BUILD_STATIC_EXTENSION OFF)
    add_compile_definitions(BUILD_DYNAMIC_LOAD)
endif ()

function(add_extension_if_enabled extension)
    if ("${extension}" IN_LIST BUILD_EXTENSIONS OR
            "${extension}" IN_LIST STATICALLY_LINKED_EXTENSIONS)
        add_subdirectory(${extension})
    endif()
endfunction()

function(add_extension_if_enabled_and_skip_32bit extension)
    if (NOT __32BIT__ AND
        ("${extension}" IN_LIST BUILD_EXTENSIONS OR
            "${extension}" IN_LIST STATICALLY_LINKED_EXTENSIONS))
        add_subdirectory(${extension})
    endif()
endfunction()

add_extension_if_enabled_and_skip_32bit("duckdb")
add_extension_if_enabled_and_skip_32bit("postgres")
add_extension_if_enabled_and_skip_32bit("sqlite")
add_extension_if_enabled_and_skip_32bit("delta")
add_extension_if_enabled_and_skip_32bit("iceberg")
add_extension_if_enabled_and_skip_32bit("azure")
add_extension_if_enabled_and_skip_32bit("unity_catalog")
add_extension_if_enabled("json")
add_extension_if_enabled("fts")
add_extension_if_enabled("vector")
add_extension_if_enabled("llm")
add_extension_if_enabled("httpfs")
add_extension_if_enabled("neo4j")
add_extension_if_enabled("algo")
