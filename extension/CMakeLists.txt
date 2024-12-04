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

if ("httpfs" IN_LIST BUILD_EXTENSIONS)
    add_subdirectory(httpfs)
endif ()

if ("duckdb" IN_LIST BUILD_EXTENSIONS)
    if (NOT __32BIT__)
        # DuckDB does not officially support 32-bit builds, so we disable the
        # extension for 32-bit builds
        add_subdirectory(duckdb)
    endif ()
endif ()

if ("json" IN_LIST BUILD_EXTENSIONS)
    add_subdirectory(json)
endif ()

if ("postgres" IN_LIST BUILD_EXTENSIONS)
    if (NOT __32BIT__)
        # DuckDB does not officially support 32-bit builds, so we disable the
        # extension for 32-bit builds
        add_subdirectory(postgres)
    endif ()
endif ()

if ("sqlite" IN_LIST BUILD_EXTENSIONS)
    if (NOT __32BIT__)
        # DuckDB does not officially support 32-bit builds, so we disable the
        # extension for 32-bit builds
        add_subdirectory(sqlite)
    endif ()
endif ()

if ("fts" IN_LIST BUILD_EXTENSIONS)
    add_subdirectory(fts)
endif ()

if ("delta" IN_LIST BUILD_EXTENSIONS)
    add_subdirectory(delta)
endif ()

if (${BUILD_EXTENSION_TESTS})
    include_directories(${CMAKE_SOURCE_DIR}/third_party/spdlog)
    add_definitions(-DTEST_FILES_DIR="extension")
    add_subdirectory(${CMAKE_SOURCE_DIR}/test/gtest ${CMAKE_CURRENT_BINARY_DIR}/test/gtest EXCLUDE_FROM_ALL)
    # Make gtest available to subdirectories.
    add_library(GTest::GTest INTERFACE IMPORTED)
    target_link_libraries(GTest::GTest INTERFACE gtest_main)
    target_link_libraries(GTest::GTest INTERFACE gmock_main)
    enable_testing()
    add_subdirectory(${CMAKE_SOURCE_DIR}/test/test_helper ${CMAKE_CURRENT_BINARY_DIR}/test/test_helper)
    add_subdirectory(${CMAKE_SOURCE_DIR}/test/test_runner ${CMAKE_CURRENT_BINARY_DIR}/test/test_runner)
    add_subdirectory(${CMAKE_SOURCE_DIR}/test/graph_test ${CMAKE_CURRENT_BINARY_DIR}/test/graph_test)
    add_subdirectory(${CMAKE_SOURCE_DIR}/test/runner ${CMAKE_CURRENT_BINARY_DIR}/test/runner)
endif ()
