if("httpfs" IN_LIST BUILD_EXTENSIONS)
    add_subdirectory(httpfs)
endif()

if ("duckdb_scanner" IN_LIST BUILD_EXTENSIONS)
    if(NOT __32BIT__)
        # DuckDB does not officially support 32-bit builds, so we disable the 
        # extension for 32-bit builds
        add_subdirectory(duckdb_scanner)
    endif()
endif()

if ("postgres_scanner" IN_LIST BUILD_EXTENSIONS)
    add_subdirectory(postgres_scanner)
endif()

if (${BUILD_EXTENSION_TESTS})
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
endif()
