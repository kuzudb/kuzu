include(FetchContent)

FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG release-1.11.0
)
FetchContent_MakeAvailable(googletest)
add_library(GTest::GTest INTERFACE IMPORTED)
target_link_libraries(GTest::GTest INTERFACE gtest_main)
target_link_libraries(GTest::GTest INTERFACE gmock_main)

enable_testing()
add_subdirectory(test_helper)
add_subdirectory(graph_test)
add_subdirectory(binder)
add_subdirectory(catalog)
add_subdirectory(common)
add_subdirectory(copy)
add_subdirectory(demo_db)
add_subdirectory(main)
add_subdirectory(optimizer)
add_subdirectory(parser)
add_subdirectory(processor)
add_subdirectory(runner)
add_subdirectory(storage)
add_subdirectory(transaction)
