add_subdirectory(gtest)

# Make gtest available to subdirectories.
add_library(GTest::GTest INTERFACE IMPORTED)
target_link_libraries(GTest::GTest INTERFACE gtest_main)
target_link_libraries(GTest::GTest INTERFACE gmock_main)

enable_testing()
add_subdirectory(test_helper)
add_subdirectory(test_runner)
add_subdirectory(graph_test)
add_subdirectory(c_api)
add_subdirectory(common)
add_subdirectory(main)
add_subdirectory(optimizer)
add_subdirectory(runner)
add_subdirectory(storage)
add_subdirectory(transaction)
add_subdirectory(util_tests)
