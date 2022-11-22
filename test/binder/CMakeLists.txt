add_executable(binder_test
        binder_test.cpp
        binder_error_test.cpp)

target_include_directories(
        binder_test
        PUBLIC
        ../mock
        ../../src/include
)

target_link_libraries(binder_test
        PUBLIC
        test_utility
        GTest::GTest)

add_test(kuzu_binder_test binder_test)
