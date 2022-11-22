add_library(
        test_utility
        OBJECT
        test_helper.cpp)

target_include_directories(
        test_utility
        PUBLIC
        ../include/
        ../../src/include
)

target_link_libraries(test_utility
        PUBLIC
        GTest::GTest
        kuzu
)
