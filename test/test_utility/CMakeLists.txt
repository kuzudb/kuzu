add_library(
        test_utility
        OBJECT
        test_helper.cpp)

target_link_libraries(test_utility PUBLIC GTest::GTest)