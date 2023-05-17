add_library(
        test_helper
        OBJECT
        test_helper.cpp
        test_parser.cpp
        test_case.cpp
        test_runner.cpp
)

target_include_directories(
        test_helper
        PUBLIC
        ../include/
        ../../src/include
)

target_link_libraries(test_helper PUBLIC GTest::GTest kuzu)
