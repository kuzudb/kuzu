add_library(
        test_runner
        OBJECT
        csv_converter.cpp
        insert_by_row.cpp
        multi_copy_split.cpp
        test_parser.cpp
        test_runner.cpp
)

target_include_directories(
        test_runner
        PUBLIC
        ../include/
)

target_link_libraries(test_runner PUBLIC GTest::GTest kuzu)
