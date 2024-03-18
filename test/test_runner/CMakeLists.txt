add_library(
        test_runner
        OBJECT
        csv_to_parquet_converter.cpp
        test_parser.cpp
        test_runner.cpp
)

target_include_directories(
        test_runner
        PUBLIC
        ../include/
)

target_link_libraries(test_runner PUBLIC GTest::GTest kuzu)
