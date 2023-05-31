add_library(
        test_runner
        OBJECT
        test_parser.cpp
        test_runner.cpp
        csv_to_parquet_converter.cpp
)

target_include_directories(
        test_runner
        PUBLIC
        ../include/
        ../../src/include
)

target_link_libraries(test_runner PUBLIC GTest::GTest kuzu)
