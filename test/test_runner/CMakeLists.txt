if (NOT TARGET test_runner)
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
endif()

if (NOT TARGET extension_test_runner)
        add_library(
                extension_test_runner
                OBJECT
                csv_converter.cpp
                insert_by_row.cpp
                multi_copy_split.cpp
                test_parser.cpp
                test_runner.cpp
        )

        target_include_directories(
                extension_test_runner
                PUBLIC
                ../include/
        )
        target_link_libraries(extension_test_runner PUBLIC GTest::GTest kuzu)
endif()
