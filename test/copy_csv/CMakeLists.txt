add_executable(copy_csv_test
        copy_csv_dates_test.cpp
        copy_csv_dos_style_newline_test.cpp
        copy_csv_fault_test.cpp
        copy_csv_interval_test.cpp
        copy_csv_lists_test.cpp
        copy_csv_test.cpp
        copy_csv_timestamp_test.cpp)

target_link_libraries(copy_csv_test
        PRIVATE
        test_helper)

add_test(kuzu_copy_csv_test copy_csv_test)
