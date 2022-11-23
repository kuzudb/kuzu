add_executable(main_test
        config_test.cpp
        connection_test.cpp
        csv_output_test.cpp
        exception_test.cpp
        prepare_test.cpp
        result_value_test.cpp)

target_link_libraries(main_test
        PRIVATE
        test_helper)

add_test(kuzu_main_test main_test)
