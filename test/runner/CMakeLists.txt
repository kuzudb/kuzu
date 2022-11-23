add_executable(runner_test
        e2e_copy_csv_transaction_test.cpp
        e2e_ddl_test.cpp
        e2e_delete_create_transaction_test.cpp
        e2e_exception_test.cpp
        e2e_order_by_test.cpp
        e2e_read_list_test.cpp
        e2e_read_test.cpp
        e2e_set_transaction_test.cpp
        e2e_update_test.cpp)

target_link_libraries(runner_test
        PRIVATE
        test_helper)

add_test(kuzu_runner_test runner_test)
