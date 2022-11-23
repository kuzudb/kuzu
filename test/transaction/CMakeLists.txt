add_executable(transaction_test
        transaction_manager_test.cpp
        transaction_test.cpp)

target_link_libraries(transaction_test
        PRIVATE
        test_helper)

add_test(kuzu_transaction_test transaction_test)
