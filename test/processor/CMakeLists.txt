add_executable(processor_test
        factorized_table/factorized_table_test.cpp
        hash_table/aggregate_hash_table_test.cpp
        order_by/key_block_merger_test.cpp
        order_by/order_by_key_encoder_test.cpp
        order_by/radix_sort_test.cpp)

target_link_libraries(processor_test
        PRIVATE
        test_helper)

add_test(kuzu_processor_test processor_test)
