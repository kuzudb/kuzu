add_executable(processor_test
        factorized_table/factorized_table_test.cpp
        hash_table/aggregate_hash_table_test.cpp
        orderBy/key_block_merger_test.cpp
        orderBy/order_by_key_encoder_test.cpp
        orderBy/radix_sort_test.cpp)

target_link_libraries(processor_test
        PRIVATE
        test_utility)

add_test(kuzu_processor_test processor_test)
