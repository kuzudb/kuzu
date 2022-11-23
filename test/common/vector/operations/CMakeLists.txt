add_executable(vector_operations
        vector_arithmetic_operations_test.cpp
        vector_boolean_operations_test.cpp
        vector_comparison_operations_test.cpp
        vector_hash_nodeid_operations_test.cpp
        vector_string_operations_test.cpp)

target_link_libraries(vector_operations
        PRIVATE
        test_helper)

add_test(kuzu_vector_operations vector_operations)
