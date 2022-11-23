add_executable(value_vector_test
        value_vector_test.cpp)

target_link_libraries(value_vector_test
        PRIVATE
        test_helper)

add_test(kuzu_value_vector_test value_vector_test)
add_subdirectory(operations)
