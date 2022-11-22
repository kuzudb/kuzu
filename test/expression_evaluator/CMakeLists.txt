add_executable(expression_evaluator_test
        aggregate_expression_evaluation_test.cpp
        operation_executor_test.cpp)

target_link_libraries(expression_evaluator_test
        PRIVATE
        test_utility)

add_test(kuzu_expression_evaluator_test expression_evaluator_test)
