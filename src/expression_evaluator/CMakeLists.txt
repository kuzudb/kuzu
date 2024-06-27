add_library(kuzu_expression_evaluator
        OBJECT
        case_evaluator.cpp
        expression_evaluator.cpp
        expression_evaluator_utils.cpp
        expression_evaluator_visitor.cpp
        function_evaluator.cpp
        lambda_evaluator.cpp
        literal_evaluator.cpp
        pattern_evaluator.cpp
        path_evaluator.cpp
        reference_evaluator.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_expression_evaluator>
        PARENT_SCOPE)
