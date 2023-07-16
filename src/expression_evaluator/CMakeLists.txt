add_library(kuzu_expression_evaluator
        OBJECT
        base_evaluator.cpp
        case_evaluator.cpp
        function_evaluator.cpp
        literal_evaluator.cpp
        node_rel_evaluator.cpp
        path_evaluator.cpp
        reference_evaluator.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_expression_evaluator>
        PARENT_SCOPE)
