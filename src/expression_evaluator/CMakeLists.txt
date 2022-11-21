add_library(kuzu_expression_evaluator
        OBJECT
        base_evaluator.cpp
        function_evaluator.cpp
        literal_evaluator.cpp
        reference_evaluator.cpp)

#target_include_directories(expression_evaluator PUBLIC ../include)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_expression_evaluator>
        PARENT_SCOPE)
