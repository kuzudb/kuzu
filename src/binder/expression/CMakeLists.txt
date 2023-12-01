add_library(
        kuzu_binder_expression
        OBJECT
        case_expression.cpp
        expression.cpp
        expression_util.cpp
        function_expression.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_binder_expression>
        PARENT_SCOPE)
