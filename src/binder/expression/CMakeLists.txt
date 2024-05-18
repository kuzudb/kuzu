add_library(
        kuzu_binder_expression
        OBJECT
        case_expression.cpp
        expression.cpp
        expression_util.cpp
        function_expression.cpp
        literal_expression.cpp
        node_expression.cpp
        node_rel_expression.cpp
        parameter_expression.cpp
        property_expression.cpp
        variable_expression.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_binder_expression>
        PARENT_SCOPE)
