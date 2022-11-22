add_library(
        kuzu_binder_expression
        OBJECT
        existential_subquery_expression.cpp
        expression.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_binder_expression>
        PARENT_SCOPE)
