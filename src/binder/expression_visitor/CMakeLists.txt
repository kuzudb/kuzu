add_library(
        kuzu_binder_expression_visitor
        OBJECT
        ../expression/expression_visitor.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_binder_expression_visitor>
        PARENT_SCOPE)
