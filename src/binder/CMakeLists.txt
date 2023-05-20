add_subdirectory(bind)
add_subdirectory(bind_expression)
add_subdirectory(expression)
add_subdirectory(query)
add_subdirectory(visitor)

add_library(kuzu_binder
        OBJECT
        binder.cpp
        bound_statement_result.cpp
        bound_statement_visitor.cpp
        expression_binder.cpp
        query_normalizer.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_binder>
        PARENT_SCOPE)
