add_library(
        kuzu_binder_query
        OBJECT
        bound_create_clause.cpp
        bound_delete_clause.cpp
        bound_projection_body.cpp
        bound_set_clause.cpp
        query_graph.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_binder_query>
        PARENT_SCOPE)
