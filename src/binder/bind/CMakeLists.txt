add_subdirectory(copy)
add_subdirectory(ddl)

add_library(
        kuzu_binder_bind
        OBJECT
        bind_comment_on.cpp
        bind_copy.cpp
        bind_create_macro.cpp
        bind_ddl.cpp
        bind_explain.cpp
        bind_file_scan.cpp
        bind_graph_pattern.cpp
        bind_projection_clause.cpp
        bind_query.cpp
        bind_reading_clause.cpp
        bind_standalone_call.cpp
        bind_transaction.cpp
        bind_updating_clause.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_binder_bind>
        PARENT_SCOPE)
