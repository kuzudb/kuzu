add_subdirectory(copy)
add_subdirectory(ddl)
add_subdirectory(read)

add_library(
        kuzu_binder_bind
        OBJECT
        bind_attach_database.cpp
        bind_create_macro.cpp
        bind_project_graph.cpp
        bind_ddl.cpp
        bind_detach_database.cpp
        bind_explain.cpp
        bind_file_scan.cpp
        bind_graph_pattern.cpp
        bind_projection_clause.cpp
        bind_query.cpp
        bind_reading_clause.cpp
        bind_standalone_call.cpp
        bind_transaction.cpp
        bind_updating_clause.cpp
        bind_extension.cpp
        bind_export_database.cpp
        bind_import_database.cpp
        bind_use_database.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_binder_bind>
        PARENT_SCOPE)
