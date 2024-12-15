add_library(kuzu_table_call
        OBJECT
        bm_info.cpp
        create_project_graph.cpp
        current_setting.cpp
        db_version.cpp
        drop_project_graph.cpp
        show_connection.cpp
        show_attached_databases.cpp
        show_tables.cpp
        show_warnings.cpp
        clear_warnings.cpp
        stats_info.cpp
        storage_info.cpp
        table_info.cpp
        show_sequences.cpp
        show_functions.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_table_call>
        PARENT_SCOPE)
