add_library(kuzu_table_call
        OBJECT
        clear_warnings.cpp
        create_hnsw_index.cpp
        current_setting.cpp
        db_version.cpp
        query_hnsw_index.cpp
        show_connection.cpp
        show_attached_databases.cpp
        show_tables.cpp
        show_warnings.cpp
        stats_info.cpp
        storage_info.cpp
        table_info.cpp
        show_sequences.cpp
        show_functions.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_table_call>
        PARENT_SCOPE)
