add_library(kuzu_table_call
        OBJECT
        checkpoint.cpp
        current_setting.cpp
        db_version.cpp
        show_connection.cpp
        show_attached_databases.cpp
        show_tables.cpp
        storage_info.cpp
        table_info.cpp
        show_sequences.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_table_call>
        PARENT_SCOPE)
