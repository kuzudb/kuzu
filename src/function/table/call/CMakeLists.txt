add_library(kuzu_table_call
        OBJECT
        current_setting.cpp
        db_version.cpp
        show_connection.cpp
        show_tables.cpp
        storage_info.cpp
        table_info.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_table_call>
        PARENT_SCOPE)
