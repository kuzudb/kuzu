add_library(kuzu_function_table_functions
        OBJECT
        current_setting.cpp
        db_version.cpp
        show_tables.cpp
        table_info.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_function_table_functions>
        PARENT_SCOPE)
