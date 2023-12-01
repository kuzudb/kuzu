add_library(kuzu_storage_store
        OBJECT
        column_chunk.cpp
        column.cpp
        node_group.cpp
        node_table.cpp
        node_table_data.cpp
        rel_table.cpp
        rel_table_data.cpp
        string_column_chunk.cpp
        string_column.cpp
        struct_column_chunk.cpp
        struct_column.cpp
        table_data.cpp
        var_list_column_chunk.cpp
        var_list_column.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_store>
        PARENT_SCOPE)
