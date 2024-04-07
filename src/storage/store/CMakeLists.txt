add_library(kuzu_storage_store
        OBJECT
        chunked_node_group.cpp
        chunked_node_group_collection.cpp
        column.cpp
        column_chunk.cpp
        dictionary_chunk.cpp
        dictionary_column.cpp
        node_table.cpp
        node_table_data.cpp
        null_column.cpp
        rel_table.cpp
        rel_table_data.cpp
        string_column_chunk.cpp
        string_column.cpp
        struct_column_chunk.cpp
        struct_column.cpp
        table_data.cpp
        list_column_chunk.cpp
        list_column.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_store>
        PARENT_SCOPE)
