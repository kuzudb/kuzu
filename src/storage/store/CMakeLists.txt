add_library(kuzu_storage_store
        OBJECT
        chunked_node_group.cpp
        chunked_node_group_collection.cpp
        column.cpp
        column_chunk_data.cpp
        dictionary_chunk.cpp
        dictionary_column.cpp
        list_chunk_data.cpp
        list_column.cpp
        node_table.cpp
        node_table_data.cpp
        null_column.cpp
        rel_table.cpp
        rel_table_data.cpp
        string_chunk_data.cpp
        string_column.cpp
        struct_chunk_data.cpp
        struct_column.cpp
        table_data.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_store>
        PARENT_SCOPE)
