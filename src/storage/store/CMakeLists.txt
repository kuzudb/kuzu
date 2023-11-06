add_library(kuzu_storage_store
        OBJECT
        column_chunk.cpp
        compression.cpp
        column.cpp
        node_group.cpp
        node_table.cpp
        node_table_data.cpp
        nodes_store.cpp
        rel_table.cpp
        rel_table_data.cpp
        rels_store.cpp
        string_column_chunk.cpp
        string_column.cpp
        struct_column_chunk.cpp
        struct_column.cpp
        table_copy_utils.cpp
        table_data.cpp
        var_list_column_chunk.cpp
        var_list_column.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_store>
        PARENT_SCOPE)

target_link_libraries(kuzu_storage_store PRIVATE fastpfor)
