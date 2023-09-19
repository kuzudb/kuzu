add_library(kuzu_storage_store
        OBJECT
        column_chunk.cpp
        node_column.cpp
        node_group.cpp
        node_table.cpp
        nodes_store.cpp
        rel_table.cpp
        rels_store.cpp
        string_column_chunk.cpp
        string_node_column.cpp
        struct_column_chunk.cpp
        struct_node_column.cpp
        table_copy_utils.cpp
        var_list_column_chunk.cpp
        var_list_node_column.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_store>
        PARENT_SCOPE)
