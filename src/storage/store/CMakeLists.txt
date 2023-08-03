add_library(kuzu_storage_store
        OBJECT
        node_column.cpp
        var_list_node_column.cpp
        node_table.cpp
        nodes_statistics_and_deleted_ids.cpp
        nodes_store.cpp
        rel_table.cpp
        rels_statistics.cpp
        rels_store.cpp
        struct_node_column.cpp
        table_statistics.cpp
        string_node_column.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_store>
        PARENT_SCOPE)
