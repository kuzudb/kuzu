add_library(kuzu_storage_store
        OBJECT
        column_chunk.cpp
        node_group.cpp
        node_table.cpp
        nodes_statistics_and_deleted_ids.cpp
        nodes_store.cpp
        rel_table.cpp
        rels_statistics.cpp
        rels_store.cpp
        struct_column_chunk.cpp
        table_statistics.cpp
        var_sized_column_chunk.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_store>
        PARENT_SCOPE)
