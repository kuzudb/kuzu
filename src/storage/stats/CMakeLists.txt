add_library(kuzu_storage_stats
        OBJECT
        metadata_dah_info.cpp
        node_table_statistics.cpp
        nodes_store_statistics.cpp
        rel_table_statistics.cpp
        rels_store_statistics.cpp
        table_statistics.cpp
table_statistics_collection.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_stats>
        PARENT_SCOPE)
