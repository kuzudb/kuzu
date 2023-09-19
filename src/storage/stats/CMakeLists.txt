add_library(kuzu_storage_stats
        OBJECT
        nodes_statistics_and_deleted_ids.cpp
        property_statistics.cpp
        rels_statistics.cpp
        table_statistics.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_stats>
        PARENT_SCOPE)
