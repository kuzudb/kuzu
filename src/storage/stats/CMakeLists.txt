add_library(kuzu_storage_stats
        OBJECT
        table_stats.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_stats>
        PARENT_SCOPE)
