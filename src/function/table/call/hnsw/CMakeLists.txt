add_library(kuzu_table_hnsw
        OBJECT
        create_hnsw_index.cpp
        drop_hnsw_index.cpp
        query_hnsw_index.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_table_hnsw>
        PARENT_SCOPE)
