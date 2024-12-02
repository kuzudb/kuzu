add_library(kuzu_storage_index
        OBJECT
        hash_index.cpp
        hnsw_config.cpp
        hnsw_index.cpp
        hnsw_graph.cpp
        hnsw_index_utils.cpp
        in_mem_hash_index.cpp
        index_utils.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_index>
        PARENT_SCOPE)
