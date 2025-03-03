add_library(kuzu_hnsw_index
        OBJECT
        hnsw_config.cpp
        hnsw_graph.cpp
        hnsw_index.cpp
        hnsw_index_utils.cpp)

set(FTS_OBJECT_FILES
        ${FTS_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_hnsw_index>
        PARENT_SCOPE)
