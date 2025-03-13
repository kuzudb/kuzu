add_library(kuzu_hnsw_index
        OBJECT
        hnsw_config.cpp
        hnsw_index.cpp
        hnsw_index_utils.cpp
        hnsw_graph.cpp)

set(VECTOR_OBJECT_FILES
        ${VECTOR_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_hnsw_index>
        PARENT_SCOPE)
