add_library(kuzu_hnsw_function
        OBJECT
        create_hnsw_index.cpp
        drop_hnsw_index.cpp
        query_hnsw_index.cpp)

set(VECTOR_EXTENSION_OBJECT_FILES
        ${VECTOR_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_hnsw_function>
        PARENT_SCOPE)
