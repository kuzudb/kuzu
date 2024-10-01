add_library(kuzu_storage_index
        OBJECT
        hash_index.cpp
        in_mem_hash_index.cpp
        vector_index_header.cpp
        vector_index_builder.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_index>
        PARENT_SCOPE)
