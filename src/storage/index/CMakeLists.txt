add_library(kuzu_storage_index
        OBJECT
        hash_index.cpp
        hash_index_builder.cpp
        hash_index_utils.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_index>
        PARENT_SCOPE)
