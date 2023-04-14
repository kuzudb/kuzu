add_library(kuzu_storage_in_mem_storage_structure
        OBJECT
        in_mem_column.cpp
        in_mem_column_chunk.cpp
        in_mem_lists.cpp
        in_mem_node_column.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_in_mem_storage_structure>
        PARENT_SCOPE)
