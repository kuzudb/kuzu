add_library(kuzu_storage_in_mem_storage_structure
        OBJECT
        in_mem_column.cpp
        in_mem_lists.cpp
        node_in_mem_column.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_in_mem_storage_structure>
        PARENT_SCOPE)
