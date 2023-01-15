add_library(kuzu_storage_in_mem_csv_copier
        OBJECT
        copy_node_arrow.cpp
        copy_rel_arrow.cpp
        copy_structures_arrow.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_in_mem_csv_copier>
        PARENT_SCOPE)
