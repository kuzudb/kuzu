add_library(kuzu_storage_in_mem_csv_copier
        OBJECT
        in_mem_arrow_node_copier.cpp
        in_mem_rel_csv_copier.cpp
        in_mem_structures_copier.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_in_mem_csv_copier>
        PARENT_SCOPE)
