add_library(kuzu_storage_in_mem_csv_copier
        OBJECT
        node_copier.cpp
        copy_node_npy.cpp
        copy_rel_arrow.cpp
        copy_structures_arrow.cpp
        npy_reader.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_in_mem_csv_copier>
        PARENT_SCOPE)
