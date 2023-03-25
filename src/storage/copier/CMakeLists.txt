add_library(kuzu_storage_in_mem_csv_copier
        OBJECT
        node_copier.cpp
        npy_node_copier.cpp
        rel_copier.cpp
        table_copier.cpp
        npy_reader.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_in_mem_csv_copier>
        PARENT_SCOPE)
