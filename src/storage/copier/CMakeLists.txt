add_library(kuzu_storage_in_mem_csv_copier
        OBJECT
        node_copy_executor.cpp
        node_copier.cpp
        rel_copy_executor.cpp
        table_copy_executor.cpp
        npy_reader.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_in_mem_csv_copier>
        PARENT_SCOPE)
