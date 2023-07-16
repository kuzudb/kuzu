add_library(kuzu_storage_in_mem_csv_copier
        OBJECT
        npy_reader.cpp
        rel_copier.cpp
        rel_copy_executor.cpp
        table_copy_utils.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_in_mem_csv_copier>
        PARENT_SCOPE)
