add_library(kuzu_storage_in_mem_csv_copier
        OBJECT
        column_chunk.cpp
        node_group.cpp
        npy_reader.cpp
        read_file_state.cpp
        rel_copier.cpp
        rel_copy_executor.cpp
        struct_column_chunk.cpp
        table_copy_utils.cpp
        string_column_chunk.cpp
        var_list_column_chunk.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_in_mem_csv_copier>
        PARENT_SCOPE)
