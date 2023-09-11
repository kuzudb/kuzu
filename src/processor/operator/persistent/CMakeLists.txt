add_library(kuzu_processor_operator_persistent
        OBJECT
        copy.cpp
        copy_node.cpp
        copy_rel.cpp
        copy_rel_columns.cpp
        copy_rel_lists.cpp
        copy_to.cpp
        csv_file_writer.cpp
        csv_reader.cpp
        delete.cpp
        delete_executor.cpp
        insert.cpp
        insert_executor.cpp
        merge.cpp
        reader.cpp
        reader_functions.cpp
        reader_state.cpp
        set.cpp
        set_executor.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_persistent>
        PARENT_SCOPE)
