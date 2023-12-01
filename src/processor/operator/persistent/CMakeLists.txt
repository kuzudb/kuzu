add_subdirectory(reader)
add_subdirectory(writer/parquet)

add_library(kuzu_processor_operator_persistent
        OBJECT
        copy_node.cpp
        copy_rdf_resource.cpp
        copy_rel.cpp
        copy_to.cpp
        copy_to_csv.cpp
        copy_to_parquet.cpp
        csv_file_writer.cpp
        delete.cpp
        delete_executor.cpp
        insert.cpp
        insert_executor.cpp
        merge.cpp
        set.cpp
        set_executor.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_persistent>
        PARENT_SCOPE)
