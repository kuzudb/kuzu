add_library(kuzu_processor_operator_copy
        OBJECT
        copy.cpp
        copy_npy.cpp
        copy_rel.cpp
        copy_node.cpp
        read_file.cpp
        read_csv.cpp
        read_parquet.cpp
        )

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_copy>
        PARENT_SCOPE)
