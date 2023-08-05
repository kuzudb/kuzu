add_library(kuzu_processor_operator_copy_to
        OBJECT
        copy_to.cpp
        csv_writer.cpp
        parquet_column_writer.cpp
        parquet_writer.cpp
        )

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_copy_to>
        PARENT_SCOPE)
