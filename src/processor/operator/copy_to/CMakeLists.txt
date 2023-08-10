add_library(kuzu_processor_operator_copy_to
        OBJECT
        copy_to.cpp
        csv_file_writer.cpp
        )

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_copy_to>
        PARENT_SCOPE)
