add_library(kuzu_processor_operator_csv_reader
        OBJECT
        csv_reader.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_csv_reader>
        PARENT_SCOPE)
