add_library(kuzu_processor_operator_vector_index
        OBJECT
        bulk_vector_indexing.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_vector_index>
        PARENT_SCOPE)
