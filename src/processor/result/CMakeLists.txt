add_library(kuzu_processor_result
        OBJECT
        factorized_table.cpp
        flat_tuple.cpp
        result_set.cpp
        result_set_descriptor.cpp
        )

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_result>
        PARENT_SCOPE)
