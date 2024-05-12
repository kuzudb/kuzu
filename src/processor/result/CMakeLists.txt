add_library(kuzu_processor_result
        OBJECT
        base_hash_table.cpp
        factorized_table.cpp
        factorized_table_schema.cpp
        factorized_table_util.cpp
        flat_tuple.cpp
        mark_hash_table.cpp
        result_set.cpp
        result_set_descriptor.cpp
        )

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_result>
        PARENT_SCOPE)
