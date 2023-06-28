add_library(kuzu_processor_operator_call
        OBJECT
        call_config.cpp
        call_table_func.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_call>
        PARENT_SCOPE)
