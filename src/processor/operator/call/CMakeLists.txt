add_library(kuzu_processor_operator_call
        OBJECT
        standalone_call.cpp
        in_query_call.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_call>
        PARENT_SCOPE)
