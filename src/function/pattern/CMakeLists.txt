add_library(kuzu_function_pattern
        OBJECT
        cost_function.cpp
        id_function.cpp
        label_function.cpp
        start_end_node_function.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_function_pattern>
        PARENT_SCOPE)
