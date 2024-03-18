add_library(kuzu_function_pattern
        OBJECT
        id_function.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_function_pattern>
        PARENT_SCOPE)
