add_library(kuzu_function_schema
        OBJECT
        keys.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_function_schema>
        PARENT_SCOPE)
