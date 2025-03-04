add_library(kuzu_json_cast_functions
        OBJECT
        json_cast.cpp)

set(JSON_OBJECT_FILES
        ${JSON_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_json_cast_functions>
        PARENT_SCOPE)
