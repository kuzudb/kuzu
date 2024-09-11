add_library(kuzu_json_scalar_functions
        OBJECT
        json_array_length.cpp
        json_contains.cpp
        json_keys.cpp
        json_structure.cpp
        json_valid.cpp
        json_minify.cpp)

set(JSON_OBJECT_FILES
        ${JSON_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_json_scalar_functions>
        PARENT_SCOPE)
