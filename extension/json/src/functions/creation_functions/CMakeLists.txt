add_library(kuzu_json_creation_functions
        OBJECT
        json_array.cpp
        json_merge_patch.cpp
        json_object.cpp
        to_json.cpp)

set(JSON_OBJECT_FILES
        ${JSON_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_json_creation_functions>
        PARENT_SCOPE)


